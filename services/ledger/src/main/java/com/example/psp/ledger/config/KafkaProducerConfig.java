package com.example.psp.ledger.config;

import io.micrometer.observation.ObservationRegistry;
import jakarta.persistence.EntityManagerFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * M7's headline configuration: a <b>transactional</b> Kafka producer, the
 * {@link KafkaTransactionManager} that drives it, and - because this service has two transactional
 * resources and only one of them is covered by Kafka's guarantee - an explicitly declared JPA
 * transaction manager alongside it.
 *
 * <h2>1. What a transactional producer actually is</h2>
 *
 * <p>Setting a {@code transactional.id} on a producer turns three separate Kafka features on at
 * once:
 *
 * <ul>
 *   <li><b>Idempotent produce</b> ({@code enable.idempotence=true}) - forced on, and unsettable.
 *       Each producer gets a {@code producerId} and per-partition sequence numbers, so a broker-side
 *       retry that was already appended is deduplicated rather than written twice. This is the M5
 *       "producer idempotence is not consumer idempotence" distinction, one layer down.
 *   <li><b>Atomic multi-partition writes</b> - every record produced between
 *       {@code beginTransaction()} and {@code commitTransaction()} becomes visible to
 *       {@code read_committed} consumers at the same instant, across every partition and topic the
 *       transaction touched, or none of them do.
 *   <li><b>Zombie fencing across process incarnations</b> - see section 3.
 * </ul>
 *
 * <h2>2. The transaction coordinator and the markers it writes</h2>
 *
 * <p>The {@code transactional.id} is hashed to a partition of the internal
 * {@code __transaction_state} topic; the broker leading that partition is this producer's
 * <b>transaction coordinator</b> (compose sets
 * {@code transaction.state.log.replication.factor=3} / {@code transaction.state.log.min.isr=2}, so
 * that state is as durable as the data). The coordinator persists the transaction's state
 * (Ongoing / PrepareCommit / CompleteCommit / PrepareAbort / CompleteAbort) and the set of
 * partitions enrolled in it.
 *
 * <p>On commit it writes a <b>COMMIT control record</b> ("transaction marker") into every enrolled
 * partition, including the {@code __consumer_offsets} partition holding the offsets that were added
 * with {@code sendOffsetsToTransaction}. On abort it writes an <b>ABORT marker</b> instead. Those
 * markers occupy real offsets in the log, which is why a {@code read_committed} consumer sees gaps
 * in the offset sequence.
 *
 * <p>Data records are appended to their partitions <em>as they are produced</em>, before the
 * outcome is known - a transaction is not buffered on the client. What isolation actually does is
 * filter on the read side: a {@code read_committed} consumer never advances past the
 * <b>Last Stable Offset</b> (the first offset of any still-open transaction), buffers what it
 * fetches beyond the last commit marker, and discards records belonging to transactions listed in
 * the fetch response's aborted-transactions index. A {@code read_uncommitted} consumer does none of
 * that and hands the aborted records straight to the application. That difference is precisely what
 * {@code ledger.fail-after-produce} exists to demonstrate.
 *
 * <h2>3. {@code transactional.id} MUST be stable per logical instance</h2>
 *
 * <p>{@code initTransactions()} - called once by {@link DefaultKafkaProducerFactory} when it first
 * creates the producer - registers this {@code transactional.id} with the coordinator, retrieves
 * (or allocates) its {@code producerId}, and <b>bumps its producer epoch</b>. Any older producer
 * still alive with the same {@code transactional.id} and a now-stale epoch gets
 * {@code ProducerFencedException} / {@code InvalidProducerEpochException} on its next produce or
 * commit, and any transaction it left open is aborted by the coordinator on the new incarnation's
 * behalf. That is zombie fencing, and it is the reason a hung-then-resurrected pod cannot commit
 * work its replacement knows nothing about.
 *
 * <p><b>All of that depends on the new incarnation presenting the same {@code transactional.id}.</b>
 * A random id per boot (a {@code UUID.randomUUID()} prefix, say) allocates a brand-new
 * {@code producerId} at epoch 0, fences nothing, and leaves the zombie free to keep writing and
 * committing - the failure is completely silent, because every individual transaction still looks
 * perfectly well-formed. So the prefix here is derived from {@code ledger.instance-id}
 * ({@code application.yml}), a value that is <b>stable across restarts of one logical instance</b>
 * and <b>distinct between instances</b>. On Kubernetes (M18) that is the StatefulSet ordinal;
 * locally it is a command-line override when running a second copy.
 *
 * <p>Two instances sharing one {@code transactional.id} is just as broken in the other direction:
 * each would fence the other on every restart-free produce, and the group would livelock.
 *
 * <h3>Why the prefix does not include topic/partition</h3>
 *
 * <p>Before Kafka 2.5 the only way to fence a consumer-initiated transaction after a rebalance was
 * to give each {@code group.id + topic + partition} its own {@code transactional.id} (Spring Kafka
 * generated exactly that, and it produced a producer per partition). Kafka 2.5's
 * {@code sendOffsetsToTransaction(offsets, ConsumerGroupMetadata)} moved the fencing to the group
 * coordinator instead: the offsets are submitted together with the consumer's {@code memberId} and
 * {@code generationId}, so a member from a stale generation has its offset commit rejected outright.
 * Spring Kafka has used that mode ({@code EOSMode.V2}) exclusively since 3.0, which is why one
 * stable id per <em>instance</em> is now both sufficient and correct.
 *
 * <h2>4. Two transaction managers, on purpose</h2>
 *
 * <p>{@link #kafkaTransactionManager} covers Kafka only. {@link #transactionManager} (JPA) covers
 * the balance write only. There is no distributed transaction between them and this service does
 * not pretend otherwise - the Postgres side is made correct with idempotency instead (see
 * {@code application.RecordLedgerEntryUseCase} and README's "Where Kafka EOS ends").
 *
 * <p>The JPA manager is declared by hand rather than left to Spring Boot because
 * {@code JpaBaseConfiguration}'s auto-configured one is
 * {@code @ConditionalOnMissingBean(TransactionManager.class)}: defining {@code KafkaTransactionManager}
 * in a user configuration class would silently suppress it, and Spring Data JPA - which resolves
 * the bean <em>named</em> {@code transactionManager} - would then fail to start. Declaring both
 * explicitly also means the names in {@code @Transactional("...")} are grep-able facts rather than
 * conventions.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> ledgerEntryProducerFactory(
            KafkaProperties kafkaProperties, LedgerProperties ledgerProperties) {
        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);

        // enable.idempotence is implied by a transactional producer, but set explicitly so the
        // requirement is visible here rather than inferred: acks=all and
        // max.in.flight.requests.per.connection<=5 (both in application.yml, ADR-0003's global
        // defaults) are its preconditions, and a transactional producer refuses to start without
        // them.
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // transaction.timeout.ms - how long the coordinator waits before aborting a transaction
        // this producer began and never finished (a hung or killed instance). Must be <= the
        // broker's transaction.max.timeout.ms (15 min default). 60s is the client default, made
        // explicit because it is the actual upper bound on how long a read_committed consumer's
        // Last Stable Offset can be pinned by one stuck instance - i.e. how long downstream lag
        // grows before the coordinator cleans up.
        producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);

        DefaultKafkaProducerFactory<String, Object> factory =
                new DefaultKafkaProducerFactory<>(producerProps);

        // THE fencing-critical line. DefaultKafkaProducerFactory appends its own suffix to this
        // prefix to build the actual transactional.id, so what must be stable and unique is the
        // prefix. See section 3 of this class's javadoc; the value comes from
        // ledger.kafka.transactional-id-prefix, which is derived from ledger.instance-id.
        factory.setTransactionIdPrefix(ledgerProperties.kafka().transactionalIdPrefix());

        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(
            ProducerFactory<String, Object> ledgerEntryProducerFactory,
            ObservationRegistry observationRegistry) {
        // Because the factory above has a transaction-id prefix, this template is transactional:
        // send() outside a transaction would throw, and inside one it enrolls the target partition
        // in the open transaction. adapters.out.kafka.KafkaLedgerEntryPublisher is only ever called
        // from inside the listener's transaction, so it never needs executeInTransaction().
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(ledgerEntryProducerFactory);
        // M15: hand-built bean, so Boot's spring.kafka.template.observation-enabled property never
        // reaches it (see infra/compose/README.md's M15 section). Observation and transactionality
        // are orthogonal - this still wraps every send() in the Micrometer span it would get
        // without the transaction manager involved, injecting a real traceparent header the same
        // way. This is the last hop the acceptance-bar trace needs: payment-api -> psp-connector ->
        // ledger.
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(
            ProducerFactory<String, Object> ledgerEntryProducerFactory) {
        // Wired onto the listener container in KafkaConsumerConfig - that is what turns
        // "produce inside a transaction" into full consume-process-produce EOS, because only the
        // container can call sendOffsetsToTransaction (it owns the consumer and its group
        // metadata).
        return new KafkaTransactionManager<>(ledgerEntryProducerFactory);
    }

    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        // @Primary so that any @Transactional without an explicit manager name (Spring Data JPA's
        // own repository methods, for one) resolves to the database rather than to Kafka. Every
        // @Transactional in this service names its manager anyway; this is belt and braces against
        // a future annotation that forgets to.
        return new JpaTransactionManager(entityManagerFactory);
    }

    // ============================================================================================
    // M17: DLQ replay republisher (adapters.out.kafka.KafkaDlqRepublisher) - a dedicated, PLAIN
    // (non-transactional) byte-array producer. Deliberately NOT built from
    // ledgerEntryProducerFactory above: that factory's setTransactionIdPrefix(...) call is exactly
    // what makes its KafkaTemplate transactional, and a transactional KafkaTemplate only sends
    // successfully from inside a transaction the listener container already opened - see
    // KafkaDlqRepublisher's javadoc, "Why this must NOT be the transactional producer". This
    // factory sets no transactional.id at all, so DefaultKafkaProducerFactory builds a plain
    // producer that sends immediately, exactly what a REST-triggered replay call needs.
    // ============================================================================================

    @Bean
    public ProducerFactory<String, byte[]> dlqReplayProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProps = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        // Byte-array producer, not KafkaAvroSerializer: replay republishes the DLQ record's raw
        // value bytes unchanged (see KafkaDlqRepublisher's javadoc) - the same "cannot re-encode
        // what it never decoded" reasoning psp-connector's identical M17 producer uses.
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    @Bean
    public KafkaTemplate<String, byte[]> dlqReplayKafkaTemplate(
            @Qualifier("dlqReplayProducerFactory") ProducerFactory<String, byte[]> dlqReplayProducerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, byte[]> template = new KafkaTemplate<>(dlqReplayProducerFactory);
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }
}
