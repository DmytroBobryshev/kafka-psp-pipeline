package com.example.psp.ledger.config;

import com.example.psp.ledger.adapters.in.kafka.PaymentStatusChangedEvent;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumer side of M7's exactly-once loop for {@code payments.payment-status-changed.v1}.
 *
 * <p>Two settings in this class carry the entire consume-process-produce guarantee; everything else
 * is the same shape as {@code psp-connector}'s consumer config:
 *
 * <ol>
 *   <li>{@code isolation.level=read_committed} on the consumer factory, and
 *   <li>{@code ContainerProperties.setKafkaAwareTransactionManager(...)} on the container factory.
 * </ol>
 *
 * <p>Neither is sufficient alone, and it is worth being precise about which does what: (1) protects
 * this service from <em>other</em> services' uncommitted writes, while (2) is what puts
 * <em>this</em> service's consumed offsets inside its own producer transaction. A service with (1)
 * and not (2) reads cleanly and still commits offsets independently of its output.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, PaymentStatusChangedEvent> paymentStatusChangedConsumerFactory(
            KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        // --- isolation.level = read_committed -----------------------------------------------------
        // The read side of Kafka transactions, and mandatory here per docs/diagrams/topic-map.md
        // ("mandatory for consumers of anything the transactional ledger produces" - and this
        //  service consumes from psp-connector, which is not transactional today, so strictly it is
        //  a no-op on THIS topic right now; it is set anyway because the moment any upstream turns
        //  transactional, the default would silently start leaking aborted records into balances).
        //
        // What it actually changes, mechanically:
        //   * The consumer never fetches past the Last Stable Offset (LSO) - the offset of the
        //     first record of the oldest still-OPEN transaction on that partition. So an in-flight
        //     transaction upstream shows up as consumer lag, not as early reads. A producer that
        //     hangs mid-transaction pins the LSO until transaction.timeout.ms expires and the
        //     coordinator aborts it (see KafkaProducerConfig).
        //   * Records belonging to transactions the fetch response lists as aborted are dropped by
        //     the client before the application ever sees them, and the COMMIT/ABORT control records
        //     themselves are consumed but never delivered - which is why offsets appear to skip.
        //
        // read_uncommitted (the Kafka default) does none of the above: it delivers every record as
        // soon as it is appended, aborted or not. That contrast is the M7 "prove it" experiment -
        // see README's "Abort visibility proof".
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // --- enable.auto.commit=false --------------------------------------------------------------
        // Non-negotiable with transactions: an auto-commit timer committing offsets on its own
        // schedule, outside the producer transaction, is exactly the dual-write this module exists
        // to remove. With the transaction manager wired below, the container never calls
        // consumer.commitSync() at all - offsets go to the coordinator via
        // producer.sendOffsetsToTransaction(...) instead.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // --- group.id / auto.offset.reset ---------------------------------------------------------
        // "ledger.v1" and "earliest" per docs/diagrams/topic-map.md's consumer-groups table; both
        // come from spring.kafka.consumer.* in application.yml. earliest matters more here than
        // anywhere else in the system: a balance service that starts fresh and skips backlog is a
        // balance service with wrong balances - and replaying that backlog is safe precisely
        // because of the Postgres idempotency key (domain.port.LedgerRepository), not because of
        // anything Kafka does.

        // --- Deserializers: ErrorHandlingDeserializer wraps the real ones ---------------------------
        // ADR-0006 category C (poison pill), same as psp-connector: without this, a deserialization
        // failure throws out of poll() itself and the container cannot advance past the bad record.
        // With transactions in play the loop is worse, not better - each attempt would also open and
        // abort a transaction - so the wrapper is if anything more important here.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        // psp-connector publishes with spring.json.add.type.headers=false (ADR-0002: headers carry
        // only traceparent/event-id/event-type/aggregate-id, never a Java FQCN), so the target type
        // is configured here instead of read from a header.
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PaymentStatusChangedEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.psp.*");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChangedEvent>
            paymentStatusChangedKafkaListenerContainerFactory(
                    @Qualifier("paymentStatusChangedConsumerFactory")
                            ConsumerFactory<String, PaymentStatusChangedEvent> consumerFactory,
                    KafkaTransactionManager<String, Object> kafkaTransactionManager) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChangedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // --- THE line that makes this consume-process-produce EOS rather than "produce in a tx" ---
        // With a KafkaAwareTransactionManager set, KafkaMessageListenerContainer switches to
        // invokeRecordListenerInTx(): ONE transaction per record, in which it calls
        //
        //     producer.beginTransaction()
        //     <listener runs; KafkaTemplate.send() enrolls ledger.ledger-entry-recorded.v1>
        //     producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())
        //     producer.commitTransaction()      // or abortTransaction() if the listener threw
        //
        // sendOffsetsToTransaction is the call that cannot be made from application code, because
        // only the container holds the Consumer and its ConsumerGroupMetadata (memberId +
        // generationId - the group-coordinator-side half of zombie fencing since Kafka 2.5, see
        // KafkaProducerConfig section 3). Passing that metadata is also what lets the offsets be
        // written to __consumer_offsets by the TRANSACTION coordinator, under the same commit
        // marker as the data - offsets and output become one atomic unit.
        //
        // Consequence worth internalising: there is no Acknowledgment in the listener signature and
        // AckMode is irrelevant, because the container does not commit offsets through the consumer
        // at all while a transaction manager is present.
        factory.getContainerProperties().setKafkaAwareTransactionManager(kafkaTransactionManager);

        // --- AfterRollbackProcessor, not CommonErrorHandler ---------------------------------------
        // With a transaction manager, a listener exception rolls the transaction back and the
        // container hands the batch to an AfterRollbackProcessor instead of the usual error handler.
        // FixedBackOff(1000ms, 2) = up to two redeliveries, 1s apart, then give up and (in
        // EOSMode.V2) commit the offset in a new transaction so the partition is not blocked
        // forever.
        //
        // Two attempts, deliberately: the ledger.fail-after-produce abort hook throws on the FIRST
        // delivery only - the redelivery is short-circuited by the Postgres dedup check before it
        // can reach the throw - so this bound only has to be >= 2 for the experiment to terminate
        // cleanly, and keeping it small keeps the aborted-record count in the log equal to the
        // number of events, which is what makes the read_uncommitted count easy to interpret.
        //
        // ADR-0006 scope note, same as psp-connector: the real policy is a non-blocking retry chain
        // ending in payments.payment-status-changed.v1.ledger.dlq (docs/diagrams/topic-map.md).
        // That chain is M8. What happens today after the retries is "logged and skipped", not
        // "parked in a DLQ" - a documented gap, see README "Known issues".
        factory.setAfterRollbackProcessor(
                new DefaultAfterRollbackProcessor<>(new FixedBackOff(1_000L, 2L)));

        return factory;
    }
}
