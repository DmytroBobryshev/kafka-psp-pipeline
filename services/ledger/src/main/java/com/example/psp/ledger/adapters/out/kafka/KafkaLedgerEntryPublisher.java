package com.example.psp.ledger.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import com.example.psp.ledger.domain.port.LedgerEntryPublisher;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Real Kafka adapter for {@link LedgerEntryPublisher}. Publishes
 * {@code ledger.ledger-entry-recorded.v1}, keyed by {@code merchantId} (ADR-0003: entries per
 * merchant in ledger order, and analytics aggregates by merchant with no repartition topic).
 *
 * <h2>This send is inside a transaction, and the code shows it by NOT saying so</h2>
 *
 * <p>The {@link KafkaTemplate} injected here is backed by a producer factory with a
 * {@code transactionIdPrefix} ({@code config.KafkaProducerConfig}), so it is transactional: calling
 * {@code send()} outside a transaction would throw {@code IllegalStateException}, and calling it
 * inside one enrols this topic-partition in the transaction that is already open on this thread.
 * There is deliberately no {@code executeInTransaction(...)} here - that helper starts a
 * <em>producer-only</em> transaction, which would exclude the consumed offsets and quietly reduce
 * the guarantee to "atomic produce" instead of exactly-once consume-process-produce. The
 * transaction that matters is begun by the listener container, one level up.
 *
 * <h2>The record has an offset before the transaction commits</h2>
 *
 * <p>The {@code whenComplete} callback below fires when the broker acknowledges the <b>append</b>,
 * which happens well before {@code commitTransaction()}. So the partition/offset logged here is
 * real and final even for a transaction that is about to abort - the record physically occupies
 * that offset in the log either way. All the commit adds is a control marker that tells
 * {@code read_committed} consumers whether they are allowed to see it. Logging the offset here and
 * then aborting is, in fact, the cleanest possible demonstration of that (see
 * {@code ledger.fail-after-produce} and README's "Abort visibility proof").
 *
 * <p>The future is deliberately not awaited: blocking inside the transaction buys nothing, since
 * {@code commitTransaction()} already flushes and fails the transaction if any enrolled send
 * failed. A send failure therefore surfaces as an aborted transaction, not as a silently swallowed
 * callback - so the {@code log.error} branch is diagnostics, not error handling.
 */
@Component
public class KafkaLedgerEntryPublisher implements LedgerEntryPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaLedgerEntryPublisher.class);
    private static final String EVENT_TYPE = "ledger.ledger-entry-recorded.v1";
    private static final String SOURCE = "ledger";
    private static final String AGGREGATE_TYPE = "merchant";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final LedgerEntryAvroEventFactory avroEventFactory;
    private final String topic;

    /**
     * Only ever true for the abort-visibility drill. A transactional send sits in the producer's
     * accumulator until the sender thread transmits it; {@code abortTransaction()} discards
     * anything still buffered. So an abort thrown immediately after {@code send()} leaves an abort
     * marker in the log with no record behind it - and then there is nothing for a
     * {@code read_uncommitted} consumer to see. Blocking here forces the append to happen before
     * the caller can abort, which is what makes the aborted record observable.
     */
    private final boolean awaitAppend;

    public KafkaLedgerEntryPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            LedgerEntryAvroEventFactory avroEventFactory,
            @Value("${ledger.kafka.ledger-entry-recorded-topic}") String topic,
            @Value("${ledger.fail-after-produce:false}") boolean awaitAppend) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroEventFactory = avroEventFactory;
        this.topic = topic;
        this.awaitAppend = awaitAppend;
    }

    @Override
    public void publishEntryRecorded(LedgerEntry entry, MerchantBalance balanceAfter) {
        // causationId = the inbound event id = the value under this entry's unique constraint. The
        // causal chain and the idempotency key are the same id, by construction (ADR-0002).
        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        entry.getInboundEventId(),
                        EVENT_TYPE,
                        1,
                        entry.getMerchantId(),
                        AGGREGATE_TYPE,
                        SOURCE,
                        entry.getTraceId(),
                        entry.getCorrelationId());
        com.example.psp.common.events.avro.LedgerEntryRecorded event =
                avroEventFactory.toAvro(envelope, entry, balanceAfter);

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, entry.getMerchantId(), event);
        record.headers()
                .add("traceparent", entry.getTraceId().getBytes(StandardCharsets.UTF_8))
                .add("event-id", envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", envelope.eventType().getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", envelope.aggregateId().getBytes(StandardCharsets.UTF_8));

        var future =
                kafkaTemplate
                        .send(record)
                        .whenComplete(
                                (result, ex) -> {
                                    if (ex != null) {
                                        log.error(
                                                "Failed to append {} for entryId={} - the surrounding "
                                                        + "transaction will abort",
                                                topic,
                                                entry.getId(),
                                                ex);
                                    } else {
                                        RecordMetadata metadata = result.getRecordMetadata();
                                        log.info(
                                                "Appended {} entryId={} merchantId={} amount={} balanceAfter={} "
                                                        + "partition={} offset={} (NOT yet committed - the commit "
                                                        + "marker follows if the listener returns normally)",
                                                topic,
                                                entry.getId(),
                                                entry.getMerchantId(),
                                                event.getAmount(),
                                                event.getBalanceAfter(),
                                                metadata.partition(),
                                                metadata.offset());
                                    }
                                });

        if (awaitAppend) {
            future.join();
        }
    }
}
