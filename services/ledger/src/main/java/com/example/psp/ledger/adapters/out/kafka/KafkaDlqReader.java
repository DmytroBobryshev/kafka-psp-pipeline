package com.example.psp.ledger.adapters.out.kafka;

import com.example.psp.ledger.domain.model.DlqHeader;
import com.example.psp.ledger.domain.model.DlqRecord;
import com.example.psp.ledger.domain.port.DlqReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

/**
 * Real Kafka adapter for {@link DlqReader}. A short-lived {@link Consumer}, created fresh per call
 * under its own dedicated {@code group.id} ({@code ledger.dlq-replay.consumer-group} - see
 * {@code config.KafkaConsumerConfig#dlqReplayConsumerFactory}, entirely separate from
 * {@code ledger.v1} and from M7's transactional consumer container), reads at most one bounded
 * batch, commits exactly the offsets it actually processed, and closes - the same shape
 * webhook-notifier's M8 {@code adapters.out.kafka.KafkaDlqReader} uses.
 *
 * <h2>Raw bytes, not the decoded Avro record, and no transaction manager</h2>
 *
 * <p>This is a PLAIN {@link Consumer} - not a {@code @KafkaListener} container, so M7's
 * {@code KafkaTransactionManager} (which only ever wires onto a listener container) does not apply
 * to it at all. It also deserializes the key as a plain string and the value as plain bytes (see
 * {@code config.KafkaConsumerConfig}), never decoding the DLQ record's value as the generated
 * {@code PaymentStatusChanged} Avro class. Replay's whole job is to put the record back on
 * {@code payments.payment-status-changed.v1} byte-for-byte unchanged, so there is nothing to decode
 * <em>for</em>; reading raw bytes also means this reader can never itself throw a deserialization
 * error on a record already sitting in a dead-letter queue, so unlike this service's OTHER consumer
 * factories, it needs no {@code ErrorHandlingDeserializer} wrapper at all.
 *
 * <h2>The guard, mechanically</h2>
 *
 * <p>The consumer factory's {@code max.poll.records} is set to
 * {@code ledger.dlq-replay.max-batch-size} (default 50), so one {@link Consumer#poll} can never
 * physically return more than that many records regardless of what the caller asked for.
 * {@code maxRecords} additionally clamps that ceiling DOWN per call, and only the offsets of the
 * records actually taken are committed - any remainder from the same {@code poll()} is left
 * uncommitted and is read again (and re-clamped) on the NEXT call, so no record is ever skipped by
 * an over-eager commit.
 */
@Component
public class KafkaDlqReader implements DlqReader {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqReader.class);

    private final ConsumerFactory<String, byte[]> dlqReplayConsumerFactory;
    private final String dlqTopic;
    private final int maxBatchSizeCeiling;
    private final Duration pollTimeout;

    public KafkaDlqReader(
            @Qualifier("dlqReplayConsumerFactory") ConsumerFactory<String, byte[]> dlqReplayConsumerFactory,
            @Value("${ledger.dlq-replay.dlq-topic}") String dlqTopic,
            @Value("${ledger.dlq-replay.max-batch-size}") int maxBatchSizeCeiling,
            @Value("${ledger.dlq-replay.poll-timeout-ms}") long pollTimeoutMs) {
        this.dlqReplayConsumerFactory = dlqReplayConsumerFactory;
        this.dlqTopic = dlqTopic;
        this.maxBatchSizeCeiling = maxBatchSizeCeiling;
        this.pollTimeout = Duration.ofMillis(pollTimeoutMs);
    }

    @Override
    public List<DlqRecord> pollBatch(int maxRecords) {
        int effectiveMax = Math.max(1, Math.min(maxRecords, maxBatchSizeCeiling));

        try (Consumer<String, byte[]> consumer = dlqReplayConsumerFactory.createConsumer()) {
            consumer.subscribe(List.of(dlqTopic));
            // Triggers the initial partition assignment (a bare subscribe() does not poll) so the
            // real poll below can return data on the very first call.
            consumer.poll(Duration.ZERO);

            ConsumerRecords<String, byte[]> polled = consumer.poll(pollTimeout);

            List<DlqRecord> toReplay = new ArrayList<>();
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            for (ConsumerRecord<String, byte[]> record : polled) {
                if (toReplay.size() >= effectiveMax) {
                    break;
                }
                toReplay.add(toDlqRecord(record));
                offsetsToCommit.merge(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1),
                        (existing, candidate) -> candidate.offset() > existing.offset() ? candidate : existing);
            }

            if (!offsetsToCommit.isEmpty()) {
                consumer.commitSync(offsetsToCommit);
            }

            log.info("DLQ replay read {} record(s) from {} (requested={})", toReplay.size(), dlqTopic, maxRecords);
            return toReplay;
        }
    }

    private static DlqRecord toDlqRecord(ConsumerRecord<String, byte[]> record) {
        List<DlqHeader> headers = new ArrayList<>();
        record.headers().forEach(header -> headers.add(new DlqHeader(header.key(), header.value())));
        return new DlqRecord(record.key(), record.value(), headers);
    }
}
