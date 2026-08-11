package com.example.psp.webhooknotifier.adapters.out.kafka;

import com.example.psp.webhooknotifier.config.WebhookNotifierProperties;
import com.example.psp.webhooknotifier.domain.model.DlqRecord;
import com.example.psp.webhooknotifier.domain.model.RetryHeaderCodec;
import com.example.psp.webhooknotifier.domain.port.DlqReader;
import java.nio.charset.StandardCharsets;
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
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

/**
 * Real Kafka adapter for {@link DlqReader}. A short-lived {@link Consumer}, created fresh per
 * call under its own dedicated {@code group.id} ({@code webhook-notifier.dlq-replay.consumer-group}
 * - see {@code config.KafkaConsumerConfig#dlqReplayConsumerFactory}, entirely separate from the
 * executor's {@code webhook-notifier.executor.v1} group), reads at most one bounded batch, commits
 * exactly the offsets it actually processed, and closes.
 *
 * <h2>The guard, mechanically</h2>
 *
 * <p>The consumer factory's {@code max.poll.records} is set to
 * {@code webhook-notifier.dlq-replay.max-batch-size} (default 50), so one {@link Consumer#poll}
 * can never physically return more than that many records regardless of what the caller asked
 * for. {@code maxRecords} additionally clamps that ceiling DOWN per call, and only the offsets of
 * the records actually taken are committed - any remainder from the same {@code poll()} is left
 * uncommitted and is read again (and re-clamped) on the NEXT call, so no record is ever skipped
 * by an over-eager commit.
 *
 * <h2>A known limitation</h2>
 *
 * <p>If a DLQ record got there via the container-level poison-pill path
 * ({@code config.KafkaConsumerConfig}'s {@code DeadLetterPublishingRecoverer}), its VALUE is the
 * original bytes that failed to deserialize in the first place - by definition, still not valid
 * JSON for {@link WebhookDeliveryRequested}. This reader's own {@code ErrorHandlingDeserializer}
 * (configured on {@code dlqReplayConsumerFactory}, same as every other consumer factory per
 * ADR-0006) keeps that from looping the replay call forever, but such a record cannot be
 * meaningfully replayed until whatever produced the bad bytes is fixed - consistent with
 * ADR-0006's "whatever put a record there usually needs a code change first".
 */
@Component
public class KafkaDlqReader implements DlqReader {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqReader.class);

    private final ConsumerFactory<String, Object> dlqReplayConsumerFactory;
    private final WebhookDeliveryEventMapper mapper;
    private final String dlqTopic;
    private final int maxBatchSizeCeiling;
    private final Duration pollTimeout;

    public KafkaDlqReader(
            @Qualifier("dlqReplayConsumerFactory") ConsumerFactory<String, Object> dlqReplayConsumerFactory,
            WebhookDeliveryEventMapper mapper,
            WebhookNotifierProperties properties) {
        this.dlqReplayConsumerFactory = dlqReplayConsumerFactory;
        this.mapper = mapper;
        this.dlqTopic = properties.kafka().dlqTopic();
        this.maxBatchSizeCeiling = properties.dlqReplay().maxBatchSize();
        this.pollTimeout = Duration.ofMillis(properties.dlqReplay().pollTimeoutMs());
    }

    @Override
    public List<DlqRecord> pollBatch(int maxRecords) {
        int effectiveMax = Math.max(1, Math.min(maxRecords, maxBatchSizeCeiling));

        try (Consumer<String, Object> consumer = dlqReplayConsumerFactory.createConsumer()) {
            consumer.subscribe(List.of(dlqTopic));
            // Triggers the initial partition assignment (a bare subscribe() does not poll) so the
            // real poll below can return data on the very first call.
            consumer.poll(Duration.ZERO);

            ConsumerRecords<String, Object> polled = consumer.poll(pollTimeout);

            List<DlqRecord> toReplay = new ArrayList<>();
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            for (ConsumerRecord<String, Object> record : polled) {
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

    private DlqRecord toDlqRecord(ConsumerRecord<String, Object> record) {
        WebhookDeliveryRequested event = (WebhookDeliveryRequested) record.value();
        return new DlqRecord(
                record.key(), mapper.toCommand(event), RetryHeaderCodec.decode(name -> headerAsString(record, name)));
    }

    private static String headerAsString(ConsumerRecord<String, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
