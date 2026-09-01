package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.pspconnector.domain.model.DlqHeader;
import com.example.psp.pspconnector.domain.model.DlqRecord;
import com.example.psp.pspconnector.domain.port.DlqReader;
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

@Component
public class KafkaDlqReader implements DlqReader {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqReader.class);

    private final ConsumerFactory<String, byte[]> dlqReplayConsumerFactory;
    private final String dlqTopic;
    private final int maxBatchSizeCeiling;
    private final Duration pollTimeout;

    public KafkaDlqReader(
            @Qualifier("dlqReplayConsumerFactory") ConsumerFactory<String, byte[]> dlqReplayConsumerFactory,
            @Value("${psp-connector.dlq-replay.dlq-topic}") String dlqTopic,
            @Value("${psp-connector.dlq-replay.max-batch-size}") int maxBatchSizeCeiling,
            @Value("${psp-connector.dlq-replay.poll-timeout-ms}") long pollTimeoutMs) {
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
