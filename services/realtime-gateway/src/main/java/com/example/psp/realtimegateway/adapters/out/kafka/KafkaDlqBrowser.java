package com.example.psp.realtimegateway.adapters.out.kafka;

import com.example.psp.realtimegateway.domain.exception.ClusterOperationException;
import com.example.psp.realtimegateway.domain.model.DlqRecordView;
import com.example.psp.realtimegateway.domain.port.DlqBrowser;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

/**
 * Real {@link DlqBrowser} adapter for M17 page 3's generic DLQ browse - a non-destructive peek at
 * the last {@code max} records of a {@code *.dlq} topic.
 *
 * <h2>Why this is safe to call repeatedly without "consuming" anything</h2>
 *
 * <p>A fresh {@link Consumer} is created per call (via {@code config.ClusterAdminConfig}'s
 * {@code dlqPeekConsumerFactory}) and closed at the end of the same call. It never calls
 * {@code subscribe()} and never joins a consumer group - {@code group.id} is deliberately absent
 * from that factory's properties (see its javadoc), and this class uses {@code assign()} to attach
 * directly to every partition of the target topic instead. With no group membership, there is
 * nothing to commit and nothing recorded in {@code __consumer_offsets}: two peeks a second apart
 * see the same tail of the DLQ (modulo new records actually landing there), never an
 * already-"consumed" gap. This is also why the ACL grant for this class
 * (see {@code infra/k8s/kafka/users/15-realtime-gateway.yaml}) is {@code Read}/{@code Describe} on
 * specific DLQ topics ONLY - no consumer-group ACL entry exists for it at all.
 *
 * <h2>"Last max records", mechanically</h2>
 *
 * <ol>
 *   <li>{@code partitionsFor} + {@code assign} attach to every partition of the topic.
 *   <li>{@code beginningOffsets}/{@code endOffsets} bound each partition.
 *   <li>Each partition is seeked to {@code max(beginning, end - share)}, where {@code share} is an
 *       even split of {@code max} across the partition count - so a topic with more partitions
 *       than {@code max} still gets at least one record's worth of seek room per partition.
 *   <li>{@code poll()} is called at most twice (a single poll can legitimately return less than a
 *       full batch even when more data is available), and every record actually returned is
 *       sorted by timestamp and trimmed down to the requested {@code max}.
 * </ol>
 *
 * <p>This is a best-effort approximation of a single global "last N" ordering across multiple
 * independent partitions, not a strict guarantee - exactly like every other multi-partition
 * "recent records" view in this kind of tool (AKHQ included). It is exact for a single-partition
 * DLQ, which is what every DLQ in this cluster's topic-map is provisioned as today.
 */
@Component
public class KafkaDlqBrowser implements DlqBrowser {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqBrowser.class);

    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);
    private static final int MAX_POLL_ATTEMPTS = 2;
    private static final int MAX_PREVIEW_CHARS = 2048;

    private final ConsumerFactory<byte[], byte[]> consumerFactory;

    public KafkaDlqBrowser(@Qualifier("dlqPeekConsumerFactory") ConsumerFactory<byte[], byte[]> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Override
    public List<DlqRecordView> peekLast(String topic, int max) {
        try (Consumer<byte[], byte[]> consumer = consumerFactory.createConsumer()) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, ADMIN_TIMEOUT);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                log.info("DLQ peek found no partitions for topic={} (does it exist?)", topic);
                return List.of();
            }

            List<TopicPartition> partitions =
                    partitionInfos.stream().map(pi -> new TopicPartition(topic, pi.partition())).toList();
            consumer.assign(partitions);

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions, ADMIN_TIMEOUT);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions, ADMIN_TIMEOUT);

            int perPartitionShare = Math.max(1, ceilDiv(max, partitions.size()));
            boolean anyData = false;
            for (TopicPartition partition : partitions) {
                long begin = beginningOffsets.getOrDefault(partition, 0L);
                long end = endOffsets.getOrDefault(partition, begin);
                consumer.seek(partition, Math.max(begin, end - perPartitionShare));
                anyData |= end > begin;
            }
            if (!anyData) {
                log.info("DLQ peek: topic={} is empty (no records on any partition)", topic);
                return List.of();
            }

            List<ConsumerRecord<byte[], byte[]>> collected = new ArrayList<>();
            for (int attempt = 0; attempt < MAX_POLL_ATTEMPTS && collected.size() < max; attempt++) {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(POLL_TIMEOUT);
                polled.forEach(collected::add);
                if (polled.isEmpty()) {
                    break;
                }
            }

            List<DlqRecordView> result =
                    collected.stream()
                            .sorted(Comparator.comparingLong(ConsumerRecord::timestamp))
                            .skip(Math.max(0, collected.size() - max))
                            .map(KafkaDlqBrowser::toView)
                            .toList();
            log.info("DLQ peek read {} record(s) from topic={} (requested max={})", result.size(), topic, max);
            return result;
        } catch (KafkaException e) {
            throw new ClusterOperationException("DLQ peek failed for topic=" + topic, e);
        }
    }

    private static int ceilDiv(int numerator, int denominator) {
        return (numerator + denominator - 1) / denominator;
    }

    private static DlqRecordView toView(ConsumerRecord<byte[], byte[]> record) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), decodeLenient(header.value()));
        }
        ValuePreview preview = previewValue(record.value());
        return new DlqRecordView(
                record.topic(),
                record.partition(),
                record.offset(),
                Instant.ofEpochMilli(record.timestamp()),
                record.key() == null ? null : decodeLenient(record.key()),
                headers,
                preview.text(),
                preview.base64());
    }

    /** Same convention as {@code webhook-notifier}'s {@code KafkaDlqReader#headerAsString}: never throws. */
    private static String decodeLenient(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Strict UTF-8 decode (unlike {@link #decodeLenient}) because a value that is NOT valid UTF-8
     * must fall through to Base64, not silently render as replacement characters -
     * {@link DlqRecordView}'s contract is "text OR base64, flagged", never "corrupted text".
     */
    private static ValuePreview previewValue(byte[] value) {
        if (value == null) {
            return new ValuePreview(null, false);
        }
        CharsetDecoder decoder =
                StandardCharsets.UTF_8
                        .newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            CharBuffer decoded = decoder.decode(ByteBuffer.wrap(value));
            String text = decoded.toString();
            if (isPrintable(text)) {
                String truncated = text.length() > MAX_PREVIEW_CHARS ? text.substring(0, MAX_PREVIEW_CHARS) : text;
                return new ValuePreview(truncated, false);
            }
        } catch (CharacterCodingException notUtf8) {
            // Falls through to Base64 below - this is the expected path for Avro-binary DLQ
            // values (see this class's and DlqRecordView's javadoc): never re-thrown, never logged
            // as an error, it is simply "not text".
        }
        return new ValuePreview(Base64.getEncoder().encodeToString(value), true);
    }

    /** Rejects control characters (tab/CR/LF excepted) - valid UTF-8 that is still binary garbage. */
    private static boolean isPrintable(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isISOControl(c) && c != '\n' && c != '\r' && c != '\t') {
                return false;
            }
        }
        return true;
    }

    private record ValuePreview(String text, boolean base64) {}
}
