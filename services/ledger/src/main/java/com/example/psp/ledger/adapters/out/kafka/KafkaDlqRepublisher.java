package com.example.psp.ledger.adapters.out.kafka;

import com.example.psp.ledger.domain.model.DlqHeader;
import com.example.psp.ledger.domain.model.DlqRecord;
import com.example.psp.ledger.domain.port.DlqRepublisher;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Real Kafka adapter for {@link DlqRepublisher}. Republishes a DLQ record to
 * {@code payments.payment-status-changed.v1} through a dedicated, PLAIN (non-transactional) byte-
 * array {@link KafkaTemplate} ({@code config.KafkaProducerConfig#dlqReplayKafkaTemplate}) - never
 * the service's transactional {@code KafkaTemplate<String, Object>}
 * ({@code config.KafkaProducerConfig#ledgerEntryProducerFactory}, M7's exactly-once producer).
 *
 * <h2>Why this must NOT be the transactional producer</h2>
 *
 * <p>{@code ledgerEntryProducerFactory}'s {@code KafkaTemplate} only ever sends successfully
 * <em>inside</em> a Kafka transaction already opened by the listener container
 * ({@code config.KafkaConsumerConfig#paymentStatusChangedKafkaListenerContainerFactory}) - calling
 * {@code send()} on it from anywhere else throws {@code IllegalStateException} (see
 * {@code adapters.out.kafka.KafkaLedgerEntryPublisher}'s javadoc, "This send is inside a
 * transaction"). {@code adapters.in.web.DlqReplayController} runs on a plain HTTP request thread
 * with no listener-container transaction open around it, so reusing that template here would fail
 * every call outright. A dedicated, transaction-free producer sidesteps the question entirely -
 * exactly the task's explicit constraint: "keep ledger's transactional producer out of this path".
 *
 * <h2>Raw bytes, not the decoded Avro record</h2>
 *
 * <p>This adapter only ever holds the DLQ record's already-encoded raw bytes (see
 * {@code domain.model.DlqRecord}'s javadoc) - {@code KafkaAvroSerializer} needs an Avro-typed
 * object to encode and has no escape hatch for a byte[] it was never asked to decode in the first
 * place, so {@code dlqReplayKafkaTemplate} is built with a plain {@code ByteArraySerializer}
 * instead. Sending those bytes through unchanged, key and headers included, is what makes this
 * republish byte-for-byte identical to the original record.
 *
 * <p>Blocks on the send, same as {@code KafkaPaymentStatusPublisher} on the psp-connector side: a
 * REST-triggered, bounded batch operation should surface a failed republish to its caller rather
 * than silently losing it.
 */
@Component
public class KafkaDlqRepublisher implements DlqRepublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqRepublisher.class);

    private final KafkaTemplate<String, byte[]> dlqReplayKafkaTemplate;
    private final String targetTopic;

    public KafkaDlqRepublisher(
            @Qualifier("dlqReplayKafkaTemplate") KafkaTemplate<String, byte[]> dlqReplayKafkaTemplate,
            @Value("${ledger.kafka.payment-status-changed-topic}") String targetTopic) {
        this.dlqReplayKafkaTemplate = dlqReplayKafkaTemplate;
        this.targetTopic = targetTopic;
    }

    @Override
    public void republish(DlqRecord record) {
        ProducerRecord<String, byte[]> producerRecord =
                new ProducerRecord<>(targetTopic, record.key(), record.value());
        for (DlqHeader header : record.headers()) {
            producerRecord.headers().add(header.key(), header.value());
        }

        try {
            SendResult<String, byte[]> result = dlqReplayKafkaTemplate.send(producerRecord).get();
            RecordMetadata metadata = result.getRecordMetadata();
            log.info(
                    "Replayed DLQ record key={} to {} partition={} offset={}",
                    record.key(),
                    targetTopic,
                    metadata.partition(),
                    metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("interrupted while replaying to " + targetTopic + " key=" + record.key(), e);
        } catch (ExecutionException e) {
            throw new KafkaException("failed to replay to " + targetTopic + " key=" + record.key(), e.getCause());
        }
    }
}
