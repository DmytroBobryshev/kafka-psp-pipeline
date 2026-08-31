package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.pspconnector.domain.model.DlqHeader;
import com.example.psp.pspconnector.domain.model.DlqRecord;
import com.example.psp.pspconnector.domain.port.DlqRepublisher;
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
 * {@code payments.payment-requested.v1} through a dedicated, plain byte-array
 * {@link KafkaTemplate} ({@code config.KafkaProducerConfig#dlqReplayKafkaTemplate}) - NOT this
 * service's normal {@code KafkaTemplate<String, Object>}, whose {@code value-serializer} is
 * {@code KafkaAvroSerializer}. That serializer needs an Avro-typed object to encode, and this
 * adapter only ever holds the DLQ record's already-encoded raw bytes (see
 * {@code domain.model.DlqRecord}'s javadoc) - sending those bytes through unchanged, key and
 * headers included, is what makes this republish byte-for-byte identical to the original record.
 *
 * <p>Blocks on the send, same as {@code KafkaPaymentStatusPublisher}: a REST-triggered, bounded
 * batch operation should surface a failed republish to its caller rather than silently losing it -
 * the same M19 drill 9 lesson that publisher's javadoc documents.
 */
@Component
public class KafkaDlqRepublisher implements DlqRepublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqRepublisher.class);

    private final KafkaTemplate<String, byte[]> dlqReplayKafkaTemplate;
    private final String targetTopic;

    public KafkaDlqRepublisher(
            @Qualifier("dlqReplayKafkaTemplate") KafkaTemplate<String, byte[]> dlqReplayKafkaTemplate,
            @Value("${psp-connector.kafka.payment-requested-topic}") String targetTopic) {
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
