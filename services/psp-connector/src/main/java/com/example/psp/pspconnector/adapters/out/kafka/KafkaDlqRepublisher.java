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
