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

@Component
public class KafkaLedgerEntryPublisher implements LedgerEntryPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaLedgerEntryPublisher.class);
    private static final String EVENT_TYPE = "ledger.ledger-entry-recorded.v1";
    private static final String SOURCE = "ledger";
    private static final String AGGREGATE_TYPE = "merchant";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final LedgerEntryAvroEventFactory avroEventFactory;
    private final String topic;

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
