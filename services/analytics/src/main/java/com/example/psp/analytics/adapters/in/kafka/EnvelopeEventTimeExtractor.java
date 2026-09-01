package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvelopeEventTimeExtractor implements TimestampExtractor {

    private static final Logger log = LoggerFactory.getLogger(EnvelopeEventTimeExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();

        Instant occurredAt = null;
        if (value instanceof PaymentStatusChanged event && event.getEnvelope() != null) {
            occurredAt = event.getEnvelope().getOccurredAt();
        } else if (value instanceof PaymentRequested event && event.getEnvelope() != null) {
            occurredAt = event.getEnvelope().getOccurredAt();
        }

        if (occurredAt != null && occurredAt.toEpochMilli() > 0L) {
            return occurredAt.toEpochMilli();
        }

        long fallback = record.timestamp();
        log.debug(
                "No usable envelope.occurredAt on {}-{}@{}; falling back to the broker timestamp {}",
                record.topic(),
                record.partition(),
                record.offset(),
                fallback);
        // Still guard the fallback: a negative timestamp makes Streams drop the record silently.
        return fallback >= 0L ? fallback : partitionTime;
    }
}
