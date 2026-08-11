package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes the windowed operations use <b>event time</b>, not ingest time: M10's 1-minute tumbling
 * windows on {@code payments.payment-status-changed.v1}, and (M13) the stream-stream join's
 * {@code JoinWindows} on both {@code payments.payment-status-changed.v1} and
 * {@code payments.payment-requested.v1}.
 *
 * <p>ADR-0002 chose an envelope-in-value design partly for this: "Streams windowing in M10 needs
 * event time, not ingest time" and "a {@code TimestampExtractor} reads {@code occurredAt}". This
 * class is the thing that ADR sentence was written for; M13 reuses it verbatim rather than
 * writing a second extractor, because both Avro records shape {@code envelope.occurredAt} the
 * same way.
 *
 * <p>The difference is not academic. Without it, Streams uses the record's broker timestamp, so a
 * consumer that falls 10 minutes behind and then catches up crams ten minutes of payments into
 * the one window it happened to be processing during - the aggregation would measure the
 * <i>consumer's</i> behaviour instead of the merchant's. With it, replaying the whole topic from
 * offset 0 reproduces exactly the same windows it produced live, which is what makes "rebuilt by
 * resetting offsets" (docs/diagrams/topic-map.md's justification for analytics having no DLQ) a
 * real recovery strategy rather than a slogan.
 *
 * <p>Event time also drives <b>stream time</b>, which is what advances windows and enforces the
 * grace period: stream time is the maximum event timestamp seen so far on a task, and a window
 * closes when stream time passes {@code windowEnd + grace}. Nothing about wall-clock time closes
 * a window - an idle topic leaves the last window open indefinitely, which is why the interactive
 * query below shows an open window rather than a missing one when traffic stops.
 *
 * <p>Fallback: on a null value (a tombstone, or a record the deserializer skipped) or an
 * implausible timestamp there is nothing to read, so the record's own broker timestamp is used.
 * Returning a negative number instead would make Streams drop the record silently.
 */
public class EnvelopeEventTimeExtractor implements TimestampExtractor {

    private static final Logger log = LoggerFactory.getLogger(EnvelopeEventTimeExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();

        Instant occurredAt = null;
        if (value instanceof PaymentStatusChanged event && event.getEnvelope() != null) {
            occurredAt = event.getEnvelope().getOccurredAt();
        } else if (value instanceof PaymentRequested event && event.getEnvelope() != null) {
            // M13: the join's other source topic. Same envelope shape, deliberately not a second
            // extractor class - see the class javadoc.
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
