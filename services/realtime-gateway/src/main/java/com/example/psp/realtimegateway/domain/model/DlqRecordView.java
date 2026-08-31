package com.example.psp.realtimegateway.domain.model;

import java.time.Instant;
import java.util.Map;

/**
 * One record read back by M17 page 3's generic DLQ browse -
 * {@code domain.port.DlqBrowser#peekLast}. Non-destructive: producing this record never commits
 * an offset (see that port's javadoc).
 *
 * @param keyString    the record key decoded as UTF-8, leniently (a garbled key renders with the
 *                      Unicode replacement character rather than failing the whole peek) - same
 *                      convention {@code webhook-notifier}'s {@code KafkaDlqReader} uses for
 *                      headers.
 * @param headers      every header decoded as UTF-8, keyed by header name. THE point of this
 *                      endpoint for a DLQ populated by Spring's {@code DeadLetterPublishingRecoverer}:
 *                      {@code kafka_dlt-exception-message}, {@code kafka_dlt-exception-stacktrace},
 *                      {@code kafka_dlt-original-topic}, and friends all arrive here.
 * @param valuePreview the value bytes as a UTF-8 string (truncated at 2048 characters) when they
 *                      decode as valid, printable UTF-8/JSON; otherwise the value's Base64
 *                      encoding. DLQ values are Avro binary on some of this cluster's DLQs
 *                      (docs/diagrams/topic-map.md), and this endpoint deliberately never attempts
 *                      to Avro-decode them - it has no schema-registry dependency at all.
 * @param valueBase64   {@code true} when {@code valuePreview} holds Base64, not text.
 */
public record DlqRecordView(
        String topic,
        int partition,
        long offset,
        Instant timestamp,
        String keyString,
        Map<String, String> headers,
        String valuePreview,
        boolean valueBase64) {}
