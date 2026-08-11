package com.example.psp.webhooknotifier.domain.model;

/**
 * The literal Kafka header names this service's hand-rolled retry chain reads and writes,
 * verbatim from ADR-0006's "DLQ records MUST carry ... " list. Centralised here (not in an
 * adapter) so the two adapters that touch them -
 * {@code adapters.out.kafka.KafkaWebhookDeliveryPublisher} (writer) and
 * {@code adapters.in.kafka.WebhookDeliveryExecutorListener} / {@code adapters.out.kafka.KafkaDlqReader}
 * (readers) - cannot drift apart on a header's name. Pure string constants, no Kafka import, so
 * living in {@code domain/} does not violate ADR-0007's "domain must not know Kafka exists".
 *
 * <h2>Why these are not Spring Kafka's own header constants</h2>
 *
 * <p>{@code @RetryableTopic} - Spring Kafka's built-in non-blocking retry mechanism - adds its
 * own headers (e.g. {@code kafka_retryTopic-attempts}, {@code kafka_original-topic} via
 * {@code KafkaHeaders}/{@code RetryTopicHeaders}) and derives retry-topic names from a single
 * suffix plus an auto-incrementing index or delay value. This module cannot use it: the retry
 * topics already exist with the literal names {@code .retry.5s}/{@code .retry.1m}/{@code .retry.15m}
 * (docs/diagrams/topic-map.md), and Spring's suffix-derivation has no way to produce three
 * unrelated literal suffixes for one base topic. See {@code application.WebhookDeliveryRetryPolicy}
 * (formerly considered "just use {@code @RetryableTopic}") and the README's "Why not
 * {@code @RetryableTopic}" section for the full reasoning. This service is therefore the
 * "explicit equivalent" the M8 brief allows, and it adopts ADR-0006's OWN header vocabulary
 * (below) rather than guessing at Spring's internal constant names - which also means these
 * headers read exactly as the ADR that mandates them describes.
 *
 * <p>The container-level path (a deserialization failure or an unclassified bug reaching
 * {@code DefaultErrorHandler}) is handled separately, by Spring's own
 * {@code DeadLetterPublishingRecoverer} - a standalone class, not part of
 * {@code @RetryableTopic} - which adds its own standard headers automatically. See
 * {@code config.KafkaConsumerConfig}.
 */
public final class RetryHeaderNames {

    /** Current attempt number, 1 for the very first delivery (the base topic). */
    public static final String ATTEMPT_COUNT = "x-attempt-count";

    /** Topic of the FIRST attempt for this delivery - set once, never overwritten on later hops. */
    public static final String ORIGINAL_TOPIC = "x-original-topic";

    /** Partition of the first attempt. */
    public static final String ORIGINAL_PARTITION = "x-original-partition";

    /** Offset of the first attempt. */
    public static final String ORIGINAL_OFFSET = "x-original-offset";

    /** Broker timestamp of the first attempt. */
    public static final String ORIGINAL_TIMESTAMP = "x-original-timestamp";

    /** Fully-qualified class name of the failure that caused this hop. */
    public static final String EXCEPTION_FQCN = "x-exception-fqcn";

    /** {@code Throwable#getMessage()} (or the HTTP error detail) of the failure that caused this hop. */
    public static final String EXCEPTION_MESSAGE = "x-exception-message";

    /** Full stack trace text of the failure, present only on the terminal DLQ record. */
    public static final String EXCEPTION_STACKTRACE = "x-exception-stacktrace";

    /** ISO-8601 instant this delivery was written to the DLQ - absent on every non-DLQ hop. */
    public static final String FAILED_AT = "x-failed-at";

    /** Present only on a record republished by the DLQ replay endpoint: the DLQ topic it came from. */
    public static final String REPLAYED_FROM = "x-replayed-from";

    /** Present only on a replayed record: how many times this logical delivery has been replayed. */
    public static final String REPLAY_COUNT = "x-replay-count";

    private RetryHeaderNames() {}
}
