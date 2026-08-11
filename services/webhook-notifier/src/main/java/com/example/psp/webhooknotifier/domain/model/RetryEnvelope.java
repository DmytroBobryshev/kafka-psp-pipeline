package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;

/**
 * Everything the retry chain needs to carry alongside a {@link WebhookDeliveryCommand} on every
 * hop, mirrored to/from the Kafka headers named in {@link RetryHeaderNames} by
 * {@code adapters.out.kafka.KafkaWebhookDeliveryPublisher} (write) and
 * {@code adapters.in.kafka.WebhookDeliveryExecutorListener} / {@code adapters.out.kafka.KafkaDlqReader}
 * (read).
 *
 * <p>Immutable; every hop through the chain produces a new instance via the {@code with*}
 * methods rather than mutating one shared object - important because a paused/pending scheduled
 * hop ({@code adapters.out.kafka.KafkaWebhookDeliveryPublisher#publishDelayed}) must publish
 * exactly the envelope that was true at the moment of failure, not whatever the field held by the
 * time the scheduled task actually runs.
 *
 * @param attemptCount        1 for the very first delivery attempt (the base topic), incremented
 *                            by {@link #nextAttempt()} on every hop.
 * @param originalTopic       topic of the first attempt, {@code null} until the first failure
 *                            stamps it (see {@link #withOriginalIfAbsent}).
 * @param originalPartition   partition of the first attempt.
 * @param originalOffset      offset of the first attempt.
 * @param originalTimestamp   broker timestamp of the first attempt.
 * @param exceptionFqcn       classification of the failure that produced THIS hop, {@code null}
 *                            on the very first (not-yet-failed) publish.
 * @param exceptionMessage    human-readable detail of that failure.
 * @param replayedFrom        the DLQ topic this delivery was replayed from, {@code null} unless
 *                            it has been through the DLQ replay endpoint at least once.
 * @param replayCount         how many times this logical delivery has been replayed, {@code null}
 *                            unless {@code replayedFrom} is set.
 */
public record RetryEnvelope(
        int attemptCount,
        String originalTopic,
        Integer originalPartition,
        Long originalOffset,
        Instant originalTimestamp,
        String exceptionFqcn,
        String exceptionMessage,
        String replayedFrom,
        Integer replayCount) {

    /** The envelope for a delivery command that has never failed: attempt 1, no original-*, no exception. */
    public static RetryEnvelope initial() {
        return new RetryEnvelope(1, null, null, null, null, null, null, null, null);
    }

    /**
     * Stamps {@code original-*} from {@code coordinates} - but only if they are not already set.
     * Called on every failure; a no-op from the second failure onward, which is exactly what makes
     * "original" mean "the first attempt", not "the most recent retry hop" (matching the semantics
     * of Spring's own {@code KafkaHeaders.ORIGINAL_TOPIC} et al.).
     */
    public RetryEnvelope withOriginalIfAbsent(RecordCoordinates coordinates) {
        if (originalTopic != null) {
            return this;
        }
        return new RetryEnvelope(
                attemptCount,
                coordinates.topic(),
                coordinates.partition(),
                coordinates.offset(),
                coordinates.timestamp(),
                exceptionFqcn,
                exceptionMessage,
                replayedFrom,
                replayCount);
    }

    /**
     * Records a classified delivery failure as the cause of the next hop. {@code fqcn} is a short
     * classification label (e.g. {@code "merchant-4xx"}), not necessarily a real
     * {@code Throwable}'s class name - a merchant HTTP outcome is not a thrown exception in this
     * design (see {@code application.ExecuteWebhookDeliveryUseCase}, which classifies
     * {@code domain.model.DeliveryResult} directly rather than catching anything). Consequently
     * this chain never populates {@code x-exception-stacktrace} - there is no JVM stack trace for
     * an HTTP status code. That header IS populated, by Spring's own
     * {@code DeadLetterPublishingRecoverer}, for the one failure surface that IS a real thrown
     * exception: a deserialization failure or unclassified bug reaching the container's error
     * handler (see {@code config.KafkaConsumerConfig}).
     */
    public RetryEnvelope withFailure(String fqcn, String message) {
        return new RetryEnvelope(
                attemptCount,
                originalTopic,
                originalPartition,
                originalOffset,
                originalTimestamp,
                fqcn,
                message,
                replayedFrom,
                replayCount);
    }

    /** The envelope for the NEXT hop: same origin/exception info, attempt count incremented. */
    public RetryEnvelope nextAttempt() {
        return new RetryEnvelope(
                attemptCount + 1,
                originalTopic,
                originalPartition,
                originalOffset,
                originalTimestamp,
                exceptionFqcn,
                exceptionMessage,
                replayedFrom,
                replayCount);
    }

    /** Stamps replay provenance - called only by {@code application.ReplayDlqUseCase}. */
    public RetryEnvelope withReplay(String dlqTopic) {
        int newReplayCount = (replayCount == null ? 0 : replayCount) + 1;
        return new RetryEnvelope(
                attemptCount,
                originalTopic,
                originalPartition,
                originalOffset,
                originalTimestamp,
                exceptionFqcn,
                exceptionMessage,
                dlqTopic,
                newReplayCount);
    }
}
