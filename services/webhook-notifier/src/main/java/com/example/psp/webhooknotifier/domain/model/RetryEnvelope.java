package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;

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

    public static RetryEnvelope initial() {
        return new RetryEnvelope(1, null, null, null, null, null, null, null, null);
    }

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
