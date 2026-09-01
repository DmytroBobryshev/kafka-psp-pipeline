package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public final class RetryHeaderCodec {

    private RetryHeaderCodec() {}

    public static Map<String, String> encode(RetryEnvelope envelope) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(RetryHeaderNames.ATTEMPT_COUNT, String.valueOf(envelope.attemptCount()));
        if (envelope.originalTopic() != null) {
            headers.put(RetryHeaderNames.ORIGINAL_TOPIC, envelope.originalTopic());
            headers.put(RetryHeaderNames.ORIGINAL_PARTITION, String.valueOf(envelope.originalPartition()));
            headers.put(RetryHeaderNames.ORIGINAL_OFFSET, String.valueOf(envelope.originalOffset()));
            headers.put(RetryHeaderNames.ORIGINAL_TIMESTAMP, envelope.originalTimestamp().toString());
        }
        if (envelope.exceptionFqcn() != null) {
            headers.put(RetryHeaderNames.EXCEPTION_FQCN, envelope.exceptionFqcn());
            headers.put(
                    RetryHeaderNames.EXCEPTION_MESSAGE,
                    envelope.exceptionMessage() == null ? "" : envelope.exceptionMessage());
        }
        if (envelope.replayedFrom() != null) {
            headers.put(RetryHeaderNames.REPLAYED_FROM, envelope.replayedFrom());
            headers.put(RetryHeaderNames.REPLAY_COUNT, String.valueOf(envelope.replayCount()));
        }
        return headers;
    }

    public static RetryEnvelope decode(Function<String, String> headerLookup) {
        String attemptCountRaw = headerLookup.apply(RetryHeaderNames.ATTEMPT_COUNT);
        int attemptCount = attemptCountRaw == null ? 1 : Integer.parseInt(attemptCountRaw);

        String originalTopic = headerLookup.apply(RetryHeaderNames.ORIGINAL_TOPIC);
        Integer originalPartition = null;
        Long originalOffset = null;
        Instant originalTimestamp = null;
        if (originalTopic != null) {
            originalPartition = Integer.valueOf(headerLookup.apply(RetryHeaderNames.ORIGINAL_PARTITION));
            originalOffset = Long.valueOf(headerLookup.apply(RetryHeaderNames.ORIGINAL_OFFSET));
            originalTimestamp = Instant.parse(headerLookup.apply(RetryHeaderNames.ORIGINAL_TIMESTAMP));
        }

        String exceptionFqcn = headerLookup.apply(RetryHeaderNames.EXCEPTION_FQCN);
        String exceptionMessage =
                exceptionFqcn == null ? null : headerLookup.apply(RetryHeaderNames.EXCEPTION_MESSAGE);

        String replayedFrom = headerLookup.apply(RetryHeaderNames.REPLAYED_FROM);
        Integer replayCount =
                replayedFrom == null ? null : Integer.valueOf(headerLookup.apply(RetryHeaderNames.REPLAY_COUNT));

        return new RetryEnvelope(
                attemptCount,
                originalTopic,
                originalPartition,
                originalOffset,
                originalTimestamp,
                exceptionFqcn,
                exceptionMessage,
                replayedFrom,
                replayCount);
    }
}
