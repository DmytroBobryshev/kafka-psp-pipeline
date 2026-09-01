package com.example.psp.webhooknotifier.domain.model;

public final class RetryHeaderNames {

    public static final String ATTEMPT_COUNT = "x-attempt-count";

    public static final String ORIGINAL_TOPIC = "x-original-topic";

    public static final String ORIGINAL_PARTITION = "x-original-partition";

    public static final String ORIGINAL_OFFSET = "x-original-offset";

    public static final String ORIGINAL_TIMESTAMP = "x-original-timestamp";

    public static final String EXCEPTION_FQCN = "x-exception-fqcn";

    public static final String EXCEPTION_MESSAGE = "x-exception-message";

    public static final String EXCEPTION_STACKTRACE = "x-exception-stacktrace";

    public static final String FAILED_AT = "x-failed-at";

    public static final String REPLAYED_FROM = "x-replayed-from";

    public static final String REPLAY_COUNT = "x-replay-count";

    private RetryHeaderNames() {}
}
