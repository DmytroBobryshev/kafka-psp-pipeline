package com.example.psp.analytics.domain.port;

public class PartialBatchWriteException extends RuntimeException {

    private final int failedIndex;

    public PartialBatchWriteException(String message, int failedIndex, Throwable cause) {
        super(message, cause);
        this.failedIndex = failedIndex;
    }

    public int failedIndex() {
        return failedIndex;
    }
}
