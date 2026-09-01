package com.example.psp.realtimegateway.domain.exception;

public class ClusterOperationException extends RuntimeException {

    public ClusterOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
