package com.example.psp.pspconnector.domain.exception;

public abstract class RetryableException extends RuntimeException {

    protected RetryableException(String message) {
        super(message);
    }
}
