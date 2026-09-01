package com.example.psp.pspconnector.domain.exception;

import java.util.UUID;

public class ProviderTimeoutException extends RetryableException {

    public ProviderTimeoutException(UUID paymentId) {
        super("provider timed out authorizing paymentId=" + paymentId);
    }
}
