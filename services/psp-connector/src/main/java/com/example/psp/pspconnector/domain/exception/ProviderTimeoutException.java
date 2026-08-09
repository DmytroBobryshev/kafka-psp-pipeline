package com.example.psp.pspconnector.domain.exception;

import java.util.UUID;

/**
 * The (simulated) provider did not respond in time. ADR-0006 category A - retryable, never
 * published as a domain event, never counted as a business outcome (contrast with
 * {@link com.example.psp.pspconnector.domain.model.ProviderOutcome#DECLINED}, which is a business
 * outcome and IS published).
 */
public class ProviderTimeoutException extends RetryableException {

    public ProviderTimeoutException(UUID paymentId) {
        super("provider timed out authorizing paymentId=" + paymentId);
    }
}
