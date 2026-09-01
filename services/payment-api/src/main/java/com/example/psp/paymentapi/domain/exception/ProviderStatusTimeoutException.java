package com.example.psp.paymentapi.domain.exception;

import java.util.UUID;

public class ProviderStatusTimeoutException extends RuntimeException {

    public ProviderStatusTimeoutException(UUID paymentId, Throwable cause) {
        super("provider-status-query timed out waiting for a reply for paymentId=" + paymentId, cause);
    }
}
