package com.example.psp.paymentapi.domain.exception;

import java.util.UUID;

/**
 * Thrown by {@code adapters.out.kafka.ProviderStatusRequestGateway} when
 * {@code ReplyingKafkaTemplate}'s future does not complete within
 * {@code config.ReplyingKafkaConfig}'s configured reply timeout - see that class's javadoc,
 * "What happens when a reply never arrives", for what a timeout here usually means and why the
 * timeout value was chosen where it was.
 *
 * <p>A plain domain exception, not a leaked Kafka type - {@code domain/} must not depend on Kafka
 * (ADR-0007), and this is the translation point (same shape as psp-connector's
 * {@code ProviderTimeoutException}, a different exception for a different leg of the same kind of
 * "the far side did not answer in time" situation).
 */
public class ProviderStatusTimeoutException extends RuntimeException {

    public ProviderStatusTimeoutException(UUID paymentId, Throwable cause) {
        super("provider-status-query timed out waiting for a reply for paymentId=" + paymentId, cause);
    }
}
