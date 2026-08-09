package com.example.psp.pspconnector.domain.exception;

/**
 * ADR-0006 category A: transient infrastructure failure. Thrown from {@code application/} and
 * left to propagate all the way out of the Kafka listener method uncaught - {@code
 * adapters.in.kafka.PaymentRequestedListener} does not catch it, so
 * {@code Acknowledgment.acknowledge()} is never called for that record and the container's error
 * handler (see {@code config.KafkaConsumerConfig}) decides what happens next.
 *
 * <p>M4 scope note: the full non-blocking retry-topic chain
 * ({@code .retry.5s -> .retry.1m -> .retry.15m -> .dlq}) is explicitly M8. For now, "retryable"
 * means the container's {@code DefaultErrorHandler} re-delivers the same record a bounded number
 * of times before giving up and logging - a real gap (repeated failures currently get logged and
 * skipped rather than parked in a DLQ for manual triage), documented in the README's "Known
 * issues" rather than silently pretended away.
 */
public abstract class RetryableException extends RuntimeException {

    protected RetryableException(String message) {
        super(message);
    }
}
