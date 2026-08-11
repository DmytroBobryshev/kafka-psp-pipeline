package com.example.psp.webhooknotifier.domain.model;

/**
 * The raw result of one call to {@code domain.port.MerchantWebhookClient#deliver}. Deliberately a
 * returned value, not a thrown exception - same shape as psp-connector's {@code ProviderResult}
 * returning a three-way {@code ProviderOutcome} rather than throwing for a timeout - so the use
 * case decides what to do with a failure instead of unwinding the stack to find out.
 *
 * @param outcome      the ADR-0006 classification of this attempt.
 * @param statusCode   the HTTP status code received, or {@code null} if the call never got a
 *                     response at all (connection refused/reset, read timeout).
 * @param errorMessage human-readable failure detail for the Mongo attempt log and, if this
 *                     attempt exhausts the retry chain, the DLQ's {@code x-exception-message}
 *                     header. {@code null} on {@link DeliveryOutcome#SUCCESS}.
 */
public record DeliveryResult(DeliveryOutcome outcome, Integer statusCode, String errorMessage) {

    public static DeliveryResult success(int statusCode) {
        return new DeliveryResult(DeliveryOutcome.SUCCESS, statusCode, null);
    }

    public static DeliveryResult retryable(Integer statusCode, String errorMessage) {
        return new DeliveryResult(DeliveryOutcome.RETRYABLE_FAILURE, statusCode, errorMessage);
    }

    public static DeliveryResult nonRetryable(Integer statusCode, String errorMessage) {
        return new DeliveryResult(DeliveryOutcome.NON_RETRYABLE_FAILURE, statusCode, errorMessage);
    }
}
