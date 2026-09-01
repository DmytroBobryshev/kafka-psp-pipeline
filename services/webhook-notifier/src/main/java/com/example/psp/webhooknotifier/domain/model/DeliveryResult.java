package com.example.psp.webhooknotifier.domain.model;

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
