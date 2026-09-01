package com.example.psp.webhooknotifier.domain.model;

public record DlqRecord(String key, WebhookDeliveryCommand command, RetryEnvelope envelope) {}
