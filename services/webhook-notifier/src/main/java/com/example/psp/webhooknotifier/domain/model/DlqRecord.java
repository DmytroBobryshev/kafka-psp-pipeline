package com.example.psp.webhooknotifier.domain.model;

/**
 * One record read back off the DLQ topic by {@code domain.port.DlqReader}, ready for
 * {@code application.ReplayDlqUseCase} to republish to {@link RetryChain#baseTopic()}.
 *
 * @param key      the record's Kafka key - the merchant id, preserved unchanged on replay so the
 *                 republished record keeps ADR-0003's partitioning (ADR-0006's replay contract).
 * @param command  the delivery command payload.
 * @param envelope the retry envelope as read back from the DLQ record's headers (attempt count,
 *                 original coordinates, the exception that landed it in the DLQ) - replay adds
 *                 {@code x-replayed-from}/{@code x-replay-count} on top via
 *                 {@link RetryEnvelope#withReplay}, it does not reset any of this.
 */
public record DlqRecord(String key, WebhookDeliveryCommand command, RetryEnvelope envelope) {}
