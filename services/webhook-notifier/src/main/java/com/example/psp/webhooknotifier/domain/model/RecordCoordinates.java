package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;

/**
 * Where a Kafka record physically lives - topic, partition, offset, broker-assigned timestamp.
 * Deliberately NOT {@code org.apache.kafka.common.TopicPartition} or anything else from
 * {@code org.apache.kafka..}: {@code application/} is allowed to know a record has coordinates
 * without depending on the Kafka client library that models them (ADR-0007). The inbound
 * listener ({@code adapters.in.kafka.WebhookDeliveryExecutorListener}) builds one of these per
 * record from {@code @Header} bindings; {@code application.ExecuteWebhookDeliveryUseCase} uses it
 * only to stamp {@link RetryEnvelope}'s {@code x-original-*} headers the first time a delivery
 * enters the retry chain (ADR-0006).
 */
public record RecordCoordinates(String topic, int partition, long offset, Instant timestamp) {}
