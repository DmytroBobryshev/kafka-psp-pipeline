package com.example.psp.webhooknotifier.adapters.out.persistence;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * MongoDB document for one merchant's projected webhookUrl, {@code webhook_notifier.merchant_webhooks}
 * (ADR-0005, second collection in the same database as {@code delivery_attempts}). Mirrors
 * {@code merchants.merchant-config-changed.v1}'s {@code webhookUrl} field only - see
 * {@code adapters.in.kafka.MerchantConfigChangedListener}. {@code merchantId} doubles as the Mongo
 * {@code _id}: one document per merchant, upserted in place - the correct behaviour for a
 * projection of a compacted topic, unlike the append-only {@code delivery_attempts} log.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "merchant_webhooks")
public class MerchantWebhookDocument {

    @Id private String merchantId;
    private String webhookUrl;
    private Instant updatedAt;
}
