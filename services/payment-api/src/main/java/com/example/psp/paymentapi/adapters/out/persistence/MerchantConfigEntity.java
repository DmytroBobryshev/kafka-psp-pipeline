package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA entity for the {@code merchant_configs} table (schema owned by
 * {@code db/migration/V7__create_merchant_configs_table.sql}). Written only through
 * {@link MerchantConfigJpaRepository#upsert}'s native {@code INSERT ... ON CONFLICT}; this
 * entity's setters back only the read path ({@code findById}/{@code search}).
 */
@Entity
@Table(name = "merchant_configs")
@Getter
@Setter
@NoArgsConstructor
public class MerchantConfigEntity {

    @Id
    @Column(name = "merchant_id", length = 255)
    private String merchantId;

    @Column(name = "display_name", nullable = false, length = 255)
    private String displayName;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private MerchantStatus status;

    @Column(name = "payout_currency", nullable = false, length = 3)
    private String payoutCurrency;

    // CSV of 1-3 ISO-4217 codes; "" means legacy row - see MerchantConfigPersistenceMapper.
    @Column(name = "allowed_currencies", nullable = false, length = 64)
    private String allowedCurrencies;

    @Column(name = "webhook_url")
    private String webhookUrl;

    @Column(name = "decline_rate_alert_threshold_bps", nullable = false)
    private int declineRateAlertThresholdBps;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
