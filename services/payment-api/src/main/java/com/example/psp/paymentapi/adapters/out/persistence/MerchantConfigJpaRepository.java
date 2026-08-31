package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import java.time.Instant;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Spring Data JPA repository behind {@link PostgresMerchantViewRepository}. */
public interface MerchantConfigJpaRepository extends JpaRepository<MerchantConfigEntity, String> {

    /**
     * Native SQL - JPQL has no {@code ON CONFLICT}. Whole-row replace, matching the source
     * topic's own compacted-snapshot semantics: redelivery converges on the same row rather than
     * erroring or duplicating.
     */
    @Modifying
    @Query(
            value =
                    "INSERT INTO merchant_configs "
                            + "(merchant_id, display_name, status, payout_currency, webhook_url, "
                            + "decline_rate_alert_threshold_bps, updated_at) "
                            + "VALUES (:merchantId, :displayName, :status, :payoutCurrency, :webhookUrl, "
                            + ":declineRateAlertThresholdBps, :updatedAt) "
                            + "ON CONFLICT (merchant_id) DO UPDATE SET "
                            + "display_name = EXCLUDED.display_name, "
                            + "status = EXCLUDED.status, "
                            + "payout_currency = EXCLUDED.payout_currency, "
                            + "webhook_url = EXCLUDED.webhook_url, "
                            + "decline_rate_alert_threshold_bps = EXCLUDED.decline_rate_alert_threshold_bps, "
                            + "updated_at = EXCLUDED.updated_at",
            nativeQuery = true)
    void upsert(
            @Param("merchantId") String merchantId,
            @Param("displayName") String displayName,
            @Param("status") String status,
            @Param("payoutCurrency") String payoutCurrency,
            @Param("webhookUrl") String webhookUrl,
            @Param("declineRateAlertThresholdBps") int declineRateAlertThresholdBps,
            @Param("updatedAt") Instant updatedAt);

    /**
     * Bulk JPQL delete, not derived {@code deleteById}: a tombstone for a merchant this
     * projection never saw a value for must be a no-op, not an
     * {@code EmptyResultDataAccessException}.
     */
    @Modifying
    @Query("DELETE FROM MerchantConfigEntity m WHERE m.merchantId = :merchantId")
    void deleteByMerchantId(@Param("merchantId") String merchantId);

    @Query(
            "SELECT m FROM MerchantConfigEntity m WHERE (:status IS NULL OR m.status = :status) "
                    + "ORDER BY m.merchantId ASC")
    Page<MerchantConfigEntity> search(@Param("status") MerchantStatus status, Pageable pageable);
}
