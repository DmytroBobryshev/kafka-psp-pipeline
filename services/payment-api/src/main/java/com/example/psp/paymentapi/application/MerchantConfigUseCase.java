package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantConfig;
import com.example.psp.paymentapi.domain.port.MerchantConfigPublisher;
import org.springframework.stereotype.Service;

/**
 * The merchant-configuration use cases (M10): upsert and delete.
 *
 * <p>Notice what is <b>not</b> here, in deliberate contrast to {@link CreatePaymentUseCase}: no
 * {@code @Transactional}, and no repository port. There is no local write to be atomic with -
 * {@code merchants.merchant-config-changed.v1} is the system of record (see
 * {@link MerchantConfig}), so the single act of publishing is the whole use case. Annotating this
 * class {@code @Transactional} would open a Postgres transaction that does nothing and, worse,
 * would suggest to a reader that the Kafka send participates in it. It does not, and cannot.
 *
 * <p>Both operations are idempotent by construction, which is what makes the REST verbs honest:
 * {@code PUT} replays converge on the same compacted last-value, and a second {@code DELETE}
 * writes a second tombstone whose effect is identical to the first.
 */
@Service
public class MerchantConfigUseCase {

    private final MerchantConfigPublisher merchantConfigPublisher;

    public MerchantConfigUseCase(MerchantConfigPublisher merchantConfigPublisher) {
        this.merchantConfigPublisher = merchantConfigPublisher;
    }

    /**
     * Publishes the merchant's complete configuration. The domain constructor is the validation
     * boundary that a bug bypassing the web DTO cannot get around (same
     * redundant-by-design layering as {@code CreatePaymentRequest} / {@code Money}).
     */
    public MerchantConfig upsert(UpsertMerchantConfigCommand command) {
        MerchantConfig config =
                new MerchantConfig(
                        command.merchantId(),
                        command.displayName(),
                        command.status(),
                        command.payoutCurrency(),
                        command.allowedCurrencies(),
                        command.webhookUrl(),
                        command.declineRateAlertThresholdBps());

        merchantConfigPublisher.publishConfigChanged(config);
        return config;
    }

    /**
     * Publishes a tombstone (null value) under the merchant's key - the only record shape that
     * makes log compaction actually remove a key. See
     * {@link MerchantConfigPublisher#publishConfigDeleted}.
     *
     * <p>There is nothing to look up first and nothing to 404 on: this service holds no merchant
     * state, and "delete a key that has no value" is a no-op that compaction resolves on its own.
     * Reading the current value before deleting would mean querying the topic (or a downstream
     * service's projection) over REST, which ADR-0004 forbids.
     */
    public void delete(String merchantId) {
        merchantConfigPublisher.publishConfigDeleted(merchantId);
    }
}
