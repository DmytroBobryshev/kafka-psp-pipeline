package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.MerchantPage;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.model.MerchantView;
import java.util.Optional;

/**
 * Outbound port for the local merchant-config projection. Written only by
 * {@code adapters.in.kafka.MerchantConfigChangedListener}; read by
 * {@code adapters.in.web.MerchantQueryController} and {@code CreatePaymentUseCase}'s onboarding
 * gate.
 */
public interface MerchantViewRepository {

    /**
     * Whole-row replace keyed by {@link MerchantView#merchantId()} - mirrors the source topic's
     * own compacted-snapshot semantics, so redelivery of the same record converges on the same
     * row rather than erroring or duplicating.
     */
    void upsert(MerchantView view);

    /**
     * No-op if {@code merchantId} is not present here: a tombstone for a key this projection
     * never saw a value for (e.g. it deleted before this consumer group's first read of the
     * compacted log) is not an error condition.
     */
    void delete(String merchantId);

    Optional<MerchantView> findById(String merchantId);

    /**
     * @param status filter, or {@code null} to match every status.
     * @param page   zero-based page index.
     * @param size   page size.
     */
    MerchantPage search(MerchantStatus status, int page, int size);
}
