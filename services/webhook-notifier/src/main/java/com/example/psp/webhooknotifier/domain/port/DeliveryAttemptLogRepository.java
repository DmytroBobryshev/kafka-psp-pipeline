package com.example.psp.webhooknotifier.domain.port;

import com.example.psp.webhooknotifier.domain.model.DeliveryAttempt;

/**
 * Outbound port for the MongoDB delivery-attempt log (M8 requirement #6). Implemented by
 * {@code adapters.out.persistence.MongoDeliveryAttemptLogRepository}. One document per call to
 * {@link #record}, no update-in-place - the collection is an append-only attempt history, not a
 * "latest status" table (that concern belongs to a future read-model, not this module).
 */
public interface DeliveryAttemptLogRepository {

    /** Persists one attempt. Called for every outcome - success included - never skipped. */
    void record(DeliveryAttempt attempt);
}
