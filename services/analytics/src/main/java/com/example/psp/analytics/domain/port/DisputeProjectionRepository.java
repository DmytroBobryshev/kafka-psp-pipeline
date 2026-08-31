package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.DisputeProjection;

/**
 * Outbound port for the M13 dispute projection - implemented by {@code adapters.out.mongo.
 * MongoDisputeProjectionRepository}. A single-record write (unlike {@code
 * PaymentStatusAuditRepository}'s bulk shape): {@code disputes.dispute-opened.v1} is a plain
 * single-record {@code @KafkaListener} (M13's dispute-opened listener), not the batch listener,
 * so there is no batch to collapse into one round trip.
 */
public interface DisputeProjectionRepository {

    /** Upserts by {@code disputeId} - a redelivered event overwrites, never duplicates. */
    void save(DisputeProjection projection);
}
