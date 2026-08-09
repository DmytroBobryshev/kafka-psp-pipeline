package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.PaymentAttempt;

/**
 * Outbound port for the M4 dedup table - {@code (paymentId, providerEventId)} unique constraint,
 * one row per authorization attempt (see {@code db/migration/V1__create_payment_attempts_table.sql}
 * and {@link PaymentAttempt}'s javadoc for why M4 only ever writes here). Implemented by
 * {@code adapters.out.persistence.PostgresAttemptLogRepository} (ADR-0005: psp-connector owns its
 * own Postgres database, no other service touches it).
 */
public interface AttemptLogRepository {

    void record(PaymentAttempt attempt);
}
