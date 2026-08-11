package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import java.util.List;

/**
 * Outbound port for the M13 batch listener's write path: one bulk write per batch instead of one
 * round trip per record - see {@code adapters.in.kafka.PaymentStatusChangedBatchListener} for
 * where {@code max.poll.records} decides the batch size, and
 * {@code adapters.out.mongo.MongoPaymentStatusAuditRepository} for the bulk-write mechanics.
 */
public interface PaymentStatusAuditRepository {

    /**
     * Upserts the whole batch in one round trip, keyed by {@link PaymentStatusAuditEntry#eventId}
     * per entry (idempotent - a redelivered entry overwrites its own document rather than
     * duplicating). Throws {@link PartialBatchWriteException} if the underlying bulk write
     * applied only a prefix - see that exception's javadoc for the exact guarantee callers may
     * rely on.
     */
    void saveAll(List<PaymentStatusAuditEntry> entries);
}
