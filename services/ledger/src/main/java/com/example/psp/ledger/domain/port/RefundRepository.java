package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.ReleaseOutcome;
import com.example.psp.ledger.domain.model.ReserveOutcome;
import com.example.psp.ledger.domain.model.RefundReservation;
import com.example.psp.ledger.domain.model.RefundSagaState;
import com.example.psp.ledger.domain.model.SettleOutcome;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Outbound port for the M11 refund saga's local state, owned exclusively by the {@code ledger}
 * database (ADR-0005) - three tables ({@code refund_reservations}, {@code refund_saga_state},
 * {@code refund_processed_events}), one port. Implemented by
 * {@code adapters.out.persistence.PostgresRefundRepository}.
 *
 * <p>Deliberately a separate port from {@link LedgerRepository} even though both write to the same
 * database: {@code LedgerRepository} is M7's contract and every line of it - the exactly-once
 * machinery this module is required not to disturb - stays untouched. This port is additive.
 *
 * <p>Every method here follows the same M5/M7 idempotency shape used throughout this codebase:
 * check-first ({@link #hasProcessedInboundEvent}) for the common case, plus a constraint-race path
 * inside each atomic write (a {@code refund_processed_events} row is inserted first, inside the
 * same transaction, so a concurrent redelivery loses on that unique key rather than corrupting the
 * balance - see {@code adapters.out.persistence.RefundWriteTransaction}).
 */
public interface RefundRepository {

    /** Check-first idempotency, shared shape across every refund saga listener in this service. */
    boolean hasProcessedInboundEvent(UUID inboundEventId);

    /**
     * Consuming {@code refunds.refund-requested.v1}: atomically, in one Postgres transaction,
     * either reserves {@code reservation}'s amount against the merchant's balance (inserting the
     * reservation row, debiting the balance, and writing {@code refund_saga_state} RESERVED) or
     * records the refusal (writing {@code refund_saga_state} FAILED with
     * {@code insufficientBalanceReason}, touching neither the reservation table nor the balance).
     * Either branch also inserts the {@code refund_processed_events} dedup row for
     * {@code inboundEventId}.
     */
    ReserveOutcome tryReserveOrFail(
            RefundReservation reservation, UUID inboundEventId, String insufficientBalanceReason);

    /** The ledger's local saga-state read model - backs {@code GET /api/refunds/{refundId}}. */
    Optional<RefundSagaState> findSagaState(UUID refundId);

    /**
     * Consuming {@code refunds.refund-completed.v1}: the guarded {@code RESERVED -> COMPLETED}
     * transition. {@code debitEntry} is inserted into {@code ledger_entries} without a further
     * balance delta (see {@link LedgerEntry#debit}). See
     * {@code adapters.out.persistence.RefundWriteTransaction#settle} for what happens on every
     * other starting state, including the documented late-completion-after-release escalation.
     */
    SettleOutcome trySettle(UUID refundId, LedgerEntry debitEntry, UUID inboundEventId);

    /**
     * Consuming {@code refunds.refund-failed.v1}: the guarded {@code RESERVED -> RELEASED}
     * transition - the compensating transaction. Restores the balance by the reserved amount only
     * when the transition actually applies; see
     * {@code adapters.out.persistence.RefundWriteTransaction#release} for every other starting
     * state, including the {@code FAILED} self-loop this service's own insufficient-balance
     * publish produces (services/ledger/README.md's M11 section, "ADR-0008 rule 7").
     */
    ReleaseOutcome tryRelease(UUID refundId, String reason, UUID inboundEventId);

    /**
     * TTL sweeper candidates: refunds still RESERVED whose {@code refund_saga_state.updated_at}
     * (set at reservation time, unchanged while RESERVED) is older than {@code cutoff}.
     */
    List<RefundSagaState> findReservedOlderThan(Instant cutoff);

    /**
     * The TTL sweep itself: the same guarded {@code RESERVED -> RELEASED} transition as
     * {@link #tryRelease}, with {@code reason = "TIMEOUT"} and no inbound event to dedup against -
     * the sweeper is not triggered by any Kafka record, so the transition's own compare-and-swap
     * ({@code WHERE status = 'RESERVED'}) is the sole race guard, race-safe against a concurrent
     * {@link #tryRelease} compensation for the same refund.
     */
    ReleaseOutcome tryReleaseForTimeout(UUID refundId);
}
