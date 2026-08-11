package com.example.psp.ledger.adapters.out.persistence;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.RefundReservation;
import com.example.psp.ledger.domain.model.RefundSagaState;
import com.example.psp.ledger.domain.model.RefundSagaStatus;
import com.example.psp.ledger.domain.model.RefundTransitionResult;
import com.example.psp.ledger.domain.model.ReleaseOutcome;
import com.example.psp.ledger.domain.model.ReserveOutcome;
import com.example.psp.ledger.domain.model.SettleOutcome;
import com.example.psp.ledger.domain.port.RefundRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

/**
 * Real Postgres adapter for {@link RefundRepository} (M11). Deliberately <b>not</b>
 * {@code @Transactional} itself - the transaction lives one level down in
 * {@link RefundWriteTransaction}, precisely so the {@code catch} blocks below sit outside it. See
 * that class's javadoc for why the split is load-bearing rather than stylistic (same reasoning as
 * {@code PostgresLedgerRepository}, M7).
 */
@Repository
public class PostgresRefundRepository implements RefundRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresRefundRepository.class);

    private final RefundProcessedEventJpaRepository processedEventRepository;
    private final RefundSagaStateJpaRepository sagaStateRepository;
    private final RefundWriteTransaction writeTransaction;
    private final RefundPersistenceMapper mapper;

    public PostgresRefundRepository(
            RefundProcessedEventJpaRepository processedEventRepository,
            RefundSagaStateJpaRepository sagaStateRepository,
            RefundWriteTransaction writeTransaction,
            RefundPersistenceMapper mapper) {
        this.processedEventRepository = processedEventRepository;
        this.sagaStateRepository = sagaStateRepository;
        this.writeTransaction = writeTransaction;
        this.mapper = mapper;
    }

    @Override
    public boolean hasProcessedInboundEvent(UUID inboundEventId) {
        return processedEventRepository.existsById(inboundEventId);
    }

    @Override
    public ReserveOutcome tryReserveOrFail(
            RefundReservation reservation, UUID inboundEventId, String insufficientBalanceReason) {
        try {
            return writeTransaction.reserveOrFail(reservation, inboundEventId, insufficientBalanceReason);
        } catch (DataIntegrityViolationException e) {
            // refund_processed_events rejected the insert: a concurrent delivery of this same
            // inbound event won the check-then-act race the use case's check-first path cannot
            // close alone. Normal outcome of at-least-once delivery under concurrency - reported by
            // return value, never rethrown (RefundRepository#tryReserveOrFail's contract).
            log.debug(
                    "Unique constraint rejected duplicate refund-requested inboundEventId={} refundId={}",
                    inboundEventId,
                    reservation.refundId(),
                    e);
            return ReserveOutcome.alreadyProcessed();
        }
    }

    @Override
    public Optional<RefundSagaState> findSagaState(UUID refundId) {
        return sagaStateRepository.findById(refundId).map(mapper::toDomain);
    }

    @Override
    public SettleOutcome trySettle(UUID refundId, LedgerEntry debitEntry, UUID inboundEventId) {
        try {
            return writeTransaction.settle(refundId, debitEntry, inboundEventId);
        } catch (DataIntegrityViolationException e) {
            log.debug(
                    "Unique constraint rejected duplicate refund-completed inboundEventId={} refundId={}",
                    inboundEventId,
                    refundId,
                    e);
            return SettleOutcome.of(RefundTransitionResult.ALREADY_APPLIED);
        }
    }

    @Override
    public ReleaseOutcome tryRelease(UUID refundId, String reason, UUID inboundEventId) {
        try {
            return writeTransaction.release(refundId, reason, inboundEventId);
        } catch (DataIntegrityViolationException e) {
            log.debug(
                    "Unique constraint rejected duplicate refund-failed inboundEventId={} refundId={}",
                    inboundEventId,
                    refundId,
                    e);
            return ReleaseOutcome.of(RefundTransitionResult.ALREADY_APPLIED);
        }
    }

    @Override
    public List<RefundSagaState> findReservedOlderThan(Instant cutoff) {
        return sagaStateRepository.findByStatusAndUpdatedAtBefore(RefundSagaStatus.RESERVED.name(), cutoff).stream()
                .map(mapper::toDomain)
                .toList();
    }

    @Override
    public ReleaseOutcome tryReleaseForTimeout(UUID refundId) {
        // No inbound Kafka event to race on - the guarded transition's own compare-and-swap is the
        // sole race guard here (see RefundWriteTransaction#release). A DataIntegrityViolationException
        // is not reachable on this path, but the catch is kept for defence-in-depth and symmetry.
        try {
            return writeTransaction.release(refundId, "TIMEOUT", null);
        } catch (DataIntegrityViolationException e) {
            log.debug("Unexpected constraint violation during TTL release for refundId={}", refundId, e);
            return ReleaseOutcome.of(RefundTransitionResult.ALREADY_APPLIED);
        }
    }
}
