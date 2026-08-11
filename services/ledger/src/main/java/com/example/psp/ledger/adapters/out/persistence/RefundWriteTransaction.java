package com.example.psp.ledger.adapters.out.persistence;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import com.example.psp.ledger.domain.model.RefundReservation;
import com.example.psp.ledger.domain.model.RefundSagaStatus;
import com.example.psp.ledger.domain.model.RefundTransitionResult;
import com.example.psp.ledger.domain.model.ReleaseOutcome;
import com.example.psp.ledger.domain.model.ReserveOutcome;
import com.example.psp.ledger.domain.model.SettleOutcome;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The <b>Postgres</b> transactions behind every M11 refund-saga write. Three methods, one per
 * guarded state transition; each is a single {@code @Transactional} boundary so a
 * {@code DataIntegrityViolationException} from the {@code refund_processed_events} insert - the
 * race-prone statement in every method - propagates cleanly out to
 * {@link PostgresRefundRepository}, exactly the split {@code LedgerWriteTransaction} (M7)
 * established and for the identical reason: catching it INSIDE this class's own
 * {@code @Transactional} method would leave the transaction marked rollback-only and blow up at
 * commit with {@code UnexpectedRollbackException}.
 *
 * <p>Every method inserts that dedup row FIRST, via {@code saveAndFlush}, so a duplicate delivery
 * is rejected before this method attempts anything else - same discipline as
 * {@code LedgerWriteTransaction#applyAtomically}.
 *
 * <h2>Why the CAS transition, not a naive load-modify-save</h2>
 *
 * <p>{@link RefundSagaStateJpaRepository#transitionIfStatus} is a compare-and-swap
 * {@code UPDATE ... WHERE refund_id = ? AND status = ?}. A read-then-write in Java would be racy
 * exactly the way {@code MerchantBalanceEntity}'s javadoc warns against: two attempts at
 * conflicting transitions for the same refund (the clearest case - the TTL sweeper and a
 * compensating {@code refunds.refund-failed.v1} both trying to release the same reservation) could
 * both read RESERVED and both believe they alone are entitled to act. The CAS makes exactly one of
 * them win, and the loser's {@code rows == 0} branch below is what distinguishes "lost a race for
 * the SAME transition" ({@link RefundTransitionResult#ALREADY_APPLIED}, silent) from "the money
 * moved and this ledger's state says something incompatible"
 * ({@link RefundTransitionResult#ESCALATED_MANUAL_REVIEW}, loud) from "there was never anything to
 * do" ({@link RefundTransitionResult#NOT_APPLICABLE}, silent) - see ADR-0008 rule 3.
 *
 * <h2>Why settlement never calls {@code MerchantBalanceJpaRepository#applyDelta}</h2>
 *
 * <p>{@link #reserveOrFail} already subtracted the amount from the balance at reservation time.
 * {@link #settle} inserts the permanent {@code ledger_entries} DEBIT row (via the SAME
 * {@code LedgerEntryJpaRepository} M7 uses - deliberately NOT through
 * {@code LedgerRepository#tryApply}, which always applies a second balance delta) and touches
 * nothing else. Only {@link #release} moves the balance again, and only in the {@code +amount}
 * direction - restoring what {@link #reserveOrFail} took.
 */
@Component
public class RefundWriteTransaction {

    private static final String REFUND_REQUESTED_EVENT_TYPE = "refunds.refund-requested.v1";
    private static final String REFUND_COMPLETED_EVENT_TYPE = "refunds.refund-completed.v1";
    private static final String REFUND_FAILED_EVENT_TYPE = "refunds.refund-failed.v1";

    private final RefundReservationJpaRepository reservationRepository;
    private final RefundSagaStateJpaRepository sagaStateRepository;
    private final RefundProcessedEventJpaRepository processedEventRepository;
    private final MerchantBalanceJpaRepository balanceRepository;
    private final LedgerEntryJpaRepository ledgerEntryRepository;
    private final LedgerPersistenceMapper ledgerPersistenceMapper;
    private final RefundPersistenceMapper refundPersistenceMapper;

    public RefundWriteTransaction(
            RefundReservationJpaRepository reservationRepository,
            RefundSagaStateJpaRepository sagaStateRepository,
            RefundProcessedEventJpaRepository processedEventRepository,
            MerchantBalanceJpaRepository balanceRepository,
            LedgerEntryJpaRepository ledgerEntryRepository,
            LedgerPersistenceMapper ledgerPersistenceMapper,
            RefundPersistenceMapper refundPersistenceMapper) {
        this.reservationRepository = reservationRepository;
        this.sagaStateRepository = sagaStateRepository;
        this.processedEventRepository = processedEventRepository;
        this.balanceRepository = balanceRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.ledgerPersistenceMapper = ledgerPersistenceMapper;
        this.refundPersistenceMapper = refundPersistenceMapper;
    }

    /**
     * Consuming {@code refunds.refund-requested.v1}: reserve, or record the refusal. See
     * {@code domain.port.RefundRepository#tryReserveOrFail}.
     *
     * @throws org.springframework.dao.DataIntegrityViolationException if
     *     {@code refund_processed_events}'s primary key rejected the insert (a concurrent delivery
     *     of the same inbound event got there first). Propagated on purpose - see class javadoc.
     */
    @Transactional("transactionManager")
    public ReserveOutcome reserveOrFail(
            RefundReservation reservation, UUID inboundEventId, String insufficientBalanceReason) {
        processedEventRepository.saveAndFlush(
                processedEvent(inboundEventId, reservation.refundId(), REFUND_REQUESTED_EVENT_TYPE));

        Instant now = Instant.now();
        BigDecimal availableBalance =
                balanceRepository
                        .findById(reservation.merchantId())
                        .map(MerchantBalanceEntity::getBalance)
                        .orElse(BigDecimal.ZERO);

        if (availableBalance.compareTo(reservation.amount().amount()) >= 0) {
            reservationRepository.saveAndFlush(refundPersistenceMapper.toEntity(reservation));
            balanceRepository.applyDelta(
                    reservation.merchantId(),
                    reservation.amount().currency(),
                    reservation.amount().amount().negate(),
                    now);
            sagaStateRepository.saveAndFlush(
                    sagaStateEntity(reservation, RefundSagaStatus.RESERVED, null, now));
            return ReserveOutcome.reserved(readBalance(reservation.merchantId()));
        }

        sagaStateRepository.saveAndFlush(
                sagaStateEntity(reservation, RefundSagaStatus.FAILED, insufficientBalanceReason, now));
        return ReserveOutcome.insufficientBalance();
    }

    /**
     * Consuming {@code refunds.refund-completed.v1}: the guarded {@code RESERVED -> COMPLETED}
     * transition, or the documented late-completion escalation. See
     * {@code domain.port.RefundRepository#trySettle}.
     *
     * @throws org.springframework.dao.DataIntegrityViolationException see {@link #reserveOrFail}.
     */
    @Transactional("transactionManager")
    public SettleOutcome settle(UUID refundId, LedgerEntry debitEntry, UUID inboundEventId) {
        processedEventRepository.saveAndFlush(
                processedEvent(inboundEventId, refundId, REFUND_COMPLETED_EVENT_TYPE));

        Instant now = Instant.now();
        int rows =
                sagaStateRepository.transitionIfStatus(
                        refundId, RefundSagaStatus.RESERVED.name(), RefundSagaStatus.COMPLETED.name(), null, now);
        if (rows == 1) {
            ledgerEntryRepository.saveAndFlush(ledgerPersistenceMapper.toEntity(debitEntry));
            return SettleOutcome.applied(readBalance(debitEntry.getMerchantId()));
        }

        RefundSagaStateEntity current = sagaStateRepository.findById(refundId).orElse(null);
        if (current == null) {
            return SettleOutcome.of(RefundTransitionResult.REJECTED_ILLEGAL);
        }

        RefundSagaStatus currentStatus = RefundSagaStatus.valueOf(current.getStatus());
        return switch (currentStatus) {
            case COMPLETED, NEEDS_MANUAL_REVIEW -> SettleOutcome.of(RefundTransitionResult.ALREADY_APPLIED);
            // The late-completion edge case (sequence-refund-saga.md): the acquirer executed the
            // refund AFTER this ledger already released (or, pathologically, failed-at-request)
            // the reservation. Neither silently apply the debit (would double-count against the
            // restored balance) nor silently drop the event - escalate, loudly, for a human.
            case RELEASED, FAILED -> {
                int escalated =
                        sagaStateRepository.transitionIfStatus(
                                refundId,
                                current.getStatus(),
                                RefundSagaStatus.NEEDS_MANUAL_REVIEW.name(),
                                "LATE_COMPLETION_AFTER_" + current.getStatus(),
                                now);
                yield escalated == 1
                        ? SettleOutcome.of(RefundTransitionResult.ESCALATED_MANUAL_REVIEW)
                        : SettleOutcome.of(RefundTransitionResult.ALREADY_APPLIED);
            }
            default -> SettleOutcome.of(RefundTransitionResult.REJECTED_ILLEGAL);
        };
    }

    /**
     * Consuming {@code refunds.refund-failed.v1} (compensation, {@code inboundEventId != null}) or
     * the TTL sweep ({@code inboundEventId == null}) - the guarded {@code RESERVED -> RELEASED}
     * transition. See {@code domain.port.RefundRepository#tryRelease} and {@code #tryReleaseForTimeout}.
     *
     * @throws org.springframework.dao.DataIntegrityViolationException see {@link #reserveOrFail}
     *     (only possible when {@code inboundEventId != null} - the TTL path skips that insert
     *     entirely and relies solely on the compare-and-swap below as its race guard).
     */
    @Transactional("transactionManager")
    public ReleaseOutcome release(UUID refundId, String reason, UUID inboundEventId) {
        if (inboundEventId != null) {
            processedEventRepository.saveAndFlush(
                    processedEvent(inboundEventId, refundId, REFUND_FAILED_EVENT_TYPE));
        }

        Instant now = Instant.now();
        int rows =
                sagaStateRepository.transitionIfStatus(
                        refundId, RefundSagaStatus.RESERVED.name(), RefundSagaStatus.RELEASED.name(), reason, now);
        if (rows == 1) {
            RefundSagaStateEntity released =
                    sagaStateRepository
                            .findById(refundId)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "refund_saga_state row vanished immediately after release "
                                                            + "for refundId=" + refundId));
            // Restore: the positive-magnitude amount this refund reserved, added back.
            balanceRepository.applyDelta(
                    released.getMerchantId(), released.getCurrency(), released.getAmount(), now);
            return ReleaseOutcome.applied(readBalance(released.getMerchantId()));
        }

        RefundSagaStateEntity current = sagaStateRepository.findById(refundId).orElse(null);
        if (current == null) {
            return ReleaseOutcome.of(RefundTransitionResult.REJECTED_ILLEGAL);
        }

        RefundSagaStatus currentStatus = RefundSagaStatus.valueOf(current.getStatus());
        return switch (currentStatus) {
            case RELEASED -> ReleaseOutcome.of(RefundTransitionResult.ALREADY_APPLIED);
            // This service consuming its OWN insufficient-balance refund-failed.v1 back (ADR-0008
            // rule 7's bounded exception - see services/ledger/README.md's M11 section). Nothing
            // was ever reserved, so there is nothing to release.
            case FAILED -> ReleaseOutcome.of(RefundTransitionResult.NOT_APPLICABLE);
            // COMPLETED: releasing here would restore a balance for money already permanently
            // settled - actively wrong, not just late. NEEDS_MANUAL_REVIEW: already flagged.
            default -> ReleaseOutcome.of(RefundTransitionResult.REJECTED_ILLEGAL);
        };
    }

    private MerchantBalance readBalance(String merchantId) {
        return balanceRepository
                .findById(merchantId)
                .map(ledgerPersistenceMapper::toDomain)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "merchant_balances row vanished immediately after upsert for "
                                                + "merchantId=" + merchantId));
    }

    private RefundSagaStateEntity sagaStateEntity(
            RefundReservation reservation, RefundSagaStatus status, String reason, Instant now) {
        RefundSagaStateEntity entity = new RefundSagaStateEntity();
        entity.setRefundId(reservation.refundId());
        entity.setPaymentId(reservation.paymentId());
        entity.setMerchantId(reservation.merchantId());
        entity.setAmount(reservation.amount().amount());
        entity.setCurrency(reservation.amount().currency());
        entity.setStatus(status.name());
        entity.setReason(reason);
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        return entity;
    }

    private RefundProcessedEventEntity processedEvent(UUID inboundEventId, UUID refundId, String eventType) {
        RefundProcessedEventEntity entity = new RefundProcessedEventEntity();
        entity.setInboundEventId(inboundEventId);
        entity.setRefundId(refundId);
        entity.setEventType(eventType);
        entity.setProcessedAt(Instant.now());
        return entity;
    }
}
