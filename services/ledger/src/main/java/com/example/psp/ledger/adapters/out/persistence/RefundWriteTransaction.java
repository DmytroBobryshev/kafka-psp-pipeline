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
            case FAILED -> ReleaseOutcome.of(RefundTransitionResult.NOT_APPLICABLE);
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
