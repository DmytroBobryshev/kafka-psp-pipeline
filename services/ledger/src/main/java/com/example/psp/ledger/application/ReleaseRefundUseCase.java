package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.RefundSagaState;
import com.example.psp.ledger.domain.model.ReleaseOutcome;
import com.example.psp.ledger.domain.port.RefundEventPublisher;
import com.example.psp.ledger.domain.port.RefundRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * M11 step 4 (COMPENSATION): consumes {@code refunds.refund-failed.v1} and, if this refund is
 * still RESERVED, releases the reservation and restores the balance - the guarded
 * {@code RESERVED -> RELEASED} transition ({@link RefundRepository#tryRelease}). This is the
 * compensating transaction and the heart of this module (ADR-0008).
 *
 * <p>This same topic has two producers (this service, on insufficient balance; psp-connector, on
 * a provider decline), and this listener consumes both without distinguishing which one wrote any
 * given record - the guarded transition decides purely from the CURRENT saga state:
 *
 * <ul>
 *   <li>RESERVED - a real compensation: release, restore, publish
 *       {@code refunds.reservation-released.v1 reason=COMPENSATION}.
 *   <li>FAILED - this service consuming its OWN insufficient-balance publish back. Nothing was
 *       ever reserved, so there is nothing to release; {@link RefundRepository#tryRelease} reports
 *       {@code NOT_APPLICABLE} and this listener no-ops. See services/ledger/README.md's M11
 *       section, "ADR-0008 rule 7", for why this single, terminating hop is not treated as the
 *       unbounded cycle that rule forbids.
 *   <li>RELEASED already - a genuine duplicate (replay, or a race against the TTL sweeper trying
 *       the same release) - idempotent no-op.
 *   <li>COMPLETED, NEEDS_MANUAL_REVIEW, or an unexpected state - rejected and logged loudly
 *       (ADR-0008 rule 3), never silently applied.
 * </ul>
 */
@Service
public class ReleaseRefundUseCase {

    private static final Logger log = LoggerFactory.getLogger(ReleaseRefundUseCase.class);

    private final RefundRepository refundRepository;
    private final RefundEventPublisher refundEventPublisher;
    private final Counter releasedCounter;
    private final Counter deduplicatedCounter;

    public ReleaseRefundUseCase(
            RefundRepository refundRepository,
            RefundEventPublisher refundEventPublisher,
            MeterRegistry meterRegistry) {
        this.refundRepository = refundRepository;
        this.refundEventPublisher = refundEventPublisher;
        this.releasedCounter =
                Counter.builder("ledger.refunds.released")
                        .tag("trigger", "compensation")
                        .description("Reservations released in response to refunds.refund-failed.v1")
                        .register(meterRegistry);
        this.deduplicatedCounter =
                Counter.builder("ledger.refunds.deduplicated")
                        .tag("step", "release")
                        .description(
                                "refunds.refund-failed.v1 deliveries that released nothing - already "
                                        + "released, never reserved (this service's own insufficient-balance "
                                        + "publish looping back), or an illegal transition")
                        .register(meterRegistry);
    }

    public void execute(ReleaseRefundCommand command) {
        ReleaseOutcome outcome =
                refundRepository.tryRelease(command.refundId(), command.reason(), command.inboundEventId());

        switch (outcome.result()) {
            case APPLIED -> {
                releasedCounter.increment();
                log.info(
                        "Released refundId={} reason={} inboundEventId={} -> balance restored to {} {}",
                        command.refundId(),
                        command.reason(),
                        command.inboundEventId(),
                        outcome.balanceAfter().balance().amount(),
                        outcome.balanceAfter().balance().currency());
                publishReleased(command);
            }
            case NOT_APPLICABLE -> {
                deduplicatedCounter.increment();
                log.info(
                        "No reservation to release for refundId={} (refunds.refund-failed.v1 "
                                + "inboundEventId={}, reason={}) - never reserved, nothing to compensate",
                        command.refundId(),
                        command.inboundEventId(),
                        command.reason());
            }
            case ALREADY_APPLIED -> {
                deduplicatedCounter.increment();
                log.info(
                        "Deduplicated refund-failed inboundEventId={} refundId={} - already released",
                        command.inboundEventId(),
                        command.refundId());
            }
            case ESCALATED_MANUAL_REVIEW, REJECTED_ILLEGAL -> {
                deduplicatedCounter.increment();
                log.error(
                        "Rejected illegal compensation attempt: refunds.refund-failed.v1 inboundEventId={} "
                                + "refundId={} result={} - a release was attempted against a refund that is "
                                + "not RESERVED and not FAILED; balance was NOT touched. See "
                                + "RefundRepository#tryRelease.",
                        command.inboundEventId(),
                        command.refundId(),
                        outcome.result());
            }
        }
    }

    /**
     * Publishing needs the amount/paymentId/merchantId this release applied to - re-read from the
     * saga state rather than threaded through every layer, since {@link ReleaseRefundCommand} only
     * carries the failure {@code reason} (the amount belongs to the reservation, not to the
     * refund-failed event that triggered releasing it).
     */
    private void publishReleased(ReleaseRefundCommand command) {
        Optional<RefundSagaState> state = refundRepository.findSagaState(command.refundId());
        if (state.isEmpty()) {
            // Cannot happen if tryRelease reported APPLIED (it just wrote this row) - defensive only.
            log.error(
                    "refund_saga_state row vanished immediately after a successful release for "
                            + "refundId={}",
                    command.refundId());
            return;
        }
        refundEventPublisher.publishReservationReleased(
                state.get(),
                "COMPENSATION",
                command.inboundEventId(),
                command.traceId(),
                command.correlationId());
    }
}
