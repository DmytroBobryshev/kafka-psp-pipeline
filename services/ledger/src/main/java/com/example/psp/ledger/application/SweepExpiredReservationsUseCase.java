package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.RefundSagaState;
import com.example.psp.ledger.domain.model.RefundTransitionResult;
import com.example.psp.ledger.domain.model.ReleaseOutcome;
import com.example.psp.ledger.domain.port.RefundEventPublisher;
import com.example.psp.ledger.domain.port.RefundRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * ADR-0008 rule 6 / M11's TTL sweep: "a reservation that is neither committed nor released within
 * {@code refund.reservation.ttl} is swept ... and publishes {@code
 * refunds.reservation-released.v1} with reason {@code TIMEOUT}. Without this, a lost {@code
 * refund-completed} leaks the reservation forever." Triggered by
 * {@code adapters.in.scheduler.ReservationTtlSweeper}'s {@code @Scheduled} method, not by any
 * Kafka record - the one inbound trigger in this whole module that is not a topic.
 *
 * <p>Idempotent by construction, and safe under concurrency with the compensation path: each
 * candidate's release attempt goes through the SAME guarded {@code RESERVED -> RELEASED}
 * compare-and-swap {@link RefundRepository#tryReleaseForTimeout} uses for
 * {@link ReleaseRefundUseCase#execute}, so if a {@code refunds.refund-failed.v1} compensation
 * lands first, this sweep simply loses the race and reports {@code ALREADY_APPLIED} - a normal,
 * silent no-op, never a double-restore.
 */
@Service
public class SweepExpiredReservationsUseCase {

    private static final Logger log = LoggerFactory.getLogger(SweepExpiredReservationsUseCase.class);
    private static final String SOURCE = "ledger";

    private final RefundRepository refundRepository;
    private final RefundEventPublisher refundEventPublisher;
    private final Counter sweptCounter;

    public SweepExpiredReservationsUseCase(
            RefundRepository refundRepository,
            RefundEventPublisher refundEventPublisher,
            MeterRegistry meterRegistry) {
        this.refundRepository = refundRepository;
        this.refundEventPublisher = refundEventPublisher;
        this.sweptCounter =
                Counter.builder("ledger.refunds.released")
                        .tag("trigger", "timeout")
                        .description(
                                "Reservations released by the TTL sweeper - refunds.refund-completed.v1 "
                                        + "was lost, or is simply still in flight past refund.reservation.ttl")
                        .register(meterRegistry);
    }

    /** @return how many reservations this sweep pass actually released. */
    public int execute(Duration reservationTtl) {
        Instant cutoff = Instant.now().minus(reservationTtl);
        List<RefundSagaState> candidates = refundRepository.findReservedOlderThan(cutoff);
        if (candidates.isEmpty()) {
            return 0;
        }

        int released = 0;
        for (RefundSagaState candidate : candidates) {
            ReleaseOutcome outcome = refundRepository.tryReleaseForTimeout(candidate.refundId());
            if (outcome.result() == RefundTransitionResult.APPLIED) {
                released++;
                sweptCounter.increment();
                log.info(
                        "TTL-released refundId={} paymentId={} merchantId={} amount={} reservedSince={} "
                                + "-> balance restored to {} {}",
                        candidate.refundId(),
                        candidate.paymentId(),
                        candidate.merchantId(),
                        candidate.amount().amount(),
                        candidate.updatedAt(),
                        outcome.balanceAfter().balance().amount(),
                        outcome.balanceAfter().balance().currency());
                refundEventPublisher.publishReservationReleased(
                        candidate,
                        "TIMEOUT",
                        null, // no inbound Kafka event caused this - a root cause in its own right
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString());
            } else {
                // Lost the CAS - a concurrent compensation (refunds.refund-failed.v1) or another
                // sweeper tick released it first. Silent, correct no-op; see class javadoc.
                log.debug(
                        "TTL sweep skipped refundId={} - lost the release race (result={})",
                        candidate.refundId(),
                        outcome.result());
            }
        }
        log.info(
                "TTL sweep complete: {} candidate(s) older than {}, {} released",
                candidates.size(),
                cutoff,
                released);
        return released;
    }

    /** Exposed so {@code adapters.in.web} can build a decently-named source tag if ever needed. */
    public static String source() {
        return SOURCE;
    }
}
