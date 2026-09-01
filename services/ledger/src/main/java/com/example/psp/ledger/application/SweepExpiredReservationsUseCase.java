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

    public static String source() {
        return SOURCE;
    }
}
