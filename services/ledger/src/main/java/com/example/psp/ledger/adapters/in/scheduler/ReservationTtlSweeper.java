package com.example.psp.ledger.adapters.in.scheduler;

import com.example.psp.ledger.application.SweepExpiredReservationsUseCase;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * ADR-0008 rule 6 / M11's TTL sweep, triggered on a fixed schedule rather than by any Kafka
 * record - the one inbound "adapter" in this whole module that is not REST or Kafka. Delegates
 * everything to {@link SweepExpiredReservationsUseCase}; this class is only the trigger and the
 * property binding (ADR-0007: adapters wire things up, they do not contain the logic).
 *
 * <p>Property names deliberately use this service's {@code ledger.*} prefix rather than
 * ADR-0008's bare {@code refund.reservation.ttl} - a minor, documented adaptation to this
 * service's existing configuration convention (every other M11/M7 property lives under
 * {@code ledger.*} too), not a different concept.
 *
 * <ul>
 *   <li>{@code ledger.refund.reservation-ttl} - how long a reservation may stay RESERVED before
 *       the sweeper releases it. Default {@code PT2M} (2 minutes) - deliberately short, the same
 *       "wrong for a real cluster, right for a demo" trade-off M10's compacted-topic settings
 *       document (services/payment-api/README.md's M10 section) already established for this
 *       codebase, so the timeout path is observable in a live-stack proof without a 15-minute
 *       wait.
 *   <li>{@code ledger.refund.sweep-interval} - how often the sweep runs. Default {@code PT10S}.
 * </ul>
 */
@Component
public class ReservationTtlSweeper {

    private static final Logger log = LoggerFactory.getLogger(ReservationTtlSweeper.class);

    private final SweepExpiredReservationsUseCase useCase;
    private final Duration reservationTtl;

    public ReservationTtlSweeper(
            SweepExpiredReservationsUseCase useCase,
            @Value("${ledger.refund.reservation-ttl:PT2M}") Duration reservationTtl) {
        this.useCase = useCase;
        this.reservationTtl = reservationTtl;
    }

    @Scheduled(fixedDelayString = "${ledger.refund.sweep-interval:PT10S}")
    public void sweep() {
        try {
            useCase.execute(reservationTtl);
        } catch (RuntimeException e) {
            // A scheduled method that lets an exception escape stops future executions entirely
            // (Spring's default TaskScheduler behaviour) - catching and logging keeps the sweep
            // running on its next tick even if one pass hit a transient Postgres/Kafka problem.
            log.error("Refund reservation TTL sweep failed - will retry on the next tick", e);
        }
    }
}
