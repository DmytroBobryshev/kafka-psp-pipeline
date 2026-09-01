package com.example.psp.ledger.adapters.in.scheduler;

import com.example.psp.ledger.application.SweepExpiredReservationsUseCase;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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
            log.error("Refund reservation TTL sweep failed - will retry on the next tick", e);
        }
    }
}
