package com.example.psp.paymentapi.adapters.in.scheduler;

import com.example.psp.paymentapi.application.ExpireRefundsUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RefundExpirationScheduler {

    private static final Logger log = LoggerFactory.getLogger(RefundExpirationScheduler.class);

    private final ExpireRefundsUseCase useCase;

    public RefundExpirationScheduler(ExpireRefundsUseCase useCase) {
        this.useCase = useCase;
    }

    @Scheduled(fixedDelayString = "${payment-api.expiration.sweep-interval:PT5S}")
    public void sweep() {
        try {
            useCase.execute();
        } catch (RuntimeException e) {
            log.error("Refund expiration sweep failed - will retry on the next tick", e);
        }
    }
}
