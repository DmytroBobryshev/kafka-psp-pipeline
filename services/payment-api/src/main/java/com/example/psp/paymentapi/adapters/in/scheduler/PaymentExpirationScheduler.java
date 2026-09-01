package com.example.psp.paymentapi.adapters.in.scheduler;

import com.example.psp.paymentapi.application.ExpirePaymentsUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * M22's expiration engine trigger - the same "one inbound 'adapter' that is not REST or Kafka"
 * shape as ledger's {@code adapters.in.scheduler.ReservationTtlSweeper} (ADR-0008 rule 6's TTL
 * sweep), mirrored here for the identical class of problem: a payment stuck {@code CREATED}/
 * {@code PENDING} past its window needs SOMETHING to notice, since nothing publishes a "time
 * passed" event on its own. Delegates everything to {@link ExpirePaymentsUseCase}; this class is
 * only the trigger and the property binding (ADR-0007: adapters wire things up, they do not
 * contain the logic).
 *
 * <p>{@code payment-api.expiration.sweep-interval}, default {@code PT5S} - short by production
 * standards, deliberate for the same "wrong for a real cluster, right for a demo" reason M10's
 * compacted-topic settings and ledger's own sweep-interval default already document: an operator
 * proving the feature works should not have to wait fifteen minutes for the first EXPIRED record.
 */
@Component
public class PaymentExpirationScheduler {

    private static final Logger log = LoggerFactory.getLogger(PaymentExpirationScheduler.class);

    private final ExpirePaymentsUseCase useCase;

    public PaymentExpirationScheduler(ExpirePaymentsUseCase useCase) {
        this.useCase = useCase;
    }

    @Scheduled(fixedDelayString = "${payment-api.expiration.sweep-interval:PT5S}")
    public void sweep() {
        try {
            useCase.execute();
        } catch (RuntimeException e) {
            // A scheduled method that lets an exception escape stops future executions entirely
            // (Spring's default TaskScheduler behaviour) - catching and logging keeps the sweep
            // running on its next tick even if one pass hit a transient Postgres/Kafka problem.
            log.error("Payment expiration sweep failed - will retry on the next tick", e);
        }
    }
}
