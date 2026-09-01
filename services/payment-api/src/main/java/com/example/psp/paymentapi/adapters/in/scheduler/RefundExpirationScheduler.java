package com.example.psp.paymentapi.adapters.in.scheduler;

import com.example.psp.paymentapi.application.ExpireRefundsUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * M24's refund-expiration sweep trigger - the refund-path sibling of
 * {@link PaymentExpirationScheduler}, not an extension of it: a refund that has sat past its
 * merchant's window is a different candidate query, against a different table, guarded by a
 * different "already settled?" check (no local status column to gate on - {@code
 * NOT EXISTS (... refund_status_history ...)} instead of {@code status IN (...)}, see
 * {@code adapters.out.persistence.RefundJpaRepository#findExpirationCandidates}'s javadoc). Kept as
 * a separate {@code @Component}/{@code @Scheduled} pair rather than folding into
 * {@link PaymentExpirationScheduler} for the same reason {@code ExpirePaymentsUseCase} and
 * {@code ExpireRefundsUseCase} are separate services: one candidate list, one publish loop, one
 * failure domain each - a Kafka hiccup mid-way through the refund sweep must not also skip the
 * payment sweep's tick, and vice versa.
 *
 * <p>Reuses the SAME {@code payment-api.expiration.sweep-interval} property (default {@code PT5S})
 * as {@link PaymentExpirationScheduler} - "keep the 5s sweep" applies to both; there is no reason
 * for the two sweeps to run on independent cadences, and one property means one place to change
 * both for an operator running the demo.
 */
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
            // Same reasoning as PaymentExpirationScheduler#sweep: an escaped exception stops
            // future @Scheduled executions entirely (Spring's default TaskScheduler behaviour) -
            // catching and logging keeps this sweep running on its next tick even if one pass hit
            // a transient Postgres/Kafka problem.
            log.error("Refund expiration sweep failed - will retry on the next tick", e);
        }
    }
}
