package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.port.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * M19: the use case behind {@code adapters.in.kafka.PaymentStatusChangedListener} - payment-api's
 * first consumer of {@code payments.payment-status-changed.v1}, closing the gap the module's
 * README has documented since M3: this table has sat frozen in {@code CREATED} for every payment
 * ever created, because nothing downstream of psp-connector/ledger ever told it otherwise. The
 * transactions panel (M19) needs a real status, so this listens for the same event the ledger and
 * webhook-notifier already consume and applies it here too - a THIRD, independent local
 * projection of the same fact, not a new source of truth (ADR-0005: each service keeps its own
 * copy of the parts of the wider truth it needs to answer its own queries).
 *
 * <p>Deliberately trivial, one line of orchestration - the interesting property lives in
 * {@link PaymentRepository#updateStatus}, not here: applying an ABSOLUTE status value is what
 * makes redelivery of the same event safe, so this use case does not need its own idempotency
 * check (a second identical call is a no-op by construction, not a special case to detect).
 *
 * <p>{@code @Transactional} for the same structural reason as {@code CreatePaymentUseCase}: the
 * repository call below is a {@code @Modifying} JPA query, and Spring Data JPA requires an active
 * transaction around one (unlike a plain {@code save}/{@code findById}, which the JPA provider
 * happily runs in an implicit, auto-committing one). There is only ever this one write in this use
 * case - no outbox partner, nothing else to make atomic with it - so this is a narrower use of the
 * annotation than {@code CreatePaymentUseCase}'s, but the same annotation for the same underlying
 * reason: Spring Data needs a transaction to run a modifying query in.
 */
@Service
public class ApplyPaymentOutcomeUseCase {

    private static final Logger log = LoggerFactory.getLogger(ApplyPaymentOutcomeUseCase.class);

    private final PaymentRepository paymentRepository;

    public ApplyPaymentOutcomeUseCase(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    @Transactional
    public void execute(ApplyPaymentOutcomeCommand command) {
        log.info(
                "Applying payment outcome paymentId={} status={}", command.paymentId(), command.status());
        paymentRepository.updateStatus(command.paymentId(), command.status());
    }
}
