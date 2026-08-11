package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.Refund;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import com.example.psp.paymentapi.domain.port.RefundEventPublisher;
import com.example.psp.paymentapi.domain.port.RefundRepository;
import java.math.BigDecimal;
import java.util.NoSuchElementException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * M11's entry point (ADR-0004: commands enter via REST; everything else is events): the single use
 * case behind {@code POST /api/payments/{paymentId}/refunds}. Validates the request against the
 * original payment, persists a local {@link Refund} row, and publishes
 * {@code refunds.refund-requested.v1} - the first event of the choreography saga (ADR-0008).
 *
 * <h2>What "validate against the payment" means here, and what it deliberately does not mean</h2>
 *
 * <p>This service checks that {@code sum(amounts already requested for this payment) + this
 * request's amount} does not exceed the payment's original amount - a fast-fail bounds check using
 * only data this service already owns. It does <b>not</b> check whether the payment actually
 * succeeded at the provider: payment-api has no consumer of
 * {@code payments.payment-status-changed.v1} anywhere in this system (that state lives in
 * psp-connector's {@code payment_attempts} and, derived from it, the ledger's
 * {@code merchant_balances} - ADR-0005 forbids a second copy here), and wiring one is outside this
 * module's declared scope (services/ledger, services/psp-connector). The REAL check for "can this
 * merchant afford this refund" is the ledger's balance reservation
 * ({@code ReserveRefundUseCase}) - a merchant balance can never reflect a payment that never
 * succeeded, so an attempt to refund one fails there with {@code refunds.refund-failed.v1
 * reason=INSUFFICIENT_BALANCE}, exactly like any other over-refund. This service's check is a
 * courtesy fast-fail, not the authority.
 *
 * <p>{@code @Transactional} is the M6 outbox fix, same reasoning as {@link CreatePaymentUseCase}:
 * {@link RefundEventPublisher} does no I/O to Kafka, only a JPA insert into the SAME
 * {@code outbox_event} table {@code PaymentEventPublisher} uses, so both writes commit atomically
 * on the one Postgres connection this annotation opens.
 */
@Service
public class RequestRefundUseCase {

    private final PaymentRepository paymentRepository;
    private final RefundRepository refundRepository;
    private final RefundEventPublisher refundEventPublisher;

    public RequestRefundUseCase(
            PaymentRepository paymentRepository,
            RefundRepository refundRepository,
            RefundEventPublisher refundEventPublisher) {
        this.paymentRepository = paymentRepository;
        this.refundRepository = refundRepository;
        this.refundEventPublisher = refundEventPublisher;
    }

    @Transactional
    public Refund execute(RequestRefundCommand command) {
        Payment payment =
                paymentRepository
                        .findById(command.paymentId())
                        .orElseThrow(
                                () ->
                                        new NoSuchElementException(
                                                "No payment with id=" + command.paymentId()));

        if (!payment.getAmount().currency().equals(command.amount().currency())) {
            throw new IllegalArgumentException(
                    "refund currency "
                            + command.amount().currency()
                            + " does not match payment currency "
                            + payment.getAmount().currency());
        }

        BigDecimal alreadyRequested = refundRepository.sumRequestedAmount(command.paymentId());
        BigDecimal totalAfterThisRequest = alreadyRequested.add(command.amount().amount());
        if (totalAfterThisRequest.compareTo(payment.getAmount().amount()) > 0) {
            throw new IllegalArgumentException(
                    "refund amount "
                            + command.amount().amount()
                            + " (already requested "
                            + alreadyRequested
                            + ") would exceed payment amount "
                            + payment.getAmount().amount()
                            + " for paymentId="
                            + command.paymentId());
        }

        Refund refund =
                Refund.request(payment.getId(), payment.getMerchantId(), command.amount(), command.reason());
        Refund saved = refundRepository.save(refund);
        refundEventPublisher.publishRefundRequested(saved);
        return saved;
    }
}
