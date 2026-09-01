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
