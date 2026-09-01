package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.exception.MerchantNotEligibleException;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.model.MerchantView;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.MerchantViewRepository;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CreatePaymentUseCase {

    private final PaymentRepository paymentRepository;
    private final MerchantViewRepository merchantViewRepository;
    private final PaymentEventPublisher paymentEventPublisher;

    public CreatePaymentUseCase(
            PaymentRepository paymentRepository,
            MerchantViewRepository merchantViewRepository,
            PaymentEventPublisher paymentEventPublisher) {
        this.paymentRepository = paymentRepository;
        this.merchantViewRepository = merchantViewRepository;
        this.paymentEventPublisher = paymentEventPublisher;
    }

    @Transactional
    public Payment execute(CreatePaymentCommand command) {
        MerchantView merchant =
                merchantViewRepository
                        .findById(command.merchantId())
                        .orElseThrow(() -> MerchantNotEligibleException.unknown(command.merchantId()));
        if (merchant.status() != MerchantStatus.ACTIVE) {
            throw MerchantNotEligibleException.notActive(command.merchantId(), merchant.status());
        }
        List<String> allowed =
                merchant.allowedCurrencies().isEmpty()
                        ? List.of(merchant.payoutCurrency())
                        : merchant.allowedCurrencies();
        if (!allowed.contains(command.amount().currency())) {
            throw MerchantNotEligibleException.currencyNotAllowed(
                    command.merchantId(), allowed, command.amount().currency());
        }

        Payment payment = Payment.create(command.merchantId(), command.amount());
        Payment saved = paymentRepository.save(payment);
        paymentEventPublisher.publishPaymentCreated(saved);
        return saved;
    }
}
