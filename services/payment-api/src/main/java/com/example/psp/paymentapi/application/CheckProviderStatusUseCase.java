package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.ProviderStatusResult;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import com.example.psp.paymentapi.domain.port.ProviderStatusPort;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class CheckProviderStatusUseCase {

    private final PaymentRepository paymentRepository;
    private final ProviderStatusPort providerStatusPort;

    public CheckProviderStatusUseCase(PaymentRepository paymentRepository, ProviderStatusPort providerStatusPort) {
        this.paymentRepository = paymentRepository;
        this.providerStatusPort = providerStatusPort;
    }

    public ProviderStatusResult execute(UUID paymentId) {
        Payment payment =
                paymentRepository
                        .findById(paymentId)
                        .orElseThrow(() -> new NoSuchElementException("No payment with id " + paymentId));

        return providerStatusPort.checkStatus(payment.getId(), payment.getMerchantId());
    }
}
