package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.ProviderStatusResult;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import com.example.psp.paymentapi.domain.port.ProviderStatusPort;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * M12's requester-side use case: look the payment up locally first (so an unknown paymentId is a
 * fast 404 that never touches Kafka at all), then perform the synchronous provider-status round
 * trip via {@link ProviderStatusPort}.
 *
 * <p>{@code application/} orchestrates ports only (ADR-0007) - which port implementation actually
 * talks to Kafka ({@code adapters.out.kafka.ProviderStatusRequestGateway}) is a {@code config/}
 * wiring concern this class knows nothing about.
 */
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
