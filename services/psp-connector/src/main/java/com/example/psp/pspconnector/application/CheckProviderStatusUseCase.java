package com.example.psp.pspconnector.application;

import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class CheckProviderStatusUseCase {

    private final AttemptLogRepository attemptLogRepository;

    public CheckProviderStatusUseCase(AttemptLogRepository attemptLogRepository) {
        this.attemptLogRepository = attemptLogRepository;
    }

    public Optional<PaymentAttempt> execute(UUID paymentId) {
        return attemptLogRepository.findLatestByPaymentId(paymentId);
    }
}
