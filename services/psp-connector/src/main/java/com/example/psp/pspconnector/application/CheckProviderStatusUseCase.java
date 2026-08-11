package com.example.psp.pspconnector.application;

import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * M12's responder-side use case: answer "what is the provider status of this payment, right
 * now?" from psp-connector's own {@code payment_attempts} table - the same table M4-M11 only ever
 * wrote to (see {@link com.example.psp.pspconnector.domain.model.PaymentAttempt}'s javadoc).
 *
 * <p>Deliberately thin, same shape as {@code CreatePaymentUseCase} in payment-api: {@code
 * application/} orchestrates a port and does no business logic of its own here - the interesting
 * decisions (correlation, timeout, wire format) live at the Kafka boundary
 * ({@code adapters.in.kafka.ProviderStatusQueryListener}), not in this class.
 */
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
