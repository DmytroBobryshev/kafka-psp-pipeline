package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import java.util.Optional;
import java.util.UUID;

public interface AttemptLogRepository {

    boolean existsByInboundEventId(UUID inboundEventId);

    boolean existsByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId);

    boolean tryRecord(PaymentAttempt attempt);

    Optional<PaymentAttempt> findLatestByPaymentId(UUID paymentId);

    Optional<PaymentAttempt> findByInboundEventId(UUID inboundEventId);

    Optional<PaymentAttempt> findByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId);
}
