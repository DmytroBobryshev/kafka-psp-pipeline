package com.example.psp.pspconnector.adapters.out.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentAttemptJpaRepository extends JpaRepository<PaymentAttemptEntity, UUID> {

    boolean existsByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId);

    boolean existsByInboundEventId(UUID inboundEventId);

    Optional<PaymentAttemptEntity> findFirstByPaymentIdOrderByProcessedAtDesc(UUID paymentId);

    Optional<PaymentAttemptEntity> findByInboundEventId(UUID inboundEventId);

    Optional<PaymentAttemptEntity> findByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId);
}
