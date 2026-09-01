package com.example.psp.paymentapi.adapters.out.persistence;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentStatusHistoryJpaRepository extends JpaRepository<PaymentStatusHistoryEntity, UUID> {

    List<PaymentStatusHistoryEntity> findByPaymentIdOrderByOccurredAtAsc(UUID paymentId);
}
