package com.example.psp.paymentapi.adapters.out.persistence;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RefundStatusHistoryJpaRepository extends JpaRepository<RefundStatusHistoryEntity, UUID> {

    List<RefundStatusHistoryEntity> findByRefundIdOrderByOccurredAtAsc(UUID refundId);
}
