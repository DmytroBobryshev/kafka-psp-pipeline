package com.example.psp.ledger.adapters.out.persistence;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RefundReservationJpaRepository extends JpaRepository<RefundReservationEntity, UUID> {
}
