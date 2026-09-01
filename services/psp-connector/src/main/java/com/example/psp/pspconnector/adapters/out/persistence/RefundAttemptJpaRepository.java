package com.example.psp.pspconnector.adapters.out.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RefundAttemptJpaRepository extends JpaRepository<RefundAttemptEntity, UUID> {

    boolean existsByCausationEventId(UUID causationEventId);

    Optional<RefundAttemptEntity> findByCausationEventId(UUID causationEventId);
}
