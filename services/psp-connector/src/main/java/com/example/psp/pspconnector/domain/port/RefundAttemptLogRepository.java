package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.RefundAttempt;
import java.util.Optional;
import java.util.UUID;

public interface RefundAttemptLogRepository {

    boolean existsByInboundEventId(UUID inboundEventId);

    boolean tryRecord(RefundAttempt attempt);

    Optional<RefundAttempt> findByInboundEventId(UUID inboundEventId);
}
