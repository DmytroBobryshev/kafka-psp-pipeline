package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Refund;
import com.example.psp.paymentapi.domain.port.RefundExpirationEventPublisher;
import com.example.psp.paymentapi.domain.port.RefundRepository;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ExpireRefundsUseCase {

    private static final Logger log = LoggerFactory.getLogger(ExpireRefundsUseCase.class);
    private static final String EVENT_ID_NAMESPACE_PREFIX = "refund-expired:";

    private final RefundRepository refundRepository;
    private final RefundExpirationEventPublisher publisher;
    private final Clock clock;

    public ExpireRefundsUseCase(
            RefundRepository refundRepository, RefundExpirationEventPublisher publisher, Clock clock) {
        this.refundRepository = refundRepository;
        this.publisher = publisher;
        this.clock = clock;
    }

    public int execute() {
        Instant now = Instant.now(clock);
        List<Refund> candidates = refundRepository.findExpirationCandidates(now);
        if (candidates.isEmpty()) {
            return 0;
        }

        for (Refund candidate : candidates) {
            UUID eventId = deterministicEventId(candidate.getId());
            log.info(
                    "Publishing EXPIRED refundId={} paymentId={} merchantId={} createdAt={} eventId={}",
                    candidate.getId(),
                    candidate.getPaymentId(),
                    candidate.getMerchantId(),
                    candidate.getCreatedAt(),
                    eventId);
            publisher.publishExpired(candidate, eventId, now);
        }
        log.info("Refund expiration sweep complete: {} candidate(s) published as EXPIRED", candidates.size());
        return candidates.size();
    }

    static UUID deterministicEventId(UUID refundId) {
        return UUID.nameUUIDFromBytes(
                (EVENT_ID_NAMESPACE_PREFIX + refundId).getBytes(StandardCharsets.UTF_8));
    }
}
