package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentExpirationEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ExpirePaymentsUseCase {

    private static final Logger log = LoggerFactory.getLogger(ExpirePaymentsUseCase.class);
    private static final String EVENT_ID_NAMESPACE_PREFIX = "expired:";

    private final PaymentRepository paymentRepository;
    private final PaymentExpirationEventPublisher publisher;
    private final Clock clock;

    public ExpirePaymentsUseCase(
            PaymentRepository paymentRepository, PaymentExpirationEventPublisher publisher, Clock clock) {
        this.paymentRepository = paymentRepository;
        this.publisher = publisher;
        this.clock = clock;
    }

    public int execute() {
        Instant now = Instant.now(clock);
        List<Payment> candidates = paymentRepository.findExpirationCandidates(now);
        if (candidates.isEmpty()) {
            return 0;
        }

        for (Payment candidate : candidates) {
            UUID eventId = deterministicEventId(candidate.getId());
            log.info(
                    "Publishing EXPIRED paymentId={} merchantId={} createdAt={} eventId={}",
                    candidate.getId(),
                    candidate.getMerchantId(),
                    candidate.getCreatedAt(),
                    eventId);
            publisher.publishExpired(candidate, eventId, now);
        }
        log.info("Expiration sweep complete: {} candidate(s) published as EXPIRED", candidates.size());
        return candidates.size();
    }

    static UUID deterministicEventId(UUID paymentId) {
        return UUID.nameUUIDFromBytes(
                (EVENT_ID_NAMESPACE_PREFIX + paymentId).getBytes(StandardCharsets.UTF_8));
    }
}
