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

/**
 * M22: the use case behind {@code adapters.in.scheduler.PaymentExpirationScheduler}. For every
 * payment still {@code CREATED}/{@code PENDING} past its merchant's configured
 * {@code paymentExpirationSeconds} window ({@link PaymentRepository#findExpirationCandidates}),
 * publishes an {@code EXPIRED} {@code payments.payment-status-changed.v1} record.
 *
 * <p><b>This use case never writes {@code payments.status} itself.</b> That is deliberate, not an
 * oversight: {@code payments.payment-status-changed.v1} already has exactly one consumer inside
 * this service that is allowed to apply it - {@code adapters.in.kafka.PaymentStatusChangedListener}
 * / {@link ApplyPaymentOutcomeUseCase} - and every other producer of this topic (psp-connector)
 * already follows the same "publish, let your own downstream listener apply it" discipline rather
 * than writing the row directly from the code path that decided the outcome. Writing
 * {@code payments.status} from here too would be a second, uncoordinated writer for the exact
 * column {@link com.example.psp.paymentapi.domain.port.PaymentRepository#applyExpiredStatus}'s
 * conditional guard exists to protect, and would desynchronise the {@code payment_status_history}
 * trail (only the listener records history rows) from the {@code payments} table.
 *
 * <h2>The deterministic eventId</h2>
 *
 * <p>{@code UUID.nameUUIDFromBytes(("expired:" + paymentId).getBytes(UTF_8))} (a UUIDv3, RFC 4122
 * §4.3) rather than a fresh {@code UUID.randomUUID()} per publish - on purpose, and unlike every
 * other event this service touches. A candidate that is still {@code CREATED}/{@code PENDING} on
 * the NEXT scheduler tick (the listener has not yet caught up, or the previous publish never
 * reached the broker) is republished with the byte-identical id every time, so
 * {@code payment_status_history}'s {@code UNIQUE(event_id)} constraint (V9) collapses every
 * republish into the same one history row instead of a fresh row per tick - the same
 * "the id, not a dedup table, is the idempotency key" convention this service already relies on
 * for {@code payments.payment-status-changed.v1} redelivery generally. Keying on {@code paymentId}
 * alone (not merchantId/occurredAt/anything time-based) is what makes it reproducible from one
 * tick to the next without this service persisting anything about a sweep in progress.
 *
 * <p>Also why {@code EXPIRED} needed ONE genuinely idempotent, at-least-once-safe way to reach the
 * topic in the first place: unlike psp-connector's terminal publish (stamped once, from a stored
 * {@code statusEventId}, see {@code KafkaPaymentStatusPublisher#publishStatusChanged}), this
 * scheduler has no attempt row to remember an id in - it re-derives candidates from the
 * {@code payments}/{@code merchant_configs} tables fresh on every tick.
 */
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

    /** @return how many candidates this pass published an EXPIRED record for. */
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

    /** {@code UUID.nameUUIDFromBytes} over {@code "expired:" + paymentId} - see class javadoc. */
    static UUID deterministicEventId(UUID paymentId) {
        return UUID.nameUUIDFromBytes(
                (EVENT_ID_NAMESPACE_PREFIX + paymentId).getBytes(StandardCharsets.UTF_8));
    }
}
