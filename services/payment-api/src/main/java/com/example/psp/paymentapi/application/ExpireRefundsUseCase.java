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

/**
 * M24: the refund-path sibling of {@code application.ExpirePaymentsUseCase}, behind
 * {@code adapters.in.scheduler.RefundExpirationScheduler}. For every refund past its merchant's
 * configured {@code refundExpirationSeconds} window with no terminal
 * {@code refund_status_history} row yet ({@link RefundRepository#findExpirationCandidates}),
 * publishes an {@code EXPIRED} {@code refunds.refund-status-changed.v1} record.
 *
 * <p><b>This use case never writes anything about the {@code Refund} aggregate itself</b> - unlike
 * {@code ExpirePaymentsUseCase}, there is not even a status column to conditionally update:
 * {@link Refund}'s status is always {@code REQUESTED} and never advances (see that class's
 * javadoc). The EXPIRED verdict this use case publishes is entirely history-only, applied via this
 * service's own pre-existing {@code adapters.in.kafka.RefundStatusChangedListener} /
 * {@code RecordRefundHistoryUseCase} - the exact same listener that already records PENDING/
 * IPN_RECEIVED/VERIFIED rows for this topic, unmodified. A later genuinely-terminal outcome
 * (COMPLETED/FAILED, the provider's own authoritative verdict) still lands in
 * {@code refund_status_history} too even after an EXPIRED row is recorded - this sweep's
 * {@code NOT EXISTS} guard only stops it from being resurfaced as a candidate again, it does not
 * and cannot prevent the provider from eventually reporting the real outcome.
 *
 * <p>Money itself never leaks regardless of what this use case does or does not do: the ledger's
 * own {@code ReservationTtlSweeper} independently releases a stuck reservation
 * ({@code refunds.reservation-released.v1} reason {@code TIMEOUT}) on its own schedule, untouched
 * by this feature. This sweep is the merchant-facing verdict layered on top of that - "your refund
 * did not complete in time" - not a second safety net for the money.
 *
 * <h2>The deterministic eventId</h2>
 *
 * <p>{@code UUID.nameUUIDFromBytes(("refund-expired:" + refundId).getBytes(UTF_8))} - the same
 * UUIDv3 scheme {@code ExpirePaymentsUseCase} uses for payments, with its own namespace prefix so
 * the two never collide by coincidence. A candidate still lacking a terminal history row on the
 * NEXT tick (this service's own listener has not yet caught up, or the previous publish never
 * reached the broker) is republished with the byte-identical id every time, so
 * {@code refund_status_history}'s {@code UNIQUE(event_id)} constraint (V12) collapses every
 * republish into the same one history row instead of a fresh row per tick.
 */
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

    /** @return how many candidates this pass published an EXPIRED record for. */
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

    /** {@code UUID.nameUUIDFromBytes} over {@code "refund-expired:" + refundId} - see class javadoc. */
    static UUID deterministicEventId(UUID refundId) {
        return UUID.nameUUIDFromBytes(
                (EVENT_ID_NAMESPACE_PREFIX + refundId).getBytes(StandardCharsets.UTF_8));
    }
}
