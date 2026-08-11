package com.example.psp.pspconnector.application;

import com.example.psp.pspconnector.domain.model.RefundAttempt;
import com.example.psp.pspconnector.domain.model.RefundProviderResult;
import com.example.psp.pspconnector.domain.port.RefundAttemptLogRepository;
import com.example.psp.pspconnector.domain.port.RefundProviderPort;
import com.example.psp.pspconnector.domain.port.RefundStatusPublisher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * M11 step 3: consumes {@code refunds.funds-reserved.v1}, executes the refund against the
 * (simulated) provider, records the attempt, and publishes the outcome - either
 * {@code refunds.refund-completed.v1} or {@code refunds.refund-failed.v1}
 * ({@link RefundStatusPublisher}). A provider decline is ADR-0006 category B - a business
 * outcome, not an error: it publishes and returns normally, exactly like a payment decline in
 * {@link ProcessPaymentRequestUseCase}.
 *
 * <p>Idempotent the M5 level-1 way, and ONLY level 1 - see {@code domain.model.RefundAttempt}'s
 * javadoc for why this module does not also replicate M5's level 2 (duplicate provider callback).
 * {@link RefundAttemptLogRepository#existsByInboundEventId} is the check-first path; a lost race
 * inside {@link RefundAttemptLogRepository#tryRecord} is the constraint-race path, reported by a
 * {@code false} return, never by throwing.
 */
@Service
public class ExecuteRefundUseCase {

    private static final Logger log = LoggerFactory.getLogger(ExecuteRefundUseCase.class);

    private final RefundProviderPort refundProvider;
    private final RefundAttemptLogRepository attemptLogRepository;
    private final RefundStatusPublisher statusPublisher;
    private final Counter processedCounter;
    private final Counter deduplicatedCounter;

    public ExecuteRefundUseCase(
            RefundProviderPort refundProvider,
            RefundAttemptLogRepository attemptLogRepository,
            RefundStatusPublisher statusPublisher,
            MeterRegistry meterRegistry) {
        this.refundProvider = refundProvider;
        this.attemptLogRepository = attemptLogRepository;
        this.statusPublisher = statusPublisher;
        this.processedCounter =
                Counter.builder("psp-connector.refund.attempts.processed")
                        .description("Refund attempts newly recorded and processed (not a duplicate)")
                        .register(meterRegistry);
        this.deduplicatedCounter =
                Counter.builder("psp-connector.refund.attempts.deduplicated")
                        .description(
                                "refunds.funds-reserved.v1 deliveries already processed - M5 level 1, "
                                        + "keyed on the inbound eventId, checked before the provider is ever "
                                        + "called")
                        .register(meterRegistry);
    }

    public void execute(ExecuteRefundCommand command) {
        UUID inboundEventId = command.causationEventId();

        if (attemptLogRepository.existsByInboundEventId(inboundEventId)) {
            deduplicatedCounter.increment();
            log.info(
                    "Deduplicated refund attempt inboundEventId={} refundId={} path=check-first - "
                            + "already processed, skipping provider call and publish",
                    inboundEventId,
                    command.refundId());
            return;
        }

        RefundProviderResult result =
                refundProvider.refund(
                        command.refundId(), command.paymentId(), command.merchantId(), command.amount());

        RefundAttempt attempt =
                RefundAttempt.from(
                        command.refundId(),
                        command.paymentId(),
                        command.merchantId(),
                        command.amount(),
                        result,
                        inboundEventId,
                        command.traceId(),
                        command.correlationId());

        boolean inserted = attemptLogRepository.tryRecord(attempt);
        if (!inserted) {
            deduplicatedCounter.increment();
            log.info(
                    "Deduplicated refund attempt inboundEventId={} refundId={} path=constraint-race - "
                            + "a concurrent delivery of this inbound event won the insert, skipping publish",
                    inboundEventId,
                    command.refundId());
            return;
        }

        processedCounter.increment();
        log.info(
                "Executed refund refundId={} paymentId={} merchantId={} amount={} outcome={} "
                        + "providerReference={}",
                command.refundId(),
                command.paymentId(),
                command.merchantId(),
                command.amount().amount(),
                attempt.getOutcome(),
                attempt.getProviderReference());

        // ADR-0006 category B: COMPLETED and DECLINED are both business outcomes, not errors. Both
        // publish and both let the listener commit normally afterwards.
        statusPublisher.publishOutcome(attempt);
    }
}
