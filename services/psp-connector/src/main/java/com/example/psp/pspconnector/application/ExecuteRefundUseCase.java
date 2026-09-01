package com.example.psp.pspconnector.application;

import com.example.psp.common.events.UuidV7;
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

        var replayed = attemptLogRepository.findByInboundEventId(inboundEventId);
        if (replayed.isPresent()) {
            deduplicatedCounter.increment();
            log.info(
                    "Deduplicated refund attempt inboundEventId={} refundId={} path=check-first - "
                            + "already recorded, skipping provider call, republishing the outcome event",
                    inboundEventId,
                    command.refundId());
            statusPublisher.publishOutcome(replayed.get());
            return;
        }

        statusPublisher.publishPending(
                command.refundId(),
                command.paymentId(),
                command.merchantId(),
                command.amount(),
                inboundEventId,
                command.traceId(),
                command.correlationId());

        RefundProviderResult result =
                refundProvider.refund(
                        command.refundId(), command.paymentId(), command.merchantId(), command.amount());

        // M23 stage 3 (IPN_RECEIVED) - right after the provider responds.
        statusPublisher.publishIpnReceived(
                command.refundId(),
                command.paymentId(),
                command.merchantId(),
                command.amount(),
                result.providerReference(),
                inboundEventId,
                command.traceId(),
                command.correlationId());

        RefundAttempt attempt =
                RefundAttempt.from(
                        command.refundId(),
                        command.paymentId(),
                        command.merchantId(),
                        command.amount(),
                        result,
                        inboundEventId,
                        UuidV7.generate(),
                        command.traceId(),
                        command.correlationId());

        boolean inserted = attemptLogRepository.tryRecord(attempt);
        if (!inserted) {
            deduplicatedCounter.increment();
            log.info(
                    "Deduplicated refund attempt inboundEventId={} refundId={} path=constraint-race - "
                            + "a concurrent delivery of this inbound event won the insert, "
                            + "republishing the winner's outcome event",
                    inboundEventId,
                    command.refundId());
            // The winner's row, not our losing attempt: its statusEventId is the one that counts.
            attemptLogRepository.findByInboundEventId(inboundEventId).ifPresent(statusPublisher::publishOutcome);
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

        statusPublisher.publishVerified(
                command.refundId(),
                command.paymentId(),
                command.merchantId(),
                command.amount(),
                attempt.getProviderReference(),
                inboundEventId,
                command.traceId(),
                command.correlationId());

        statusPublisher.publishOutcome(attempt);
    }
}
