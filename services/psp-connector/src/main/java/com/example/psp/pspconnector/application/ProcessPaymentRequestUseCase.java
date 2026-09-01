package com.example.psp.pspconnector.application;

import com.example.psp.common.events.UuidV7;
import com.example.psp.pspconnector.domain.exception.ProviderTimeoutException;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import com.example.psp.pspconnector.domain.port.PaymentProviderPort;
import com.example.psp.pspconnector.domain.port.PaymentStatusPublisher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProcessPaymentRequestUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessPaymentRequestUseCase.class);

    private final PaymentProviderPort paymentProvider;
    private final AttemptLogRepository attemptLogRepository;
    private final PaymentStatusPublisher statusPublisher;
    private final Counter processedCounter;
    private final Counter deduplicatedReplayCounter;
    private final Counter deduplicatedProviderCallbackCounter;

    public ProcessPaymentRequestUseCase(
            PaymentProviderPort paymentProvider,
            AttemptLogRepository attemptLogRepository,
            PaymentStatusPublisher statusPublisher,
            MeterRegistry meterRegistry) {
        this.paymentProvider = paymentProvider;
        this.attemptLogRepository = attemptLogRepository;
        this.statusPublisher = statusPublisher;
        this.processedCounter =
                Counter.builder("psp-connector.payment.attempts.processed")
                        .description(
                                "Payment attempts newly recorded and processed (not an M5 duplicate)")
                        .register(meterRegistry);
        this.deduplicatedReplayCounter =
                Counter.builder("psp-connector.payment.attempts.deduplicated")
                        .tag("reason", "replay")
                        .description(
                                "M5 level 1: inbound-event replays skipped BEFORE calling the provider - "
                                        + "the fix for the proven defect described in this class's javadoc. "
                                        + "Keyed on the inbound EventEnvelope.eventId, not anything the "
                                        + "provider mints.")
                        .register(meterRegistry);
        this.deduplicatedProviderCallbackCounter =
                Counter.builder("psp-connector.payment.attempts.deduplicated")
                        .tag("reason", "provider-callback")
                        .description(
                                "M5 level 2 (unchanged): duplicate (paymentId, providerEventId) provider "
                                        + "callbacks skipped - a different failure mode than level 1, which "
                                        + "cannot see it; see class javadoc")
                        .register(meterRegistry);
    }

    public void execute(ProcessPaymentRequestCommand command) {
        UUID inboundEventId = command.causationEventId();

        Optional<PaymentAttempt> replayed = attemptLogRepository.findByInboundEventId(inboundEventId);
        if (replayed.isPresent()) {
            recordDuplicate(DedupReason.REPLAY, command, inboundEventId, null);
            republish(replayed.get());
            return;
        }

        statusPublisher.publishPending(
                command.paymentId(),
                command.merchantId(),
                command.amount(),
                inboundEventId,
                command.traceId(),
                command.correlationId());

        ProviderResult result =
                paymentProvider.authorize(command.paymentId(), command.merchantId(), command.amount());

        if (result.outcome() != ProviderOutcome.TIMEOUT) {
            statusPublisher.publishIpnReceived(
                    command.paymentId(),
                    command.merchantId(),
                    command.amount(),
                    result.providerEventId(),
                    inboundEventId,
                    command.traceId(),
                    command.correlationId());
        }

        Optional<PaymentAttempt> callbackDuplicate =
                attemptLogRepository.findByPaymentIdAndProviderEventId(
                        command.paymentId(), result.providerEventId());
        if (callbackDuplicate.isPresent()) {
            recordDuplicate(
                    DedupReason.PROVIDER_CALLBACK, command, inboundEventId, result.providerEventId());
            republish(callbackDuplicate.get());
            return;
        }

        PaymentAttempt attempt =
                PaymentAttempt.from(
                        command.paymentId(),
                        command.merchantId(),
                        command.amount(),
                        result,
                        command.causationEventId(),
                        UuidV7.generate(),
                        command.traceId(),
                        command.correlationId());

        boolean inserted = attemptLogRepository.tryRecord(attempt);
        if (!inserted) {
            Optional<PaymentAttempt> replayWinner = attemptLogRepository.findByInboundEventId(inboundEventId);
            DedupReason reason = replayWinner.isPresent() ? DedupReason.REPLAY : DedupReason.PROVIDER_CALLBACK;
            recordDuplicate(reason, command, inboundEventId, result.providerEventId());
            replayWinner
                    .or(
                            () ->
                                    attemptLogRepository.findByPaymentIdAndProviderEventId(
                                            command.paymentId(), result.providerEventId()))
                    .ifPresent(this::republish);
            return;
        }

        processedCounter.increment();

        if (attempt.getOutcome() != ProviderOutcome.TIMEOUT) {
            statusPublisher.publishVerified(
                    command.paymentId(),
                    command.merchantId(),
                    command.amount(),
                    result.providerEventId(),
                    inboundEventId,
                    command.traceId(),
                    command.correlationId());
        }

        if (attempt.getOutcome() == ProviderOutcome.TIMEOUT) {
            throw new ProviderTimeoutException(command.paymentId());
        }

        statusPublisher.publishStatusChanged(attempt);
    }

    private void republish(PaymentAttempt attempt) {
        if (attempt.getOutcome() == ProviderOutcome.TIMEOUT) {
            return;
        }
        statusPublisher.publishStatusChanged(attempt);
    }

    private void recordDuplicate(
            DedupReason reason,
            ProcessPaymentRequestCommand command,
            UUID inboundEventId,
            UUID providerEventId) {
        switch (reason) {
            case REPLAY -> deduplicatedReplayCounter.increment();
            case PROVIDER_CALLBACK -> deduplicatedProviderCallbackCounter.increment();
        }
        log.info(
                "Deduplicated payment attempt reason={} paymentId={} inboundEventId={} "
                        + "providerEventId={} merchantId={} - already recorded, skipping attempt-log "
                        + "write, republishing the stored status event, acknowledging normally",
                reason,
                command.paymentId(),
                inboundEventId,
                providerEventId,
                command.merchantId());
    }

    private enum DedupReason {
        REPLAY,
        PROVIDER_CALLBACK
    }
}
