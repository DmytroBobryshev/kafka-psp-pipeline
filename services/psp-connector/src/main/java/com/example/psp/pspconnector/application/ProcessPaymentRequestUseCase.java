package com.example.psp.pspconnector.application;

import com.example.psp.pspconnector.domain.exception.ProviderTimeoutException;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import com.example.psp.pspconnector.domain.port.PaymentProviderPort;
import com.example.psp.pspconnector.domain.port.PaymentStatusPublisher;
import org.springframework.stereotype.Service;

/**
 * The single use case: authorize a payment against the (simulated) provider, record the attempt,
 * and - unless the provider timed out - publish the resulting status change.
 *
 * <p>{@code application/} orchestrates ports and MAY use Spring annotations, but never imports an
 * adapter type directly (ADR-0007).
 */
@Service
public class ProcessPaymentRequestUseCase {

    private final PaymentProviderPort paymentProvider;
    private final AttemptLogRepository attemptLogRepository;
    private final PaymentStatusPublisher statusPublisher;

    public ProcessPaymentRequestUseCase(
            PaymentProviderPort paymentProvider,
            AttemptLogRepository attemptLogRepository,
            PaymentStatusPublisher statusPublisher) {
        this.paymentProvider = paymentProvider;
        this.attemptLogRepository = attemptLogRepository;
        this.statusPublisher = statusPublisher;
    }

    public void execute(ProcessPaymentRequestCommand command) {
        ProviderResult result =
                paymentProvider.authorize(command.paymentId(), command.merchantId(), command.amount());

        PaymentAttempt attempt =
                PaymentAttempt.from(
                        command.paymentId(),
                        command.merchantId(),
                        command.amount(),
                        result,
                        command.causationEventId(),
                        command.traceId(),
                        command.correlationId());

        // "M5 builds real idempotency on this; for now just record attempts" - every attempt is
        // written here regardless of outcome, including TIMEOUT. There is no read-before-write
        // check against this table yet, so a redelivered payments.payment-requested.v1 record
        // (e.g. after the manual-ack duplicates-vs-loss drill, or a future M8 retry) calls the
        // provider AGAIN and inserts a second, distinct row for the same paymentId - a genuine
        // duplicate provider charge. Real idempotency (query this table first, skip the call if
        // an attempt already exists) is M5's job, not M4's.
        attemptLogRepository.record(attempt);

        if (attempt.getOutcome() == ProviderOutcome.TIMEOUT) {
            // ADR-0006 category A (retryable). Deliberately NOT published, and this throw
            // propagates straight out of adapters.in.kafka.PaymentRequestedListener uncaught, so
            // Acknowledgment.acknowledge() is never reached for this record - see
            // config.KafkaConsumerConfig for what the container's error handler does next.
            throw new ProviderTimeoutException(command.paymentId());
        }

        // ADR-0006 category B: APPROVED and DECLINED are both business outcomes, not errors.
        // Both publish a status event and both let the listener ack normally afterwards - a
        // decline is the answer, not a failure, and must never be retried or DLQ'd.
        statusPublisher.publishStatusChanged(attempt);
    }
}
