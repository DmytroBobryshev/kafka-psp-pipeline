package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.RefundAttempt;

/**
 * Outbound port for publishing the refund saga's execution outcome (M11). Unlike
 * {@link PaymentStatusPublisher} (which always publishes to the same topic, encoding the outcome
 * in a {@code status} field), COMPLETED and DECLINED map to two DIFFERENT topics here -
 * {@code refunds.refund-completed.v1} and {@code refunds.refund-failed.v1} - per the module brief
 * (docs/PLAN.md M11) and ADR-0008's topology. One method, because the caller
 * ({@code application.ExecuteRefundUseCase}) should not have to know which topic a given outcome
 * belongs to - that routing decision lives entirely in the adapter
 * ({@code adapters.out.kafka.KafkaRefundStatusPublisher}).
 */
public interface RefundStatusPublisher {

    void publishOutcome(RefundAttempt attempt);
}
