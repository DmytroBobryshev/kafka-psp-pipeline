package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundStatusChanged;
import com.example.psp.webhooknotifier.application.PlanWebhookDeliveryUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * M24's fourth planner listener (same {@code webhook-notifier.planner.v1} consumer group as
 * {@link RefundCompletedListener}/{@link RefundFailedListener}, per
 * {@code config.KafkaConsumerConfig}'s {@code refundStatusChangedKafkaListenerContainerFactory}):
 * consumes {@code refunds.refund-status-changed.v1} but, unlike every other planner listener in
 * this service, does NOT plan a delivery for every event on the topic - only for {@code status ==
 * "EXPIRED"}.
 *
 * <p><b>The allowlist, and why it exists.</b> {@code refunds.refund-status-changed.v1} carries
 * PENDING/IPN_RECEIVED/VERIFIED (psp-connector's own saga-progress trail, produced on every refund
 * attempt) alongside EXPIRED (payment-api's own refund-expiration sweep verdict, M24). A merchant
 * webhook exists to notify about outcomes worth acting on - PENDING/IPN_RECEIVED/VERIFIED are
 * intermediate saga bookkeeping with nothing for a merchant to do about them, and planning a
 * delivery for all four values would flood a merchant's endpoint with noise for every refund that
 * completes normally. EXPIRED is the one value on this topic that IS a merchant-facing outcome -
 * "this refund did not resolve in time" - so it is the only one this listener reacts to. This is a
 * closed allowlist, not an exclude-list: a future status value added to this topic is silently
 * skipped by default, not accidentally delivered.
 *
 * <p>Same scope boundary as the other refund planner listeners: no DLQ for this topic (a pure
 * translation step over a source topic that already has its own consumer/DLQ elsewhere - payment-
 * api's {@code RefundStatusChangedListener}), zero retries via the shared error handler,
 * manual-immediate ack either way (skip or plan).
 */
@Component
public class RefundExpiredListener {

    private static final Logger log = LoggerFactory.getLogger(RefundExpiredListener.class);
    private static final String EXPIRED_STATUS = "EXPIRED";

    private final PlanWebhookDeliveryUseCase useCase;
    private final RefundExpiredMapper mapper;

    public RefundExpiredListener(PlanWebhookDeliveryUseCase useCase, RefundExpiredMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${webhook-notifier.kafka.refund-status-changed-topic}",
            containerFactory = "refundStatusChangedKafkaListenerContainerFactory")
    public void onMessage(RefundStatusChanged event, Acknowledgment ack) {
        if (!EXPIRED_STATUS.equals(event.getStatus())) {
            // PENDING/IPN_RECEIVED/VERIFIED - not a merchant-facing outcome, see class javadoc.
            log.debug(
                    "Skipping refund-status-changed refundId={} status={} - not a webhook trigger",
                    event.getRefundId(),
                    event.getStatus());
            ack.acknowledge();
            return;
        }

        log.info(
                "Consumed refund-status-changed EXPIRED refundId={} paymentId={} merchantId={}",
                event.getRefundId(),
                event.getPaymentId(),
                event.getMerchantId());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
