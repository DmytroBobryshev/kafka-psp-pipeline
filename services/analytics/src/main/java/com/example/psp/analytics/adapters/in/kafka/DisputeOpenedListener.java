package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.application.ProjectDisputeUseCase;
import com.example.psp.common.events.avro.DisputeOpened;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * M13's "check-out" listener: a plain, single-record {@code @KafkaListener} on {@code
 * disputes.dispute-opened.v1} - a THIRD, independent consumer identity in this service alongside
 * the Kafka Streams application ({@code adapters.in.kafka.AnalyticsTopology}) and the batch
 * listener ({@code adapters.in.kafka.PaymentStatusChangedBatchListener}). Own consumer group
 * ({@code analytics.dispute-projection.v1}, {@code config.DisputeKafkaConfig}), own committed
 * offsets.
 *
 * <p>No DLQ - same documented scope boundary every other analytics consumer carries
 * (docs/diagrams/topic-map.md: "analytics and realtime-gateway deliberately have none... rebuilt
 * by resetting offsets"). A record this listener cannot process (a MinIO fetch failure, a
 * malformed reference) is logged and skipped by the container's error handler ({@code
 * config.DisputeKafkaConfig}); {@code disputes.dispute-opened.v1} keeps 30 days of retention, so a
 * consumer-group offset reset replays it.
 *
 * <p>Manual, immediate acknowledgment - {@link ProjectDisputeUseCase#execute} is a single
 * synchronous, idempotent (upsert-by-disputeId) write, the same "ack after the DB write returns"
 * shape {@code payment-api}'s {@code PaymentStatusChangedListener} already uses for an identical
 * reason.
 */
@Component
public class DisputeOpenedListener {

    private static final Logger log = LoggerFactory.getLogger(DisputeOpenedListener.class);

    private final ProjectDisputeUseCase useCase;
    private final DisputeOpenedMapper mapper;

    public DisputeOpenedListener(ProjectDisputeUseCase useCase, DisputeOpenedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${analytics.disputes.dispute-opened-topic}",
            containerFactory = "disputeOpenedKafkaListenerContainerFactory")
    public void onMessage(DisputeOpened event, Acknowledgment ack) {
        log.info(
                "Consumed dispute-opened disputeId={} paymentId={}",
                event.getDisputeId(),
                event.getPaymentId());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
