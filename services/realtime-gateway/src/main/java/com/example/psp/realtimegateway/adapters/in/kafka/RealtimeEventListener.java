package com.example.psp.realtimegateway.adapters.in.kafka;

import com.example.psp.realtimegateway.application.BroadcastRealtimeEventUseCase;
import com.example.psp.realtimegateway.domain.model.RealtimeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RealtimeEventListener {

    private static final Logger log = LoggerFactory.getLogger(RealtimeEventListener.class);

    private final BroadcastRealtimeEventUseCase broadcastUseCase;
    private final RealtimeEventMapper mapper;

    public RealtimeEventListener(BroadcastRealtimeEventUseCase broadcastUseCase, RealtimeEventMapper mapper) {
        this.broadcastUseCase = broadcastUseCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = {
                "${realtime-gateway.kafka.payment-requested-topic}",
                "${realtime-gateway.kafka.payment-status-changed-topic}",
                "${realtime-gateway.kafka.refund-requested-topic}",
                "${realtime-gateway.kafka.funds-reserved-topic}",
                "${realtime-gateway.kafka.refund-completed-topic}",
                "${realtime-gateway.kafka.refund-failed-topic}",
                "${realtime-gateway.kafka.reservation-released-topic}"
            },
            containerFactory = "realtimeKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object event = record.value();
        try {
            RealtimeEvent realtimeEvent = mapper.toDomain(event);
            log.debug(
                    "Broadcasting eventType={} paymentId={} merchantId={}",
                    realtimeEvent.eventType(),
                    realtimeEvent.paymentId(),
                    realtimeEvent.merchantId());
            broadcastUseCase.execute(realtimeEvent);
        } catch (RuntimeException ex) {
            log.warn("Failed to process realtime event topic={} {}, skipping", record.topic(), event, ex);
        }
        ack.acknowledge();
    }
}
