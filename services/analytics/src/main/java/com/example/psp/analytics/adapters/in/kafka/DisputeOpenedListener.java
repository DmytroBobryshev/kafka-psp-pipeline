package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.application.ProjectDisputeUseCase;
import com.example.psp.common.events.avro.DisputeOpened;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

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
