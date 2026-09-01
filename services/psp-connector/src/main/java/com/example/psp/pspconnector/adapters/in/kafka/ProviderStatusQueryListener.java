package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.avro.ProviderStatusQuery;
import com.example.psp.common.events.avro.ProviderStatusReply;
import com.example.psp.pspconnector.application.CheckProviderStatusUseCase;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
public class ProviderStatusQueryListener {

    private static final Logger log = LoggerFactory.getLogger(ProviderStatusQueryListener.class);

    private final CheckProviderStatusUseCase useCase;
    private final ProviderStatusAvroEventFactory avroEventFactory;

    public ProviderStatusQueryListener(
            CheckProviderStatusUseCase useCase, ProviderStatusAvroEventFactory avroEventFactory) {
        this.useCase = useCase;
        this.avroEventFactory = avroEventFactory;
    }

    @KafkaListener(
            topics = "${psp-connector.kafka.provider-status-query-topic}",
            containerFactory = "providerStatusQueryKafkaListenerContainerFactory")
    @SendTo
    public ProviderStatusReply onMessage(ProviderStatusQuery query, Acknowledgment ack) {
        UUID paymentId = UUID.fromString(query.getPaymentId());
        log.info(
                "Consumed provider-status-query paymentId={} merchantId={}",
                paymentId,
                query.getMerchantId());

        Optional<PaymentAttempt> latestAttempt = useCase.execute(paymentId);

        ProviderStatusReply reply =
                avroEventFactory.toReply(
                        query,
                        latestAttempt,
                        query.getEnvelope().getTraceId(),
                        query.getEnvelope().getCorrelationId());

        log.info(
                "Replying provider-status-reply paymentId={} found={} status={}",
                paymentId,
                reply.getFound(),
                reply.getStatus());

        ack.acknowledge();
        return reply;
    }
}
