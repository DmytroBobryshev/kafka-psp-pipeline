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

/**
 * M12's request-reply responder: {@code psp.provider-status-query.v1} -&gt;
 * {@code psp.provider-status-reply.v1}, correlated by Kafka header (ADR-0004's "synchronous need"
 * carve-out - see services/payment-api/README.md's M12 section for the full ADR-0004 discussion
 * and the requesting side's {@code ReplyingKafkaTemplate}).
 *
 * <h2>How the reply mechanics work, from this side</h2>
 *
 * <p>Spring Kafka's {@code @SendTo} (no explicit topic argument) tells the container - once its
 * factory has a {@code replyTemplate} configured ({@code config.ProviderStatusKafkaConfig}) - to
 * take this method's return value and publish it to whatever topic the INBOUND record's
 * {@code KafkaHeaders.REPLY_TOPIC} header names, copying {@code KafkaHeaders.CORRELATION_ID} (and
 * {@code REPLY_PARTITION}, when the requester set one) onto the outbound record automatically.
 * This class never reads or writes those headers itself, and never needs to know the reply
 * topic's name - {@code psp.provider-status-reply.v1} appears nowhere in this file. That is the
 * entire point of header-based correlation: the responder is a pure function of the request, with
 * zero coupling to who is asking or where they want the answer sent.
 *
 * <p>Deliberately thin and exception-agnostic, same convention as every other listener in this
 * service (see {@code PaymentRequestedListener}): on success it acks after returning the reply;
 * an exception here propagates out uncaught and the container's default error handling applies -
 * no custom retry classification is added for this listener (a status CHECK has no side effect to
 * protect against re-running, unlike an authorization call, so redelivery on failure is harmless
 * by construction).
 */
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
