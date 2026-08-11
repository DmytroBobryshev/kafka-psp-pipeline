package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.common.events.avro.ProviderStatusQuery;
import com.example.psp.common.events.avro.ProviderStatusReply;
import com.example.psp.paymentapi.domain.exception.ProviderStatusTimeoutException;
import com.example.psp.paymentapi.domain.model.ProviderStatusResult;
import com.example.psp.paymentapi.domain.port.ProviderStatusPort;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.requestreply.KafkaReplyTimeoutException;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.stereotype.Component;

/**
 * Real adapter for {@link ProviderStatusPort} (M12): sends {@code psp.provider-status-query.v1}
 * and blocks the calling thread for {@code psp.provider-status-reply.v1} via
 * {@link ReplyingKafkaTemplate}, turning an async Kafka round trip into the synchronous answer
 * {@code adapters.in.web.ProviderStatusController} needs. See
 * {@code config.ReplyingKafkaConfig}'s javadoc for the reply-topic-partitions/multi-instance
 * mechanics and the timeout's justification, and the README's M12 section for the ADR-0004
 * discussion this class is the concrete instance of.
 *
 * <p>{@code sendAndReceive} adds {@code KafkaHeaders.REPLY_TOPIC} (from the reply container's own
 * topic) and {@code KafkaHeaders.CORRELATION_ID} (a fresh random id per call) to the outgoing
 * record automatically - this class never touches either header directly. The returned {@link
 * RequestReplyFuture} is completed by the template itself, either with the matching reply or
 * exceptionally with a {@link KafkaReplyTimeoutException} once {@code
 * ReplyingKafkaConfig}'s {@code defaultReplyTimeout} elapses - calling {@code future.get()} with
 * no explicit timeout here is intentional and safe for exactly that reason (the template, not the
 * caller, owns the deadline).
 */
@Component
public class ProviderStatusRequestGateway implements ProviderStatusPort {

    private static final Logger log = LoggerFactory.getLogger(ProviderStatusRequestGateway.class);
    private static final String EVENT_TYPE = "psp.provider-status-query.v1";
    private static final String SOURCE = "payment-api";
    private static final String AGGREGATE_TYPE = "payment";

    private final ReplyingKafkaTemplate<String, ProviderStatusQuery, ProviderStatusReply> replyingKafkaTemplate;
    private final ProviderStatusQueryAvroEventFactory avroEventFactory;
    private final String queryTopic;

    public ProviderStatusRequestGateway(
            ReplyingKafkaTemplate<String, ProviderStatusQuery, ProviderStatusReply> replyingKafkaTemplate,
            ProviderStatusQueryAvroEventFactory avroEventFactory,
            @Value("${payment-api.kafka.provider-status-query-topic}") String queryTopic) {
        this.replyingKafkaTemplate = replyingKafkaTemplate;
        this.avroEventFactory = avroEventFactory;
        this.queryTopic = queryTopic;
    }

    @Override
    public ProviderStatusResult checkStatus(UUID paymentId, String merchantId) {
        String correlationId = MDC.get("correlationId");
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        String traceId = correlationId;

        EventEnvelope envelope =
                EventEnvelope.root(
                        EVENT_TYPE, 1, paymentId.toString(), AGGREGATE_TYPE, SOURCE, traceId, correlationId);
        ProviderStatusQuery query = avroEventFactory.toAvro(envelope, paymentId.toString(), merchantId);

        ProducerRecord<String, ProviderStatusQuery> record =
                new ProducerRecord<>(queryTopic, paymentId.toString(), query);

        long startedAt = System.nanoTime();
        log.info("Sending provider-status-query paymentId={} merchantId={}", paymentId, merchantId);

        RequestReplyFuture<String, ProviderStatusQuery, ProviderStatusReply> future =
                replyingKafkaTemplate.sendAndReceive(record);

        try {
            ConsumerRecord<String, ProviderStatusReply> replyRecord = future.get();
            long roundTripMillis = (System.nanoTime() - startedAt) / 1_000_000;
            ProviderStatusReply reply = replyRecord.value();

            log.info(
                    "Received provider-status-reply paymentId={} found={} status={} roundTripMillis={}",
                    paymentId,
                    reply.getFound(),
                    reply.getStatus(),
                    roundTripMillis);

            return new ProviderStatusResult(
                    paymentId,
                    merchantId,
                    reply.getFound(),
                    reply.getStatus(),
                    reply.getProviderReference(),
                    reply.getCheckedAt() == null ? Instant.now() : reply.getCheckedAt(),
                    roundTripMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProviderStatusTimeoutException(paymentId, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof KafkaReplyTimeoutException) {
                log.warn("provider-status-query timed out paymentId={}", paymentId);
                throw new ProviderStatusTimeoutException(paymentId, cause);
            }
            throw new IllegalStateException(
                    "provider-status-query failed for paymentId=" + paymentId, cause);
        }
    }
}
