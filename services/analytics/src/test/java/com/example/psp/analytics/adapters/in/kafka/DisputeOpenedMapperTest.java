package com.example.psp.analytics.adapters.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.analytics.application.ProjectDisputeCommand;
import com.example.psp.common.events.EventEnvelope;
import com.example.psp.common.events.avro.ClaimCheckReference;
import com.example.psp.common.events.avro.DisputeOpened;
import com.example.psp.common.events.avro.InlineDocument;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * M13's "dereference mapper" test - the one the task brief asks for by name. No Spring, no
 * Kafka broker: builds a {@link DisputeOpened} Avro record directly (both union branches) and
 * asserts {@link DisputeOpenedMapper} picks the right branch and copies every field it needs -
 * this is the ONLY place in the analytics module that ever does an {@code instanceof} check on
 * the generated union, so a mistake here (say, swapping which branch means claim-checked) would
 * otherwise only be caught by watching Mongo during a live demo.
 */
class DisputeOpenedMapperTest {

    private final DisputeOpenedMapper mapper = new DisputeOpenedMapper();

    @Test
    void mapsAnInlineDocumentBranchWithBytesInHand() {
        byte[] bytes = {1, 2, 3, 4, 5};
        DisputeOpened event =
                baseBuilder()
                        .setDocument(
                                InlineDocument.newBuilder()
                                        .setContentType("text/plain")
                                        .setSizeBytes(bytes.length)
                                        .setBytes(ByteBuffer.wrap(bytes))
                                        .build())
                        .build();

        ProjectDisputeCommand command = mapper.toCommand(event);

        assertThat(command.claimChecked()).isFalse();
        assertThat(command.inlineBytes()).containsExactly(bytes);
        assertThat(command.bucket()).isNull();
        assertThat(command.objectKey()).isNull();
        assertThat(command.disputeId()).isEqualTo(event.getDisputeId().toString());
        assertThat(command.paymentId()).isEqualTo(event.getPaymentId().toString());
        assertThat(command.merchantId()).isEqualTo("acme");
        assertThat(command.reason()).isEqualTo("goods not received");
    }

    @Test
    void mapsAClaimCheckReferenceBranchWithNoBytes() {
        DisputeOpened event =
                baseBuilder()
                        .setDocument(
                                ClaimCheckReference.newBuilder()
                                        .setBucket("disputes")
                                        .setObjectKey("some-dispute-id")
                                        .setSizeBytes(5_242_880L)
                                        .setContentType("application/pdf")
                                        .build())
                        .build();

        ProjectDisputeCommand command = mapper.toCommand(event);

        assertThat(command.claimChecked()).isTrue();
        assertThat(command.inlineBytes()).isNull();
        assertThat(command.bucket()).isEqualTo("disputes");
        assertThat(command.objectKey()).isEqualTo("some-dispute-id");
        assertThat(command.referenceSizeBytes()).isEqualTo(5_242_880L);
    }

    private DisputeOpened.Builder baseBuilder() {
        EventEnvelope envelope =
                EventEnvelope.root(
                        "disputes.dispute-opened.v1",
                        1,
                        UUID.randomUUID().toString(),
                        "dispute",
                        "payment-api",
                        "trace-1",
                        "corr-1");
        com.example.psp.common.events.avro.EventEnvelope avroEnvelope =
                com.example.psp.common.events.avro.EventEnvelope.newBuilder()
                        .setEventId(envelope.eventId().toString())
                        .setEventType(envelope.eventType())
                        .setEventVersion(envelope.eventVersion())
                        .setAggregateId(envelope.aggregateId())
                        .setAggregateType(envelope.aggregateType())
                        .setOccurredAt(envelope.occurredAt())
                        .setSource(envelope.source())
                        .setTraceId(envelope.traceId())
                        .setCorrelationId(envelope.correlationId())
                        .setCausationId(null)
                        .build();

        return DisputeOpened.newBuilder()
                .setEnvelope(avroEnvelope)
                .setDisputeId(UUID.randomUUID().toString())
                .setPaymentId(UUID.randomUUID().toString())
                .setMerchantId("acme")
                .setReason("goods not received");
    }
}
