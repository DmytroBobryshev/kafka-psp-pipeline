package com.example.psp.common.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventEnvelopeTest {

    @Test
    void rootEnvelopeHasNoCause() {
        EventEnvelope envelope =
                EventEnvelope.root(
                        "payments.payment-requested.v1",
                        1,
                        "payment-123",
                        "payment",
                        "payment-api",
                        "trace-1",
                        "corr-1");

        assertNull(envelope.causationId());
    }

    @Test
    void causedByPropagatesTheCauseEventId() {
        UUID causeId = UuidV7.generate();

        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        causeId,
                        "payments.payment-status-changed.v1",
                        1,
                        "payment-123",
                        "payment",
                        "psp-connector",
                        "trace-1",
                        "corr-1");

        assertEquals(causeId, envelope.causationId());
    }

    @Test
    void rejectsBlankRequiredFields() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        EventEnvelope.root(
                                " ", 1, "payment-123", "payment", "payment-api", "trace-1", "corr-1"));
    }

    @Test
    void rejectsNonPositiveEventVersion() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        EventEnvelope.root(
                                "payments.payment-requested.v1",
                                0,
                                "payment-123",
                                "payment",
                                "payment-api",
                                "trace-1",
                                "corr-1"));
    }
}
