package com.example.psp.paymentapi.adapters.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.common.events.avro.EventEnvelope;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.paymentapi.application.ApplyPaymentOutcomeCommand;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * M21: {@code PaymentStatusChangedMapperImpl} has no framework dependency at construction time -
 * same precedent as webhook-notifier's own {@code PaymentStatusChangedListenerTest} - so it is
 * instantiated directly rather than through Spring. Exercises what {@code toCommand}'s
 * hand-written branching exists to demonstrate: PENDING/SUCCEEDED/DECLINED keep their M20
 * domainStatus translation while IPN_RECEIVED/VERIFIED map to a {@code null} one (history-only),
 * and a blank wire {@code providerReference} (PENDING's) becomes {@code null}, never an empty
 * string.
 */
class PaymentStatusChangedMapperTest {

    private final PaymentStatusChangedMapper mapper = new PaymentStatusChangedMapperImpl();

    @Test
    void pendingKeepsItsDomainStatusAndBlankProviderReferenceBecomesNull() {
        ApplyPaymentOutcomeCommand command = mapper.toCommand(event("PENDING", ""));

        assertThat(command.domainStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(command.rawStatus()).isEqualTo("PENDING");
        assertThat(command.providerReference()).isNull();
    }

    @Test
    void succeededMapsToSucceededDomainStatus() {
        ApplyPaymentOutcomeCommand command = mapper.toCommand(event("SUCCEEDED", "prov-ref-1"));

        assertThat(command.domainStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(command.rawStatus()).isEqualTo("SUCCEEDED");
        assertThat(command.providerReference()).isEqualTo("prov-ref-1");
    }

    @Test
    void declinedMapsToFailedDomainStatusButRawStatusStaysDeclined() {
        ApplyPaymentOutcomeCommand command = mapper.toCommand(event("DECLINED", "prov-ref-2"));

        assertThat(command.domainStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(command.rawStatus()).isEqualTo("DECLINED");
        assertThat(command.providerReference()).isEqualTo("prov-ref-2");
    }

    @Test
    void ipnReceivedIsHistoryOnlyWithNullDomainStatus() {
        ApplyPaymentOutcomeCommand command = mapper.toCommand(event("IPN_RECEIVED", "prov-ref-3"));

        assertThat(command.domainStatus()).isNull();
        assertThat(command.rawStatus()).isEqualTo("IPN_RECEIVED");
        assertThat(command.providerReference()).isEqualTo("prov-ref-3");
    }

    @Test
    void verifiedIsHistoryOnlyWithNullDomainStatus() {
        ApplyPaymentOutcomeCommand command = mapper.toCommand(event("VERIFIED", "prov-ref-4"));

        assertThat(command.domainStatus()).isNull();
        assertThat(command.rawStatus()).isEqualTo("VERIFIED");
        assertThat(command.providerReference()).isEqualTo("prov-ref-4");
    }

    @Test
    void expiredMapsToExpiredDomainStatus() {
        ApplyPaymentOutcomeCommand command = mapper.toCommand(event("EXPIRED", ""));

        assertThat(command.domainStatus()).isEqualTo(PaymentStatus.EXPIRED);
        assertThat(command.rawStatus()).isEqualTo("EXPIRED");
        assertThat(command.providerReference()).isNull();
    }

    @Test
    void unknownStatusThrows() {
        assertThatThrownBy(() -> mapper.toCommand(event("SOMETHING_ELSE", "")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PaymentStatusChanged event(String status, String providerReference) {
        UUID paymentId = UUID.randomUUID();
        return PaymentStatusChanged.newBuilder()
                .setEnvelope(
                        EventEnvelope.newBuilder()
                                .setEventId(UUID.randomUUID().toString())
                                .setEventType("payments.payment-status-changed.v1")
                                .setEventVersion(1)
                                .setAggregateId(paymentId.toString())
                                .setAggregateType("payment")
                                .setOccurredAt(Instant.parse("2026-01-01T00:00:00Z"))
                                .setSource("psp-connector")
                                .setTraceId("trace-1")
                                .setCorrelationId("corr-1")
                                .setCausationId(null)
                                .build())
                .setPaymentId(paymentId.toString())
                .setMerchantId("merchant-1")
                .setAmount(BigDecimal.TEN)
                .setCurrency("EUR")
                .setStatus(status)
                .setProviderReference(providerReference)
                .setDeclineReason(null)
                .build();
    }
}
