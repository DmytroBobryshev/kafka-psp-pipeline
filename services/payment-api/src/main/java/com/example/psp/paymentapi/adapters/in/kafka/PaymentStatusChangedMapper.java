package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.paymentapi.application.ApplyPaymentOutcomeCommand;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.util.UUID;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/**
 * Mapper at the M19 status-view listener's inbound Kafka boundary: event -&gt; domain command
 * (ADR-0007). {@code componentModel = "spring"} still registers this as a Spring bean the same way
 * every other boundary mapper in this codebase does, but {@code toCommand} (M21) is a hand-written
 * default method rather than a declarative {@code @Mapping} pile - unlike a straight field copy, it
 * now branches on the event's own status vocabulary to decide TWO independent things at once (see
 * {@link ApplyPaymentOutcomeCommand}'s javadoc): the nullable {@code domainStatus} to apply to
 * {@code payments.status}, and the always-present {@code rawStatus} the
 * {@code payment_status_history} row stores - a decision declarative MapStruct mapping cannot
 * express. {@code paymentId}/{@code eventId}'s UUID-shaped fields are plain Avro {@code string},
 * hence the explicit {@code UUID.fromString(...)} calls - the same pattern webhook-notifier's and
 * ledger's own {@code PaymentStatusChangedMapper}s already use for this identical event.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusChangedMapper {

    default ApplyPaymentOutcomeCommand toCommand(PaymentStatusChanged event) {
        String rawStatus = event.getStatus();
        return new ApplyPaymentOutcomeCommand(
                UUID.fromString(event.getPaymentId()),
                toDomainStatus(rawStatus),
                rawStatus,
                blankToNull(event.getProviderReference()),
                UUID.fromString(event.getEnvelope().getEventId()),
                event.getEnvelope().getOccurredAt());
    }

    /**
     * Translates the event's status vocabulary into this table's own, where one exists.
     * {@link PaymentStatus} (M1) predates {@code payments.payment-status-changed.v1}'s (M9 Phase 2)
     * event vocabulary by several milestones and was never renamed to match it.
     *
     * <p>{@code "PENDING"} (M20 - the pre-provider-call status
     * {@code KafkaPaymentStatusPublisher#publishPending} emits) maps straight across to
     * {@link PaymentStatus#PENDING}: unlike {@code "DECLINED"}, this table already has a
     * same-named state for it, so no vocabulary translation is needed - only the downstream
     * NO-DOWNGRADE guard ({@code domain.port.PaymentRepository#applyPendingStatus}) is new.
     *
     * <p>A declined card is not an error, it is a business outcome (ADR-0006 category B) - the
     * event says so plainly with {@code "DECLINED"} - but this table has no {@code DECLINED}
     * state of its own, only {@link PaymentStatus#FAILED}: from this row's point of view, "the
     * provider said no" and "we could not reach the provider at all" are the same terminal fact
     * as far as this column is concerned - the payment did not succeed. {@code FAILED} is this
     * table's word for that fact, not a claim about which of the two actually happened; the
     * decline reason itself, if a caller needs it, lives in psp-connector's own record of the
     * attempt, not here.
     *
     * <p>{@code "IPN_RECEIVED"}/{@code "VERIFIED"} (M21's stage 3/4 trail events) return {@code
     * null} on purpose: they have no {@link PaymentStatus} equivalent and must never touch
     * {@code payments.status} - {@link ApplyPaymentOutcomeCommand}'s javadoc explains why a
     * {@code null} domainStatus is exactly that instruction, not a mapping gap.
     *
     * <p>{@code "EXPIRED"} (M22 - published only by THIS service's own
     * {@code adapters.in.scheduler.PaymentExpirationScheduler}, consumed back through this exact
     * listener like any other producer's event) maps straight across to
     * {@link PaymentStatus#EXPIRED}: unlike DECLINED, this table now has a same-named terminal
     * state for it. {@code ApplyPaymentOutcomeUseCase#execute} is what makes applying it
     * conditional (CREATED/PENDING only) rather than absolute - this mapper's job stops at "which
     * enum value", the same division of labour PENDING already established.
     *
     * <p>Any other status is a contract violation this mapper cannot classify (the schema's own
     * doc says the event is "never emitted for a TIMEOUT outcome" - ADR-0006 category A is not a
     * business outcome) - it throws rather than silently defaulting, so it fails loudly at the
     * listener (ADR-0006 category D: unknown = non-retryable, logged and skipped, never silently
     * mis-recorded as a status that never happened).
     */
    private PaymentStatus toDomainStatus(String eventStatus) {
        return switch (eventStatus) {
            case "PENDING" -> PaymentStatus.PENDING;
            case "SUCCEEDED" -> PaymentStatus.SUCCEEDED;
            case "DECLINED" -> PaymentStatus.FAILED;
            case "EXPIRED" -> PaymentStatus.EXPIRED;
            case "IPN_RECEIVED", "VERIFIED" -> null;
            default ->
                    throw new IllegalArgumentException(
                            "unknown payments.payment-status-changed.v1 status: " + eventStatus);
        };
    }

    /** PENDING's providerReference is always {@code ""} on the wire (no provider call yet). */
    private static String blankToNull(String value) {
        return (value == null || value.isBlank()) ? null : value;
    }
}
