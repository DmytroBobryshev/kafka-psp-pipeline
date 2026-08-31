package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.paymentapi.application.ApplyPaymentOutcomeCommand;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the M19 status-view listener's inbound Kafka boundary: event -&gt; domain
 * command (ADR-0007). {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} -
 * same rule as every other boundary mapper in this codebase. {@code paymentId}'s UUID-shaped
 * field is plain Avro {@code string}, hence the explicit {@code UUID.fromString(...)} expression -
 * the same pattern webhook-notifier's and ledger's own {@code PaymentStatusChangedMapper}s already
 * use for this identical event.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusChangedMapper {

    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "status", source = "status", qualifiedByName = "toPaymentStatus")
    ApplyPaymentOutcomeCommand toCommand(PaymentStatusChanged event);

    /**
     * Translates the event's status vocabulary into this table's own. {@link PaymentStatus} (M1)
     * predates {@code payments.payment-status-changed.v1}'s (M9 Phase 2) event vocabulary by
     * several milestones and was never renamed to match it. A declined card is not an error, it
     * is a business outcome (ADR-0006 category B) - the event says so plainly with
     * {@code "DECLINED"} - but this table has no {@code DECLINED} state of its own, only
     * {@link PaymentStatus#FAILED}: from this row's point of view, "the provider said no" and "we
     * could not reach the provider at all" are the same terminal fact as far as this column is
     * concerned - the payment did not succeed. {@code FAILED} is this table's word for that fact,
     * not a claim about which of the two actually happened; the decline reason itself, if a caller
     * needs it, lives in psp-connector's own record of the attempt, not here.
     *
     * <p>Any status other than {@code "SUCCEEDED"}/{@code "DECLINED"} is a contract violation this
     * mapper cannot classify (the schema's own doc says the event is "never emitted for a TIMEOUT
     * outcome" - ADR-0006 category A is not a business outcome) - it throws rather than silently
     * defaulting, so it fails loudly at the listener (ADR-0006 category D: unknown = non-retryable,
     * logged and skipped, never silently mis-recorded as a status that never happened).
     */
    @Named("toPaymentStatus")
    default PaymentStatus toPaymentStatus(String eventStatus) {
        return switch (eventStatus) {
            case "SUCCEEDED" -> PaymentStatus.SUCCEEDED;
            case "DECLINED" -> PaymentStatus.FAILED;
            default ->
                    throw new IllegalArgumentException(
                            "unknown payments.payment-status-changed.v1 status: " + eventStatus);
        };
    }
}
