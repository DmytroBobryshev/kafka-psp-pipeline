package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.paymentapi.application.ApplyPaymentOutcomeCommand;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.util.UUID;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

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

    private static String blankToNull(String value) {
        return (value == null || value.isBlank()) ? null : value;
    }
}
