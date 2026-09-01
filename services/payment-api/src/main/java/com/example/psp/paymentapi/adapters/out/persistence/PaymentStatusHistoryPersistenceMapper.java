package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusHistoryPersistenceMapper {

    PaymentStatusHistoryEntity toEntity(PaymentStatusHistoryEntry entry);

    default PaymentStatusHistoryEntry toDomain(PaymentStatusHistoryEntity entity) {
        if (entity == null) {
            return null;
        }
        return PaymentStatusHistoryEntry.reconstitute(
                entity.getId(),
                entity.getPaymentId(),
                entity.getStatus(),
                entity.getProviderReference(),
                entity.getEventId(),
                entity.getOccurredAt(),
                entity.getRecordedAt());
    }
}
