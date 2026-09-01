package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.RefundStatusHistoryEntry;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundStatusHistoryPersistenceMapper {

    RefundStatusHistoryEntity toEntity(RefundStatusHistoryEntry entry);

    default RefundStatusHistoryEntry toDomain(RefundStatusHistoryEntity entity) {
        if (entity == null) {
            return null;
        }
        return RefundStatusHistoryEntry.reconstitute(
                entity.getId(),
                entity.getRefundId(),
                entity.getPaymentId(),
                entity.getStatus(),
                entity.getProviderReference(),
                entity.getEventId(),
                entity.getOccurredAt(),
                entity.getRecordedAt());
    }
}
