package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.RefundStatusHistoryEntry;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the refund-status-history persistence hexagon boundary (M23, ADR-0007).
 * Mirrors {@link PaymentStatusHistoryPersistenceMapper}: {@code toDomain} is hand-written (no
 * bean-convention constructor on {@link RefundStatusHistoryEntry}), {@code toEntity} is generated.
 */
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
