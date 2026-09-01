package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the status-history persistence hexagon boundary (M20, ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 *
 * <p>{@code toDomain} is a hand-written default method rather than a generated one, same reason
 * as {@code PaymentPersistenceMapper#toDomain}: {@link PaymentStatusHistoryEntry} has no public
 * constructor or setters (only {@code record}/{@code reconstitute} factory methods), so MapStruct
 * has no bean-convention way to build one automatically. {@code toEntity} IS generated: every
 * field name on {@link PaymentStatusHistoryEntry} matches a {@link PaymentStatusHistoryEntity}
 * getter/setter pair one-for-one, so no {@code @Mapping} overrides are needed.
 */
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
