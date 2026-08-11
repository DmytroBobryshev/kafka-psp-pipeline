package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Refund;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the refund persistence hexagon boundary (M11, ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundPersistenceMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    RefundEntity toEntity(Refund refund);

    default Refund toDomain(RefundEntity entity) {
        return Refund.reconstitute(
                entity.getId(),
                entity.getPaymentId(),
                entity.getMerchantId(),
                Money.of(entity.getAmount(), entity.getCurrency()),
                entity.getReason(),
                entity.getCreatedAt());
    }
}
