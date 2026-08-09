package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Payment;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the persistence hexagon boundary: {@link PaymentEntity} &lt;-&gt;
 * {@link Payment} (ADR-0007). {@code componentModel = "spring"}, {@code unmappedTargetPolicy =
 * ERROR} - same rule as every other boundary mapper in this service (see
 * {@code adapters.in.web.PaymentWebMapper}, {@code adapters.out.kafka.PaymentEventMapper}).
 *
 * <p>{@code toDomain} is a hand-written default method rather than a generated one: {@link
 * Payment} has no public constructor or setters (it only exposes {@code create}/{@code
 * reconstitute} factory methods - see its Javadoc), so MapStruct has no bean-convention way to
 * build one automatically. {@code toEntity} IS generated: {@link PaymentEntity} exposes a no-arg
 * constructor + setters (Lombok {@code @Setter}), so MapStruct's default strategy applies, same
 * as {@code PaymentWebMapper.toResponse} does for the {@code amount.amount}/{@code
 * amount.currency} nested-property split.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentPersistenceMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    PaymentEntity toEntity(Payment payment);

    default Payment toDomain(PaymentEntity entity) {
        if (entity == null) {
            return null;
        }
        return Payment.reconstitute(
                entity.getId(),
                entity.getMerchantId(),
                Money.of(entity.getAmount(), entity.getCurrency()),
                entity.getStatus(),
                entity.getCreatedAt());
    }
}
