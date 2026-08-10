package com.example.psp.ledger.adapters.out.persistence;

import com.example.psp.ledger.domain.model.EntryDirection;
import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import com.example.psp.ledger.domain.model.Money;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the persistence hexagon boundary (ADR-0007). {@code componentModel =
 * "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every other boundary mapper here.
 *
 * <p>Both directions are needed in this service, unlike {@code psp-connector}'s write-only
 * equivalent: {@link #toEntity} for the ledger entry insert, and {@link #toDomain} to turn the
 * balance row that the atomic upsert just produced back into the {@link MerchantBalance} the
 * outbound event carries.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface LedgerPersistenceMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "direction", expression = "java(entry.getDirection().name())")
    LedgerEntryEntity toEntity(LedgerEntry entry);

    default MerchantBalance toDomain(MerchantBalanceEntity entity) {
        return new MerchantBalance(
                entity.getMerchantId(),
                Money.of(entity.getBalance(), entity.getCurrency()),
                entity.getEntryCount(),
                entity.getUpdatedAt());
    }

    default LedgerEntry toDomain(LedgerEntryEntity entity) {
        return LedgerEntry.reconstitute(
                entity.getId(),
                entity.getInboundEventId(),
                entity.getMerchantId(),
                entity.getPaymentId(),
                EntryDirection.valueOf(entity.getDirection()),
                Money.of(entity.getAmount(), entity.getCurrency()),
                entity.getTraceId(),
                entity.getCorrelationId(),
                entity.getRecordedAt());
    }
}
