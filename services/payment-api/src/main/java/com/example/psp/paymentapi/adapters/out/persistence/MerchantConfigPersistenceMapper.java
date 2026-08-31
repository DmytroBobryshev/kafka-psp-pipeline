package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.MerchantView;
import java.util.Arrays;
import java.util.List;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the persistence boundary: {@link MerchantConfigEntity} -> {@link
 * MerchantView}. {@code allowedCurrencies} is CSV in the column (see V8) but a {@code List<String>}
 * in the domain - {@code ""} (legacy row) maps to an empty list, same "empty means fall back to
 * payoutCurrency" convention the Avro field and {@link MerchantView} itself carry.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MerchantConfigPersistenceMapper {

    @Mapping(target = "allowedCurrencies", source = "allowedCurrencies", qualifiedByName = "splitCsv")
    MerchantView toDomain(MerchantConfigEntity entity);

    @Named("splitCsv")
    static List<String> splitCsv(String csv) {
        return csv == null || csv.isBlank() ? List.of() : Arrays.asList(csv.split(","));
    }
}
