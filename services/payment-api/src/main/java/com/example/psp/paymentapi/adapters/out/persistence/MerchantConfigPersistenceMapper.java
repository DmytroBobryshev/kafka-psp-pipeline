package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.MerchantView;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/** MapStruct mapper at the persistence boundary: {@link MerchantConfigEntity} -> {@link MerchantView}. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MerchantConfigPersistenceMapper {

    MerchantView toDomain(MerchantConfigEntity entity);
}
