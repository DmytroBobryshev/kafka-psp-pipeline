package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.DisputeProjection;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface DisputeProjectionMapper {

    @Mapping(target = "id", source = "disputeId")
    DisputeDocument toDocument(DisputeProjection projection);
}
