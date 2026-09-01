package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.AuthorizationLatency;
import java.time.Instant;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface AuthorizationLatencyMapper {

    @Mapping(target = "id", source = "latency.paymentId")
    @Mapping(target = "merchantId", source = "latency.merchantId")
    @Mapping(target = "providerReference", source = "latency.providerReference")
    @Mapping(target = "status", source = "latency.status")
    @Mapping(target = "declined", source = "latency.declined")
    @Mapping(target = "requestedAt", source = "latency.requestedAt")
    @Mapping(target = "decidedAt", source = "latency.decidedAt")
    @Mapping(target = "latencyMillis", source = "latency.latencyMillis")
    @Mapping(target = "projectedAt", source = "projectedAt")
    AuthorizationLatencyDocument toDocument(AuthorizationLatency latency, Instant projectedAt);
}
