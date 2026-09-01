package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Payment;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentEventMapper {

    @Mapping(target = "envelope", source = "envelope")
    @Mapping(target = "paymentId", source = "payment.id")
    @Mapping(target = "merchantId", source = "payment.merchantId")
    @Mapping(target = "amount", source = "payment.amount.amount")
    @Mapping(target = "currency", source = "payment.amount.currency")
    @Mapping(target = "status", expression = "java(payment.getStatus().name())")
    PaymentRequested toEvent(EventEnvelope envelope, Payment payment);
}
