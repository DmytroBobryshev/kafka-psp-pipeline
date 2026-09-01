package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.CreatePaymentCommand;
import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Payment;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentWebMapper {

    @Mapping(target = "amount", expression = "java(new Money(request.amount(), request.currency()))")
    CreatePaymentCommand toCommand(CreatePaymentRequest request);

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "status", expression = "java(payment.getStatus().name())")
    PaymentResponse toResponse(Payment payment);
}
