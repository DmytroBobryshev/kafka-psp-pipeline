package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.CreatePaymentCommand;
import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Payment;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the web hexagon boundary: request/response DTO &lt;-&gt; domain
 * (ADR-0007). {@code componentModel = "spring"} so it's injected like any bean;
 * {@code unmappedTargetPolicy = ERROR} so a forgotten field fails the build, not a code review.
 *
 * <p>Domain types ({@link Payment}, {@link Money}) never leak past this boundary as a wire
 * contract - {@link CreatePaymentRequest} and {@link PaymentResponse} are the only shapes a
 * client ever sees.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentWebMapper {

    @Mapping(target = "amount", expression = "java(new Money(request.amount(), request.currency()))")
    CreatePaymentCommand toCommand(CreatePaymentRequest request);

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "status", expression = "java(payment.getStatus().name())")
    PaymentResponse toResponse(Payment payment);
}
