package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.RequestRefundUseCase;
import com.example.psp.paymentapi.domain.model.Refund;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * M11's entry point (ADR-0004: commands enter via REST; everything else is events). The web
 * adapter for the refund hexagon: DTO -&gt; MapStruct ({@link RefundWebMapper}) -&gt; use case
 * -&gt; domain -&gt; MapStruct -&gt; DTO, same shape as {@link PaymentController}.
 *
 * <p>{@code 202 Accepted}, not {@code 201 Created}: the refund row and its outbox event are
 * durably committed when this method returns, but the thing the caller actually asked for -
 * money being reserved and eventually refunded - has not happened yet and may not (insufficient
 * balance, a provider decline). 202 states that honestly, matching
 * docs/diagrams/sequence-refund-saga.md's "202 Accepted {refundId, status: REQUESTED}".
 */
@RestController
@RequestMapping("/api/payments/{paymentId}/refunds")
public class RefundController {

    private final RequestRefundUseCase requestRefundUseCase;
    private final RefundWebMapper mapper;

    public RefundController(RequestRefundUseCase requestRefundUseCase, RefundWebMapper mapper) {
        this.requestRefundUseCase = requestRefundUseCase;
        this.mapper = mapper;
    }

    @PostMapping
    public ResponseEntity<RefundResponse> create(
            @PathVariable("paymentId") UUID paymentId, @Valid @RequestBody RequestRefundRequest request) {
        Refund refund = requestRefundUseCase.execute(mapper.toCommand(paymentId, request));
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(mapper.toResponse(refund));
    }
}
