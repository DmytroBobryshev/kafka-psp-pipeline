package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.CreatePaymentUseCase;
import com.example.psp.paymentapi.domain.model.Payment;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final CreatePaymentUseCase createPaymentUseCase;
    private final PaymentWebMapper mapper;

    public PaymentController(CreatePaymentUseCase createPaymentUseCase, PaymentWebMapper mapper) {
        this.createPaymentUseCase = createPaymentUseCase;
        this.mapper = mapper;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> create(@Valid @RequestBody CreatePaymentRequest request) {
        Payment payment = createPaymentUseCase.execute(mapper.toCommand(request));
        return ResponseEntity.status(HttpStatus.CREATED).body(mapper.toResponse(payment));
    }
}
