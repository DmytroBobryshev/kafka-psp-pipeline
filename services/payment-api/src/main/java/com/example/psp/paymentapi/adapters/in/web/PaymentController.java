package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.CreatePaymentUseCase;
import com.example.psp.paymentapi.domain.model.Payment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The M1 "health-ish" web adapter: thin enough to prove the hexagon wiring end to end (DTO -&gt;
 * MapStruct -&gt; use case -&gt; domain -&gt; MapStruct -&gt; DTO) without any real persistence or
 * Kafka behind it - {@code adapters/out/persistence} and {@code adapters/out/kafka} are stubs at
 * this stage. Real payment creation semantics (validation, outbox, actual publish) land in M3.
 *
 * <p>Liveness/readiness for the service itself is Spring Boot Actuator's job
 * ({@code /actuator/health}), already wired via {@code spring-boot-starter-actuator}.
 */
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
    public ResponseEntity<PaymentResponse> create(@RequestBody CreatePaymentRequest request) {
        Payment payment = createPaymentUseCase.execute(mapper.toCommand(request));
        return ResponseEntity.status(HttpStatus.CREATED).body(mapper.toResponse(payment));
    }
}
