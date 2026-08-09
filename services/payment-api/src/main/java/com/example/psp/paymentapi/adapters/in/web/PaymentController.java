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

/**
 * The web adapter for the payment hexagon: DTO -&gt; MapStruct ({@link PaymentWebMapper}) -&gt;
 * use case -&gt; domain -&gt; MapStruct -&gt; DTO (ADR-0007). M3 wires the use case to a real
 * Postgres repository and a real Kafka producer (see {@code adapters/out/persistence} and
 * {@code adapters/out/kafka}) - this controller itself stays thin; {@code @Valid} is the only
 * addition over the M1 skeleton.
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
    public ResponseEntity<PaymentResponse> create(@Valid @RequestBody CreatePaymentRequest request) {
        Payment payment = createPaymentUseCase.execute(mapper.toCommand(request));
        return ResponseEntity.status(HttpStatus.CREATED).body(mapper.toResponse(payment));
    }
}
