package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.DisputeOutcome;
import com.example.psp.paymentapi.application.OpenDisputeUseCase;
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
 * M13's entry point (ADR-0004: commands enter via REST; everything else is events) for the
 * claim-check demo: DTO -&gt; {@link DisputeWebMapper} -&gt; use case -&gt; DTO, same shape as
 * {@link PaymentController}/{@link RefundController}.
 *
 * <p>{@code 202 Accepted}, matching {@link RefundController}'s reasoning: the event is durably
 * published (this adapter blocks on the send - see {@code adapters.out.kafka.
 * KafkaDisputeEventPublisher}) when this method returns, but "the dispute is opened" is a fact
 * about the downstream projection (analytics), not about this HTTP response - this service keeps
 * no dispute table of its own to answer synchronously from (see {@code domain.port.
 * DisputeEventPublisher}'s javadoc).
 */
@RestController
@RequestMapping("/api/payments/{paymentId}/disputes")
public class DisputeController {

    private final OpenDisputeUseCase openDisputeUseCase;
    private final DisputeWebMapper mapper;

    public DisputeController(OpenDisputeUseCase openDisputeUseCase, DisputeWebMapper mapper) {
        this.openDisputeUseCase = openDisputeUseCase;
        this.mapper = mapper;
    }

    @PostMapping
    public ResponseEntity<DisputeResponse> open(
            @PathVariable("paymentId") UUID paymentId, @Valid @RequestBody OpenDisputeRequest request) {
        DisputeOutcome outcome = openDisputeUseCase.execute(mapper.toCommand(paymentId, request));
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(mapper.toResponse(outcome));
    }
}
