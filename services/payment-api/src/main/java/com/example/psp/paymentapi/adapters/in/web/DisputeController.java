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
