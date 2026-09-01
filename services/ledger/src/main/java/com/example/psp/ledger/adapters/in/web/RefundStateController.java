package com.example.psp.ledger.adapters.in.web;

import com.example.psp.ledger.application.GetRefundSagaStateUseCase;
import com.example.psp.ledger.domain.model.RefundSagaState;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/refunds")
public class RefundStateController {

    private final GetRefundSagaStateUseCase useCase;
    private final RefundStateWebMapper mapper;

    public RefundStateController(GetRefundSagaStateUseCase useCase, RefundStateWebMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @GetMapping("/{refundId}")
    public ResponseEntity<RefundStateResponse> get(@PathVariable("refundId") UUID refundId) {
        RefundSagaState state = useCase.execute(refundId);
        return ResponseEntity.ok(mapper.toResponse(state));
    }
}
