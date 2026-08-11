package com.example.psp.ledger.adapters.in.web;

import com.example.psp.ledger.application.GetRefundSagaStateUseCase;
import com.example.psp.ledger.domain.model.RefundSagaState;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * M11 step 5: "track each refund's state so it is inspectable ... with a REST endpoint to read
 * it." A read, not a command, so ADR-0004's "commands enter via REST" is not what this endpoint
 * is for - it exists because {@code refund_saga_state} is otherwise invisible outside this
 * service's own database (ADR-0005), and this is what makes the saga's eventual consistency
 * something a caller (and, eventually, the M17 refund tracker) can actually observe rather than
 * infer.
 *
 * <p>404 (via {@link java.util.NoSuchElementException} + common-web's
 * {@code GlobalExceptionHandler}) for a {@code refundId} this ledger has not consumed
 * {@code refunds.refund-requested.v1} for yet - the caller already has {@code REQUESTED} from
 * payment-api's synchronous {@code 202 Accepted} response to the original {@code POST}.
 */
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
