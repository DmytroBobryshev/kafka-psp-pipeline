package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.RefundSagaState;
import com.example.psp.ledger.domain.port.RefundRepository;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Backs {@code GET /api/refunds/{refundId}} (M11 step 5: "track each refund's state so it is
 * inspectable"). This is the ledger's own local view (ADR-0008 rule 1) - it does not aggregate
 * payment-api's or psp-connector's local state, and it returns 404 for a refund whose
 * {@code refunds.refund-requested.v1} has not yet been consumed here (eventual consistency you can
 * see, not hide - the caller already knows the refund is {@code REQUESTED} from payment-api's
 * synchronous {@code 202 Accepted}).
 */
@Service
public class GetRefundSagaStateUseCase {

    private final RefundRepository refundRepository;

    public GetRefundSagaStateUseCase(RefundRepository refundRepository) {
        this.refundRepository = refundRepository;
    }

    public RefundSagaState execute(UUID refundId) {
        return refundRepository
                .findSagaState(refundId)
                .orElseThrow(
                        () ->
                                new NoSuchElementException(
                                        "No refund saga state for refundId=" + refundId + " in this ledger "
                                                + "yet - either it was never requested, or "
                                                + "refunds.refund-requested.v1 has not been consumed here "
                                                + "yet (the caller's original request already reported "
                                                + "status=REQUESTED)"));
    }
}
