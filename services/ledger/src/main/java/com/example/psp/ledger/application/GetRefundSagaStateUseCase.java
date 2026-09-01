package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.RefundSagaState;
import com.example.psp.ledger.domain.port.RefundRepository;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.stereotype.Service;

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
