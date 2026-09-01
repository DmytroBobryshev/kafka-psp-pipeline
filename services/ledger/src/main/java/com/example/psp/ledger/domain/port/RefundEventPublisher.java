package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.MerchantBalance;
import com.example.psp.ledger.domain.model.RefundRequest;
import com.example.psp.ledger.domain.model.RefundSagaState;
import java.util.UUID;

public interface RefundEventPublisher {

    void publishFundsReserved(
            RefundRequest request,
            UUID causationEventId,
            String traceId,
            String correlationId,
            MerchantBalance balanceAfter);

    void publishRefundFailedInsufficientBalance(
            RefundRequest request, UUID causationEventId, String traceId, String correlationId);

    void publishReservationReleased(
            RefundSagaState state, String reason, UUID causationEventId, String traceId, String correlationId);
}
