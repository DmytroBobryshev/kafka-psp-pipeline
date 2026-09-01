package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.RefundAttempt;
import java.util.UUID;

public interface RefundStatusPublisher {

    void publishOutcome(RefundAttempt attempt);

    void publishPending(
            UUID refundId,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID causationEventId,
            String traceId,
            String correlationId);

    void publishIpnReceived(
            UUID refundId,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId);

    void publishVerified(
            UUID refundId,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId);
}
