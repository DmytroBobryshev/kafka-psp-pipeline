package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import java.util.UUID;

public interface PaymentStatusPublisher {

    void publishStatusChanged(PaymentAttempt attempt);

    void publishPending(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID causationEventId,
            String traceId,
            String correlationId);

    void publishIpnReceived(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId);

    void publishVerified(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId);
}
