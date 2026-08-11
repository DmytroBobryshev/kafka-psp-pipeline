package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.RefundProviderResult;
import java.util.UUID;

/**
 * Outbound port to the payment provider/acquirer's refund operation (M11) - the refund-path
 * counterpart of {@link PaymentProviderPort}. Implemented by
 * {@code adapters.out.http.SimulatedPaymentProviderAdapter}, the same simulated acquirer the
 * payment path uses (ADR-0004's explicit carve-out for outbound HTTP that leaves the system).
 */
public interface RefundProviderPort {

    RefundProviderResult refund(UUID refundId, UUID paymentId, String merchantId, Money amount);
}
