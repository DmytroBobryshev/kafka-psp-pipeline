package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import java.util.UUID;

/**
 * Outbound port to the payment provider/acquirer. Implemented by
 * {@code adapters.out.http.SimulatedPaymentProviderAdapter} - this is the ADR-0004 "explicit
 * carve-out" for outbound HTTP that leaves the system entirely (a real acquirer call, simulated
 * for now), which is why the adapter lives under {@code adapters.out.http} even though M4 never
 * opens a socket: a later milestone can replace the simulation with a real {@code WebClient} call
 * behind this exact port with zero change to {@code application/} or {@code domain/}.
 */
public interface PaymentProviderPort {

    ProviderResult authorize(UUID paymentId, String merchantId, Money amount);
}
