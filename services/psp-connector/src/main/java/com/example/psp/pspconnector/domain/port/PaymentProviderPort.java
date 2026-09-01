package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import java.util.UUID;

public interface PaymentProviderPort {

    ProviderResult authorize(UUID paymentId, String merchantId, Money amount);
}
