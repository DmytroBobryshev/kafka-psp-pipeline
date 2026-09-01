package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.RefundProviderResult;
import java.util.UUID;

public interface RefundProviderPort {

    RefundProviderResult refund(UUID refundId, UUID paymentId, String merchantId, Money amount);
}
