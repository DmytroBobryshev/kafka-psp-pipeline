package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.MerchantConfig;

public interface MerchantConfigPublisher {

    void publishConfigChanged(MerchantConfig config);

    void publishConfigDeleted(String merchantId);
}
