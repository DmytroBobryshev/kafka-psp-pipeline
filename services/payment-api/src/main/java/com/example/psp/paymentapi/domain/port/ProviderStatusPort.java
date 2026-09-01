package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.ProviderStatusResult;
import java.util.UUID;

public interface ProviderStatusPort {

    ProviderStatusResult checkStatus(UUID paymentId, String merchantId);
}
