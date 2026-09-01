package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.MerchantPage;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.model.MerchantView;
import java.util.Optional;

public interface MerchantViewRepository {

    void upsert(MerchantView view);

    void delete(String merchantId);

    Optional<MerchantView> findById(String merchantId);

    MerchantPage search(MerchantStatus status, int page, int size);
}
