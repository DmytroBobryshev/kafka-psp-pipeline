package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantPage;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.model.MerchantView;
import com.example.psp.paymentapi.domain.port.MerchantViewRepository;
import java.util.NoSuchElementException;
import org.springframework.stereotype.Service;

@Service
public class MerchantQueryUseCase {

    private final MerchantViewRepository merchantViewRepository;

    public MerchantQueryUseCase(MerchantViewRepository merchantViewRepository) {
        this.merchantViewRepository = merchantViewRepository;
    }

    public MerchantPage search(MerchantStatus status, int page, int size) {
        return merchantViewRepository.search(status, page, size);
    }

    public MerchantView getById(String merchantId) {
        return merchantViewRepository
                .findById(merchantId)
                .orElseThrow(() -> new NoSuchElementException("No merchant with id=" + merchantId));
    }
}
