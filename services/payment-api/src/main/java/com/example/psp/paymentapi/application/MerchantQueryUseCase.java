package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantPage;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.model.MerchantView;
import com.example.psp.paymentapi.domain.port.MerchantViewRepository;
import java.util.NoSuchElementException;
import org.springframework.stereotype.Service;

/** Read side of the local merchant-config projection: search and single-merchant lookup. */
@Service
public class MerchantQueryUseCase {

    private final MerchantViewRepository merchantViewRepository;

    public MerchantQueryUseCase(MerchantViewRepository merchantViewRepository) {
        this.merchantViewRepository = merchantViewRepository;
    }

    /**
     * @param status filter, or {@code null} to match every status.
     * @param page   zero-based page index - already clamped by the web adapter.
     * @param size   page size - already clamped by the web adapter.
     */
    public MerchantPage search(MerchantStatus status, int page, int size) {
        return merchantViewRepository.search(status, page, size);
    }

    /**
     * @throws NoSuchElementException if no merchant exists with this id - translated to
     *                                {@code 404} by common-web's {@code GlobalExceptionHandler},
     *                                same convention as {@code PaymentQueryUseCase#getById}.
     */
    public MerchantView getById(String merchantId) {
        return merchantViewRepository
                .findById(merchantId)
                .orElseThrow(() -> new NoSuchElementException("No merchant with id=" + merchantId));
    }
}
