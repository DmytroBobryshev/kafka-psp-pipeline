package com.example.psp.paymentapi.domain.exception;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import java.util.List;

/**
 * Thrown by {@code CreatePaymentUseCase} when the merchant is absent from the local projection or
 * is not {@code ACTIVE}. A business outcome, not a bug (ADR-0006 category B) - never retried;
 * extends {@link IllegalArgumentException} so common-web's existing handler maps it to
 * {@code 400 Bad Request} with no new handler code needed.
 */
public class MerchantNotEligibleException extends IllegalArgumentException {

    private MerchantNotEligibleException(String message) {
        super(message);
    }

    /**
     * {@code allowed}'s own {@code toString()} renders as {@code "[EUR, USD]"} - exactly the
     * message form this factory wants - so it is interpolated as a {@link List}, not pre-joined.
     */
    public static MerchantNotEligibleException currencyNotAllowed(
            String merchantId, List<String> allowed, String requested) {
        return new MerchantNotEligibleException(
                "merchant " + merchantId + " accepts only " + allowed + " (got " + requested + ")");
    }

    public static MerchantNotEligibleException unknown(String merchantId) {
        return new MerchantNotEligibleException(
                "unknown merchant " + merchantId + " - create its config first");
    }

    public static MerchantNotEligibleException notActive(String merchantId, MerchantStatus status) {
        return new MerchantNotEligibleException(
                "merchant " + merchantId + " is not active (status=" + status + ")");
    }
}
