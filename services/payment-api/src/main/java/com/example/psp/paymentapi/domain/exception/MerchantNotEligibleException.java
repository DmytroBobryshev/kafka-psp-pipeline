package com.example.psp.paymentapi.domain.exception;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import java.util.List;

public class MerchantNotEligibleException extends IllegalArgumentException {

    private MerchantNotEligibleException(String message) {
        super(message);
    }

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
