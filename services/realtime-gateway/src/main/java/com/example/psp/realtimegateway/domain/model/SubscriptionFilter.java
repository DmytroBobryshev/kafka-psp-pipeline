package com.example.psp.realtimegateway.domain.model;

public record SubscriptionFilter(String paymentId, String merchantId) {

    public boolean matches(RealtimeEvent event) {
        boolean paymentMatches = paymentId == null || paymentId.equals(event.paymentId());
        boolean merchantMatches = merchantId == null || merchantId.equals(event.merchantId());
        return paymentMatches && merchantMatches;
    }
}
