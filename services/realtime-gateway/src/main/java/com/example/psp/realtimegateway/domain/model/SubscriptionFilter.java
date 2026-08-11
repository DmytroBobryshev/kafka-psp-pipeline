package com.example.psp.realtimegateway.domain.model;

/**
 * What one browser connection asked to watch (module brief: "filtered by paymentId and/or
 * merchantId so a browser can watch one payment's timeline"). Either field may be {@code null};
 * a non-null field must match exactly, so specifying both narrows to their intersection (a single
 * payment belonging to a single merchant - always true, but occasionally useful as an explicit
 * assertion), specifying one watches everything for that payment or that merchant, and specifying
 * neither would watch the entire event firehose - {@code adapters.in.web.PaymentTimelineController}
 * rejects that combination at the REST boundary rather than silently allowing it here.
 */
public record SubscriptionFilter(String paymentId, String merchantId) {

    public boolean matches(RealtimeEvent event) {
        boolean paymentMatches = paymentId == null || paymentId.equals(event.paymentId());
        boolean merchantMatches = merchantId == null || merchantId.equals(event.merchantId());
        return paymentMatches && merchantMatches;
    }
}
