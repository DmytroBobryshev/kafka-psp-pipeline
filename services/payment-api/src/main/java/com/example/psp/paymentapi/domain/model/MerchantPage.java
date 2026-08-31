package com.example.psp.paymentapi.domain.model;

import java.util.List;

/**
 * One page of a {@link MerchantView} search. Same shape and purpose as {@link PaymentPage}, kept
 * as a separate type because the two aggregates are unrelated and a port must not leak one
 * aggregate's page type into another's contract.
 */
public record MerchantPage(List<MerchantView> items, int page, int size, long total) {
}
