package com.example.psp.paymentapi.adapters.in.web;

import java.util.List;

public record PaymentPageResponse(List<PaymentResponse> items, int page, int size, long total) {
}
