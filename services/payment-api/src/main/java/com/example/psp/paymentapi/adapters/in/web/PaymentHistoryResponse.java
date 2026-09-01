package com.example.psp.paymentapi.adapters.in.web;

import java.util.List;

public record PaymentHistoryResponse(List<PaymentHistoryItemResponse> items) {
}
