package com.example.psp.paymentapi.domain.model;

import java.util.List;

public record PaymentPage(List<Payment> items, int page, int size, long total) {
}
