package com.example.psp.paymentapi.domain.model;

import java.util.List;

public record MerchantPage(List<MerchantView> items, int page, int size, long total) {
}
