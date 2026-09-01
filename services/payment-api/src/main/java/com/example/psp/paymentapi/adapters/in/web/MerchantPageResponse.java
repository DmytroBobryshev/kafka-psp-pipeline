package com.example.psp.paymentapi.adapters.in.web;

import java.util.List;

public record MerchantPageResponse(List<MerchantResponse> items, int page, int size, long total) {
}
