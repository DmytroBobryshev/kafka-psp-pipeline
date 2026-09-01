package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Money;

public record CreatePaymentCommand(String merchantId, Money amount) {
}
