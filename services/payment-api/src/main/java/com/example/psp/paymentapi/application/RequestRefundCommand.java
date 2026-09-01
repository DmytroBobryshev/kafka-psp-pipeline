package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Money;
import java.util.UUID;

public record RequestRefundCommand(UUID paymentId, Money amount, String reason) {
}
