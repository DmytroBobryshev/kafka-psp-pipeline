package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Payment;

public interface PaymentEventPublisher {

    void publishPaymentCreated(Payment payment);
}
