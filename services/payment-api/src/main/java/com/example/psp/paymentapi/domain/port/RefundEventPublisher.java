package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Refund;

public interface RefundEventPublisher {

    void publishRefundRequested(Refund refund);
}
