package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository {

    Payment save(Payment payment);

    Optional<Payment> findById(UUID id);

    void updateStatus(UUID paymentId, PaymentStatus status);

    void applyPendingStatus(UUID paymentId);

    void applyExpiredStatus(UUID paymentId);

    List<Payment> findExpirationCandidates(Instant now);

    PaymentPage search(String merchantId, PaymentStatus status, int page, int size);
}
