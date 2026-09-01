package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresPaymentRepository implements PaymentRepository {

    // M22: the two FROM states applyExpiredStatus's guard accepts - see the port's javadoc.
    private static final List<PaymentStatus> EXPIRABLE_FROM_STATUSES =
            List.of(PaymentStatus.CREATED, PaymentStatus.PENDING);

    private final PaymentJpaRepository jpaRepository;
    private final PaymentPersistenceMapper mapper;

    public PostgresPaymentRepository(PaymentJpaRepository jpaRepository, PaymentPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public Payment save(Payment payment) {
        PaymentEntity saved = jpaRepository.save(mapper.toEntity(payment));
        return mapper.toDomain(saved);
    }

    @Override
    public Optional<Payment> findById(UUID id) {
        return jpaRepository.findById(id).map(mapper::toDomain);
    }

    @Override
    public void updateStatus(UUID paymentId, PaymentStatus status) {
        jpaRepository.updateStatus(paymentId, status, java.time.Instant.now());
    }

    @Override
    public void applyPendingStatus(UUID paymentId) {
        jpaRepository.updateStatusIfCurrentStatus(
                paymentId, PaymentStatus.PENDING, PaymentStatus.CREATED, java.time.Instant.now());
    }

    @Override
    public void applyExpiredStatus(UUID paymentId) {
        jpaRepository.updateStatusIfCurrentStatusIn(
                paymentId, PaymentStatus.EXPIRED, EXPIRABLE_FROM_STATUSES, java.time.Instant.now());
    }

    @Override
    public List<Payment> findExpirationCandidates(Instant now) {
        return jpaRepository.findExpirationCandidates(now).stream().map(mapper::toDomain).toList();
    }

    @Override
    public PaymentPage search(String merchantId, PaymentStatus status, int page, int size) {
        Page<PaymentEntity> result =
                jpaRepository.search(merchantId, status, PageRequest.of(page, size));
        List<Payment> items = result.getContent().stream().map(mapper::toDomain).toList();
        return new PaymentPage(items, page, size, result.getTotalElements());
    }
}
