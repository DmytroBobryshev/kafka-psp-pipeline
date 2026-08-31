package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.MerchantPage;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.model.MerchantView;
import com.example.psp.paymentapi.domain.port.MerchantViewRepository;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

/** Postgres adapter for {@link MerchantViewRepository}, same shape as {@link PostgresPaymentRepository}. */
@Repository
public class PostgresMerchantViewRepository implements MerchantViewRepository {

    private final MerchantConfigJpaRepository jpaRepository;
    private final MerchantConfigPersistenceMapper mapper;

    public PostgresMerchantViewRepository(
            MerchantConfigJpaRepository jpaRepository, MerchantConfigPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public void upsert(MerchantView view) {
        jpaRepository.upsert(
                view.merchantId(),
                view.displayName(),
                view.status().name(),
                view.payoutCurrency(),
                view.webhookUrl(),
                view.declineRateAlertThresholdBps(),
                view.updatedAt());
    }

    @Override
    public void delete(String merchantId) {
        jpaRepository.deleteByMerchantId(merchantId);
    }

    @Override
    public Optional<MerchantView> findById(String merchantId) {
        return jpaRepository.findById(merchantId).map(mapper::toDomain);
    }

    @Override
    public MerchantPage search(MerchantStatus status, int page, int size) {
        Page<MerchantConfigEntity> result = jpaRepository.search(status, PageRequest.of(page, size));
        List<MerchantView> items = result.getContent().stream().map(mapper::toDomain).toList();
        return new MerchantPage(items, page, size, result.getTotalElements());
    }
}
