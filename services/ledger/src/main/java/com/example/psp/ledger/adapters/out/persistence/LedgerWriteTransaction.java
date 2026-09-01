package com.example.psp.ledger.adapters.out.persistence;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import java.time.Instant;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class LedgerWriteTransaction {

    private final LedgerEntryJpaRepository entryRepository;
    private final MerchantBalanceJpaRepository balanceRepository;
    private final LedgerPersistenceMapper mapper;

    public LedgerWriteTransaction(
            LedgerEntryJpaRepository entryRepository,
            MerchantBalanceJpaRepository balanceRepository,
            LedgerPersistenceMapper mapper) {
        this.entryRepository = entryRepository;
        this.balanceRepository = balanceRepository;
        this.mapper = mapper;
    }

    @Transactional("transactionManager")
    public MerchantBalance applyAtomically(LedgerEntry entry) {
        entryRepository.saveAndFlush(mapper.toEntity(entry));

        balanceRepository.applyDelta(
                entry.getMerchantId(),
                entry.getAmount().currency(),
                entry.signedAmount(),
                Instant.now());

        return balanceRepository
                .findById(entry.getMerchantId())
                .map(mapper::toDomain)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "merchant_balances row vanished immediately after upsert for "
                                                + "merchantId="
                                                + entry.getMerchantId()));
    }
}
