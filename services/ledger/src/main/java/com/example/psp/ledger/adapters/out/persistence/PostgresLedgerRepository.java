package com.example.psp.ledger.adapters.out.persistence;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import com.example.psp.ledger.domain.port.LedgerRepository;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresLedgerRepository implements LedgerRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresLedgerRepository.class);

    private final LedgerEntryJpaRepository entryRepository;
    private final LedgerWriteTransaction writeTransaction;

    public PostgresLedgerRepository(
            LedgerEntryJpaRepository entryRepository, LedgerWriteTransaction writeTransaction) {
        this.entryRepository = entryRepository;
        this.writeTransaction = writeTransaction;
    }

    @Override
    public boolean existsByInboundEventId(UUID inboundEventId) {
        return entryRepository.existsByInboundEventId(inboundEventId);
    }

    @Override
    public Optional<MerchantBalance> tryApply(LedgerEntry entry) {
        try {
            return Optional.of(writeTransaction.applyAtomically(entry));
        } catch (DataIntegrityViolationException e) {
            log.debug(
                    "Unique constraint rejected duplicate ledger entry inboundEventId={} merchantId={}",
                    entry.getInboundEventId(),
                    entry.getMerchantId(),
                    e);
            return Optional.empty();
        }
    }
}
