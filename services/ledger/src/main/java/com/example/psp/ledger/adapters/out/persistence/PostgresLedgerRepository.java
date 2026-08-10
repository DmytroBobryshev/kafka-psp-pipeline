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

/**
 * Real Postgres adapter for {@link LedgerRepository}. Talks to the {@code ledger} database
 * (infra/compose, ADR-0005) via Spring Data JPA; {@link LedgerPersistenceMapper} keeps the JPA
 * entities out of {@code domain/} and {@code application/} entirely (ADR-0007).
 *
 * <p>Deliberately <b>not</b> {@code @Transactional} itself: the transaction lives one level down in
 * {@link LedgerWriteTransaction}, precisely so the {@code catch} below sits outside it. See that
 * class's javadoc for why that split is load-bearing rather than stylistic.
 */
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
            // uq_ledger_entries_inbound_event_id rejected the insert: a concurrent delivery of this
            // same inbound event won the check-then-act race that
            // application.RecordLedgerEntryUseCase's check-first path cannot close on its own. The
            // database is the authority, and losing to it is a normal outcome of at-least-once
            // delivery under concurrency - reported by return value, never rethrown
            // (LedgerRepository#tryApply's contract).
            //
            // Rethrowing here would abort the Kafka transaction, which would redeliver the record,
            // which would hit exactly the same constraint again: an infinite loop built out of a
            // situation that is not even an error.
            log.debug(
                    "Unique constraint rejected duplicate ledger entry inboundEventId={} merchantId={}",
                    entry.getInboundEventId(),
                    entry.getMerchantId(),
                    e);
            return Optional.empty();
        }
    }
}
