package com.example.psp.ledger.adapters.out.persistence;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import java.time.Instant;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The <b>Postgres</b> transaction: insert the ledger entry and move the merchant's balance, both or
 * neither.
 *
 * <h2>Why this is a separate bean from {@link PostgresLedgerRepository}</h2>
 *
 * <p>Because a caught {@code DataIntegrityViolationException} must be caught <em>outside</em> the
 * transaction it aborted. Catching it inside the same {@code @Transactional} method and returning
 * normally would leave the transaction marked rollback-only and blow up at commit with
 * {@code UnexpectedRollbackException} - the classic "I handled the duplicate but the commit still
 * failed" bug. Splitting the two means the proxy boundary sits between the failing statement and
 * the {@code catch}: by the time {@link PostgresLedgerRepository#tryApply} sees the exception, this
 * transaction has already been cleanly rolled back and nothing is left half-marked.
 *
 * <h2>Why {@code @Transactional("transactionManager")} and not the Kafka one</h2>
 *
 * <p>This service has two transaction managers and they cover different systems
 * ({@code config.KafkaProducerConfig}). This method names the JPA one explicitly. At the moment it
 * runs, a <b>Kafka</b> transaction is already open on this thread (begun by the listener
 * container), and these two do not compose into one distributed transaction - they are simply two
 * independent transactions with overlapping lifetimes. Spring will happily begin this JPA
 * transaction inside the Kafka one, commit it here, and later commit or abort the Kafka one on its
 * own; nothing coordinates the two outcomes, and nothing in this codebase pretends otherwise. The
 * asymmetry that creates - a committed balance under an aborted Kafka transaction - is handled by
 * idempotency, not by transactionality. See README's "Where Kafka EOS ends".
 */
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

    /**
     * @return the merchant's balance after applying {@code entry}.
     * @throws org.springframework.dao.DataIntegrityViolationException if
     *     {@code uq_ledger_entries_inbound_event_id} rejected the insert (a concurrent delivery of
     *     the same inbound event got there first). Propagated on purpose so the caller - outside
     *     this transaction - can translate it into the "already applied" outcome it actually is.
     */
    @Transactional("transactionManager")
    public MerchantBalance applyAtomically(LedgerEntry entry) {
        // saveAndFlush, not save: the constraint violation must surface HERE, synchronously, rather
        // than at an arbitrary later flush point where it would be much harder to attribute.
        entryRepository.saveAndFlush(mapper.toEntity(entry));

        // Same transaction: an entry row without its balance delta would be a permanently wrong
        // balance that no replay could repair, because the replay would be deduplicated by that
        // very row.
        balanceRepository.applyDelta(
                entry.getMerchantId(),
                entry.getAmount().currency(),
                entry.signedAmount(),
                Instant.now());

        // Read back inside the same transaction, after the @Modifying query's automatic clear, so
        // this is the post-update row and not a stale first-level-cache copy.
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
