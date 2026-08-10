package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import java.util.Optional;
import java.util.UUID;

/**
 * Outbound port for the ledger's own Postgres database (ADR-0005: the {@code ledger} database is
 * owned exclusively by this service). Implemented by
 * {@code adapters.out.persistence.PostgresLedgerRepository}.
 *
 * <h2>This port is MECHANISM 2, and it is not the Kafka transaction</h2>
 *
 * <p>M7 has two independent correctness mechanisms and this port is the second one. Kafka
 * transactions make "commit the consumed offsets + publish the outbound entry" atomic, because
 * both of those live in Kafka. This database does not live in Kafka, so nothing about the Kafka
 * transaction protects it: the balance write can commit while the surrounding Kafka transaction
 * aborts, and the redelivery that follows is indistinguishable from a first delivery.
 *
 * <p>The answer is the M5 pattern, unchanged and for the same reason: dedup on the <b>inbound
 * envelope {@code eventId}</b>, which is stable across replays, rebalances, aborted transactions
 * and operator-driven offset resets, unlike anything generated during processing. Backed by
 * {@code uq_ledger_entries_inbound_event_id} ({@code db/migration/V1__create_ledger_tables.sql}).
 *
 * <p>Two paths, both mandatory, exactly as in {@code psp-connector}:
 *
 * <ul>
 *   <li><b>Check-first</b> ({@link #existsByInboundEventId}) - the common case and the cheap one:
 *       no wasted insert, no wasted balance update.
 *   <li><b>Constraint race</b> ({@link #tryApply} returning empty) - the check above is a
 *       check-then-act and is racy by construction. The unique constraint is the actual authority,
 *       and losing that race is a normal outcome of at-least-once delivery, never an error to
 *       propagate.
 * </ul>
 *
 * <p>All signatures use {@code domain/} types only - no Spring, JPA or Kafka type leaks through
 * this port (ADR-0007), even though the implementation catches a Spring Data exception internally
 * to satisfy {@link #tryApply}'s contract.
 */
public interface LedgerRepository {

    /**
     * Has this inbound {@code payments.payment-status-changed.v1} event already been applied to a
     * balance? Called by {@code application.RecordLedgerEntryUseCase} <b>before</b> any write.
     */
    boolean existsByInboundEventId(UUID inboundEventId);

    /**
     * Applies {@code entry} <b>atomically in one Postgres transaction</b>: inserts the ledger row
     * and adds the entry's signed amount to the merchant's balance. Either both happen or neither
     * does - an entry row without its balance delta would be a permanently wrong balance that no
     * replay could repair, because the replay would be deduplicated by that very row.
     *
     * @return the merchant's balance <em>after</em> this entry was applied, or
     *     {@link Optional#empty()} if the unique constraint on {@code inbound_event_id} rejected
     *     the insert - i.e. a concurrent delivery of the same inbound event won the race. Empty is
     *     a normal result and MUST NOT be reported by throwing.
     */
    Optional<MerchantBalance> tryApply(LedgerEntry entry);
}
