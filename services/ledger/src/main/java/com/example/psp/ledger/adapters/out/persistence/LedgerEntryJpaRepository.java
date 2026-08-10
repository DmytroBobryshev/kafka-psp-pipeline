package com.example.psp.ledger.adapters.out.persistence;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository behind {@link PostgresLedgerRepository}. Not a hexagon port itself:
 * the domain never sees this type, only
 * {@link com.example.psp.ledger.domain.port.LedgerRepository} (ADR-0007).
 */
public interface LedgerEntryJpaRepository extends JpaRepository<LedgerEntryEntity, UUID> {

    /**
     * M7's idempotency pre-check - Spring Data generates
     * {@code SELECT EXISTS(... WHERE inbound_event_id = ?)}, served by the unique index backing
     * {@code uq_ledger_entries_inbound_event_id} (V1 migration).
     */
    boolean existsByInboundEventId(UUID inboundEventId);
}
