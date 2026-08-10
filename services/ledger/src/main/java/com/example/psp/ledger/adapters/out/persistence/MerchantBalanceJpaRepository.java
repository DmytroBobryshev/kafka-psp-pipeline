package com.example.psp.ledger.adapters.out.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data JPA repository for {@code merchant_balances}. Not a hexagon port (ADR-0007).
 */
public interface MerchantBalanceJpaRepository extends JpaRepository<MerchantBalanceEntity, String> {

    /**
     * Atomic upsert-and-add: creates the merchant's balance row on first entry, otherwise adds
     * {@code delta} to the existing balance and bumps {@code entry_count} - in <b>one</b> statement,
     * so there is no read-modify-write window and no lost update, regardless of how many writers
     * the partitioning happens to allow (see {@link MerchantBalanceEntity}'s javadoc).
     *
     * <p>Native rather than JPQL because {@code ON CONFLICT ... DO UPDATE} is Postgres syntax with
     * no JPQL equivalent; {@code EXCLUDED} refers to the row the {@code INSERT} would have written,
     * which is how the new delta and timestamp are reachable inside the {@code DO UPDATE} clause.
     *
     * <p>{@code flushAutomatically} pushes any pending entity state (the just-inserted
     * {@code ledger_entries} row, if it has not been flushed yet) to the database before this
     * statement runs; {@code clearAutomatically} evicts the persistence context afterwards so a
     * subsequent {@code findById} re-reads the row this statement changed behind Hibernate's back
     * rather than returning a stale cached copy. Both flags are the standard, easy-to-forget
     * requirement for a native {@code @Modifying} query that touches mapped tables.
     *
     * @param delta signed - {@code +amount} for a CREDIT, {@code -amount} for a DEBIT (M11).
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
            value =
                    """
                    INSERT INTO merchant_balances (merchant_id, currency, balance, entry_count, updated_at)
                    VALUES (:merchantId, :currency, :delta, 1, :updatedAt)
                    ON CONFLICT (merchant_id) DO UPDATE
                       SET balance     = merchant_balances.balance + EXCLUDED.balance,
                           entry_count = merchant_balances.entry_count + 1,
                           updated_at  = EXCLUDED.updated_at
                    """,
            nativeQuery = true)
    void applyDelta(
            @Param("merchantId") String merchantId,
            @Param("currency") String currency,
            @Param("delta") BigDecimal delta,
            @Param("updatedAt") Instant updatedAt);
}
