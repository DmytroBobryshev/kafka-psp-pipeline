package com.example.psp.ledger.adapters.out.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface MerchantBalanceJpaRepository extends JpaRepository<MerchantBalanceEntity, String> {

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
