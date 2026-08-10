package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;

/**
 * Outbound port for publishing {@code ledger.ledger-entry-recorded.v1}. Implemented by
 * {@code adapters.out.kafka.KafkaLedgerEntryPublisher} - the domain never imports
 * {@code org.apache.kafka} or Spring Kafka directly (ADR-0007).
 *
 * <p>The implementation's send happens on a <b>transactional</b> producer and therefore only
 * inside the Kafka transaction the listener container already started. That is invisible from
 * here on purpose: the port's contract is "record this entry as an event", and whether the
 * surrounding transaction later commits or aborts is decided by whether the listener method
 * returns normally, not by anything this port exposes.
 *
 * @see com.example.psp.ledger.domain.exception.DeliberateAbortException
 */
public interface LedgerEntryPublisher {

    /**
     * @param entry        the entry that was just durably applied to Postgres.
     * @param balanceAfter the merchant's balance after applying it, published alongside so
     *                     downstream consumers never need to query this service's database
     *                     (ADR-0004/ADR-0005).
     */
    void publishEntryRecorded(LedgerEntry entry, MerchantBalance balanceAfter);
}
