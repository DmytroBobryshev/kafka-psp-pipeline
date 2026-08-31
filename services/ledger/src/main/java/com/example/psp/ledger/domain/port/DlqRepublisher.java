package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.DlqRecord;

/**
 * Outbound port for republishing one DLQ record, byte-for-byte unchanged, back onto
 * {@code payments.payment-status-changed.v1}. Implemented by
 * {@code adapters.out.kafka.KafkaDlqRepublisher}.
 *
 * <p>Safe to call even for a record that has already been successfully applied some other way: the
 * ledger's idempotency key ({@code ledger_entries.inbound_event_id}, see
 * {@code application.RecordLedgerEntryUseCase} and README's "Where Kafka EOS ends") makes a replay
 * of an already-applied record a no-op on the consuming side. A record that genuinely was never
 * successfully applied - the actual reason it is in the DLQ - is instead processed fresh, exactly
 * as if it had just arrived on {@code payments.payment-status-changed.v1} for the first time.
 */
public interface DlqRepublisher {

    void republish(DlqRecord record);
}
