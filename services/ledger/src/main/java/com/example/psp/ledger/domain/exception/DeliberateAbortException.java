package com.example.psp.ledger.domain.exception;

import java.util.UUID;

/**
 * Thrown on purpose by {@code application.RecordLedgerEntryUseCase} when
 * {@code ledger.fail-after-produce=true}, <b>after</b> the {@code ledger.ledger-entry-recorded.v1}
 * record has been produced inside the Kafka transaction and <b>before</b> that transaction
 * commits. This is the abort hook for the read_committed-vs-read_uncommitted experiment; see
 * README.md's "Abort visibility proof".
 *
 * <p>Escaping the listener method is the entire mechanism: the listener container invokes the
 * listener inside a {@code TransactionTemplate} backed by
 * {@code org.springframework.kafka.transaction.KafkaTransactionManager}, so an exception out of
 * that method means {@code Producer.abortTransaction()} instead of {@code commitTransaction()}.
 * The record therefore stays physically in the partition at its assigned offset, but the
 * transaction coordinator writes an <b>ABORT</b> control marker instead of a COMMIT one - which is
 * exactly what the two isolation levels disagree about:
 *
 * <ul>
 *   <li>{@code isolation.level=read_committed} - the consumer buffers records above the Last
 *       Stable Offset and drops the ones listed in the fetch response's aborted-transaction index.
 *       The entry is never delivered.
 *   <li>{@code isolation.level=read_uncommitted} - no buffering, no filtering. The aborted record
 *       is delivered like any other.
 * </ul>
 *
 * <p>The consumer offsets are in the same aborted transaction (they were added with
 * {@code sendOffsetsToTransaction}), so they are not committed either and the record is
 * redelivered. On that redelivery the Postgres idempotency check - a completely separate
 * mechanism, see the class javadoc on {@code application.RecordLedgerEntryUseCase} - finds the
 * ledger entry already applied and short-circuits <em>before</em> reaching this throw, so the
 * consumer makes progress rather than looping forever and the balance is not double-counted.
 *
 * <p>Not classified under ADR-0006 on purpose: this is a test hook, not a production failure mode.
 * It is deliberately <b>not</b> a {@code RetryableException} - a retry chain would only hide the
 * abort that the experiment exists to observe.
 */
public class DeliberateAbortException extends RuntimeException {

    public DeliberateAbortException(UUID entryId, UUID inboundEventId) {
        super(
                "ledger.fail-after-produce=true: aborting the Kafka transaction AFTER producing "
                        + "ledger.ledger-entry-recorded.v1 (entryId="
                        + entryId
                        + ", inboundEventId="
                        + inboundEventId
                        + ") and BEFORE commit - deliberate, see README 'Abort visibility proof'");
    }
}
