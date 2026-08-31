package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.DlqRecord;
import com.example.psp.ledger.domain.port.DlqReader;
import com.example.psp.ledger.domain.port.DlqRepublisher;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * M17's DLQ replay endpoint: reads a bounded batch of records off
 * {@code payments.payment-status-changed.v1.ledger.dlq} and republishes each, byte-for-byte
 * unchanged, to {@code payments.payment-status-changed.v1} - giving it a full fresh pass through
 * {@code adapters.in.kafka.PaymentStatusChangedListener} exactly as if it had just arrived.
 *
 * <p>Called ONLY by {@code adapters.in.web.DlqReplayController}, on demand, never on a schedule or
 * a timer - the same "DLQ is not a queue, it is an inbox" rule webhook-notifier's M8
 * {@code ReplayDlqUseCase} documents: whatever put a record here almost certainly needs a code or
 * configuration fix first, so automatic redrive would just as automatically fail again. Replay is
 * safe to call speculatively, including on records already applied some other way - see
 * {@code domain.port.DlqRepublisher}'s javadoc for why a replay of an already-applied record is a
 * guaranteed no-op downstream (the {@code ledger_entries.inbound_event_id} idempotency key), never
 * a double-counted balance change.
 *
 * <p>Holds no Kafka or transaction-management import of any kind - see
 * {@code architecture.HexagonalArchitectureTest#applicationMustNotDependOnKafkaOrTransactionApis}.
 * Unlike {@code application.RecordLedgerEntryUseCase}, this use case has nothing to do with M7's
 * exactly-once machinery at all: it republishes onto the plain, non-transactional producer
 * {@code adapters.out.kafka.KafkaDlqRepublisher} wraps, entirely separate from
 * {@code config.KafkaProducerConfig#ledgerEntryProducerFactory}.
 */
@Service
public class ReplayDlqUseCase {

    private static final Logger log = LoggerFactory.getLogger(ReplayDlqUseCase.class);

    private final DlqReader dlqReader;
    private final DlqRepublisher republisher;

    public ReplayDlqUseCase(DlqReader dlqReader, DlqRepublisher republisher) {
        this.dlqReader = dlqReader;
        this.republisher = republisher;
    }

    /**
     * @param maxRecords the caller's requested batch size - clamped to the configured ceiling by
     *                   {@code adapters.out.kafka.KafkaDlqReader} (see that class and
     *                   {@code ledger.dlq-replay.max-batch-size} in {@code application.yml} for the
     *                   guard).
     * @return how many records were actually read and republished.
     */
    public int replay(int maxRecords) {
        List<DlqRecord> records = dlqReader.pollBatch(maxRecords);
        for (DlqRecord record : records) {
            log.info("Replaying DLQ record key(merchantId)={}", record.key());
            republisher.republish(record);
        }
        log.info("DLQ replay complete: {} record(s) republished", records.size());
        return records.size();
    }
}
