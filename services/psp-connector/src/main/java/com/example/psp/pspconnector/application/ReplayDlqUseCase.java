package com.example.psp.pspconnector.application;

import com.example.psp.pspconnector.domain.model.DlqRecord;
import com.example.psp.pspconnector.domain.port.DlqReader;
import com.example.psp.pspconnector.domain.port.DlqRepublisher;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * M17's DLQ replay endpoint: reads a bounded batch of records off
 * {@code payments.payment-requested.v1.psp-connector.dlq} and republishes each, byte-for-byte
 * unchanged, to {@code payments.payment-requested.v1} - giving it a full fresh pass through
 * {@code adapters.in.kafka.PaymentRequestedListener} exactly as if it had just arrived.
 *
 * <p>Called ONLY by {@code adapters.in.web.DlqReplayController}, on demand, never on a schedule or
 * a timer - the same "DLQ is not a queue, it is an inbox" rule webhook-notifier's M8
 * {@code ReplayDlqUseCase} documents: whatever put a record here almost certainly needs a code or
 * configuration fix first, so automatic redrive would just as automatically fail again. Replay is
 * safe to call speculatively, including on records already fixed and reprocessed some other way -
 * see {@code domain.port.DlqRepublisher}'s javadoc for why a replay of an already-processed record
 * is a guaranteed no-op downstream (M5 level 1 dedup), never a duplicate authorization.
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
     *                   {@code psp-connector.dlq-replay.max-batch-size} in {@code application.yml}
     *                   for the guard).
     * @return how many records were actually read and republished.
     */
    public int replay(int maxRecords) {
        List<DlqRecord> records = dlqReader.pollBatch(maxRecords);
        for (DlqRecord record : records) {
            log.info("Replaying DLQ record key(paymentId)={}", record.key());
            republisher.republish(record);
        }
        log.info("DLQ replay complete: {} record(s) republished", records.size());
        return records.size();
    }
}
