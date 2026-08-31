package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.DlqRecord;
import java.util.List;

/**
 * Outbound port for reading a bounded batch of records off
 * {@code payments.payment-status-changed.v1.ledger.dlq}, for
 * {@code application.ReplayDlqUseCase} (M17). Implemented by
 * {@code adapters.out.kafka.KafkaDlqReader}.
 *
 * <p>Deliberately not a {@code @KafkaListener}: replay is manual and explicit, triggered only by
 * {@code adapters.in.web.DlqReplayController} - the same "DLQ is not a queue, it is an inbox"
 * reasoning webhook-notifier's M8 {@code DlqReader} documents for the identical shape. This port
 * models a one-shot, on-demand read, not a subscription, and is entirely separate from M7's
 * transactional {@code payment-status-changed} consumer ({@code config.KafkaConsumerConfig}) - see
 * {@code adapters.out.kafka.KafkaDlqRepublisher}'s javadoc for why replay stays off that machinery.
 */
public interface DlqReader {

    /**
     * Reads and commits up to {@code maxRecords} records from the DLQ topic under this reader's
     * own dedicated consumer group ({@code ledger.dlq-replay.consumer-group}), so records already
     * replayed are never returned again by a later call. Returns fewer than {@code maxRecords} if
     * the DLQ currently holds fewer than that many unreplayed records, and an empty list if the DLQ
     * is empty.
     */
    List<DlqRecord> pollBatch(int maxRecords);
}
