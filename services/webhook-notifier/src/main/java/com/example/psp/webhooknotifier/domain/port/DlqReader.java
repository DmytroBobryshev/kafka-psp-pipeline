package com.example.psp.webhooknotifier.domain.port;

import com.example.psp.webhooknotifier.domain.model.DlqRecord;
import java.util.List;

/**
 * Outbound port for reading a bounded batch of records off the DLQ topic, for
 * {@code application.ReplayDlqUseCase} (M8 requirement #7). Implemented by
 * {@code adapters.out.kafka.KafkaDlqReader}.
 *
 * <p>Deliberately not a {@code @KafkaListener}: ADR-0006 is explicit that "DLQ is not a queue, it
 * is an inbox" and replay must be manual and explicit, never automatic. This port models a
 * one-shot, on-demand read, not a subscription.
 */
public interface DlqReader {

    /**
     * Reads and commits up to {@code maxRecords} records from the DLQ topic under this reader's
     * own dedicated consumer group, so records already replayed are never returned again by a
     * later call. Returns fewer than {@code maxRecords} if the DLQ currently holds fewer than
     * that many unreplayed records, and an empty list if the DLQ is empty.
     */
    List<DlqRecord> pollBatch(int maxRecords);
}
