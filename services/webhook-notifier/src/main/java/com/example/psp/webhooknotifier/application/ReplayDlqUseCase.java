package com.example.psp.webhooknotifier.application;

import com.example.psp.webhooknotifier.domain.model.DlqRecord;
import com.example.psp.webhooknotifier.domain.model.RetryChain;
import com.example.psp.webhooknotifier.domain.port.DlqReader;
import com.example.psp.webhooknotifier.domain.port.WebhookDeliveryPublisher;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * M8 requirement #7: DLQ replay. Reads a bounded batch of records off the DLQ topic and
 * republishes each, unchanged in key and payload, to {@link RetryChain#baseTopic()} - giving it a
 * full fresh pass through the retry chain if it fails again.
 *
 * <p>Per ADR-0006, "DLQ is not a queue, it is an inbox": this use case is called ONLY by
 * {@code adapters.in.web.DlqReplayController}, on demand, never on a schedule or a timer -
 * "whatever put a record there usually needs a code change first", so automatic redrive would
 * just as automatically fail again.
 */
@Service
public class ReplayDlqUseCase {

    private static final Logger log = LoggerFactory.getLogger(ReplayDlqUseCase.class);

    private final DlqReader dlqReader;
    private final WebhookDeliveryPublisher publisher;
    private final RetryChain retryChain;

    public ReplayDlqUseCase(DlqReader dlqReader, WebhookDeliveryPublisher publisher, RetryChain retryChain) {
        this.dlqReader = dlqReader;
        this.publisher = publisher;
        this.retryChain = retryChain;
    }

    /**
     * @param maxRecords the caller's requested batch size - clamped to the configured ceiling by
     *                   {@code adapters.out.kafka.KafkaDlqReader} (see that class and
     *                   {@code config.WebhookNotifierProperties#dlqReplay()} for the guard).
     * @return how many records were actually read and republished.
     */
    public int replay(int maxRecords) {
        List<DlqRecord> records = dlqReader.pollBatch(maxRecords);
        for (DlqRecord record : records) {
            log.info(
                    "Replaying DLQ record merchantId={} paymentId={} originalAttempt={} -> {}",
                    record.key(),
                    record.command().paymentId(),
                    record.envelope().attemptCount(),
                    retryChain.baseTopic());
            publisher.publishNow(
                    retryChain.baseTopic(), record.command(), record.envelope().withReplay(retryChain.dlqTopic()));
        }
        log.info("DLQ replay complete: {} record(s) republished to {}", records.size(), retryChain.baseTopic());
        return records.size();
    }
}
