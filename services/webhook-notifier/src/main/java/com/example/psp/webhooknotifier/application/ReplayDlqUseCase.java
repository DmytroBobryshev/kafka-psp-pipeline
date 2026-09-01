package com.example.psp.webhooknotifier.application;

import com.example.psp.webhooknotifier.domain.model.DlqRecord;
import com.example.psp.webhooknotifier.domain.model.RetryChain;
import com.example.psp.webhooknotifier.domain.port.DlqReader;
import com.example.psp.webhooknotifier.domain.port.WebhookDeliveryPublisher;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
