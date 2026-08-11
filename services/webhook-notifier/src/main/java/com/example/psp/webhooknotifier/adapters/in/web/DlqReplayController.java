package com.example.psp.webhooknotifier.adapters.in.web;

import com.example.psp.webhooknotifier.application.ReplayDlqUseCase;
import com.example.psp.webhooknotifier.domain.model.RetryChain;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * M8 requirement #7: the DLQ replay endpoint. Reads a bounded batch of records off
 * {@code webhooks.webhook-delivery-requested.v1.dlq} and republishes each to
 * {@code webhooks.webhook-delivery-requested.v1} (ADR-0006: "Replay is manual and explicit").
 *
 * <h2>The guard</h2>
 *
 * <p>{@code maxRecords} is a REQUEST from the caller, not a promise - it is clamped by
 * {@code adapters.out.kafka.KafkaDlqReader} to {@code webhook-notifier.dlq-replay.max-batch-size}
 * (default 50) no matter what is passed here, so one call can never accidentally attempt to
 * replay an unbounded backlog. There is no "replay everything" mode by design: a merchant with a
 * thousand DLQ'd deliveries almost certainly has a code or configuration problem that a bulk
 * replay would just as efficiently fail against a second time (ADR-0006's "whatever put a record
 * there usually needs a code change first").
 */
@RestController
@RequestMapping("/api/webhooks/dlq")
public class DlqReplayController {

    private final ReplayDlqUseCase replayDlqUseCase;
    private final RetryChain retryChain;

    public DlqReplayController(ReplayDlqUseCase replayDlqUseCase, RetryChain retryChain) {
        this.replayDlqUseCase = replayDlqUseCase;
        this.retryChain = retryChain;
    }

    @PostMapping("/replay")
    public DlqReplayResponse replay(@RequestParam(name = "maxRecords", defaultValue = "10") int maxRecords) {
        int replayedCount = replayDlqUseCase.replay(maxRecords);
        return new DlqReplayResponse(replayedCount, retryChain.dlqTopic(), retryChain.baseTopic());
    }
}
