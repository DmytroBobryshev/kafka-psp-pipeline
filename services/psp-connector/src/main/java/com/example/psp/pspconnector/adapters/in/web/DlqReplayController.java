package com.example.psp.pspconnector.adapters.in.web;

import com.example.psp.pspconnector.application.ReplayDlqUseCase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * M17's DLQ replay endpoint: reads a bounded batch of records off
 * {@code payments.payment-requested.v1.psp-connector.dlq} and republishes each, byte-for-byte
 * unchanged, to {@code payments.payment-requested.v1} - see {@code application.ReplayDlqUseCase}
 * for why that is safe even for a record that turns out to have already been processed.
 *
 * <p>Reachable through api-gateway at {@code POST /api/psp-connector/dlq/replay}: the gateway's
 * {@code psp-connector} route (services/api-gateway/src/main/resources/application-*.yml) matches
 * {@code Path=/api/psp-connector/**} with {@code StripPrefix=2}, which removes exactly the two
 * leading segments ({@code /api/psp-connector}) before forwarding - so the path this service
 * receives, and therefore the path mapped below, is {@code /dlq/replay}.
 *
 * <h2>The guard</h2>
 *
 * <p>{@code maxRecords} is a REQUEST from the caller, not a promise - it is clamped by
 * {@code adapters.out.kafka.KafkaDlqReader} to {@code psp-connector.dlq-replay.max-batch-size}
 * (default 50) no matter what is passed here, so one call can never accidentally attempt to replay
 * an unbounded backlog. There is no "replay everything" mode by design - the same reasoning as
 * webhook-notifier's identically-shaped M8 endpoint: whatever put a record here almost certainly
 * needs a code or configuration fix first, and a bulk replay would just as efficiently fail against
 * it a second time.
 */
@RestController
@RequestMapping("/dlq")
public class DlqReplayController {

    private final ReplayDlqUseCase replayDlqUseCase;
    private final String dlqTopic;
    private final String mainTopic;

    public DlqReplayController(
            ReplayDlqUseCase replayDlqUseCase,
            @Value("${psp-connector.dlq-replay.dlq-topic}") String dlqTopic,
            @Value("${psp-connector.kafka.payment-requested-topic}") String mainTopic) {
        this.replayDlqUseCase = replayDlqUseCase;
        this.dlqTopic = dlqTopic;
        this.mainTopic = mainTopic;
    }

    @PostMapping("/replay")
    public DlqReplayResponse replay(@RequestParam(name = "maxRecords", defaultValue = "10") int maxRecords) {
        int replayedCount = replayDlqUseCase.replay(maxRecords);
        return new DlqReplayResponse(replayedCount, dlqTopic, mainTopic);
    }
}
