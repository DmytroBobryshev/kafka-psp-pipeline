package com.example.psp.pspconnector.adapters.in.web;

import com.example.psp.pspconnector.application.ReplayDlqUseCase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
