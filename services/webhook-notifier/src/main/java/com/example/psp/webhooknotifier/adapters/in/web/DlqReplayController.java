package com.example.psp.webhooknotifier.adapters.in.web;

import com.example.psp.webhooknotifier.application.ReplayDlqUseCase;
import com.example.psp.webhooknotifier.domain.model.RetryChain;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
