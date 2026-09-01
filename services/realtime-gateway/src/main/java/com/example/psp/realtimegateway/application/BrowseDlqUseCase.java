package com.example.psp.realtimegateway.application;

import com.example.psp.realtimegateway.domain.model.DlqRecordView;
import com.example.psp.realtimegateway.domain.port.DlqBrowser;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class BrowseDlqUseCase {

    private static final int MAX_RECORDS_CEILING = 500;

    private final DlqBrowser dlqBrowser;

    public BrowseDlqUseCase(DlqBrowser dlqBrowser) {
        this.dlqBrowser = dlqBrowser;
    }

    public List<DlqRecordView> peekLast(String topic, int max) {
        if (topic == null || !topic.endsWith(".dlq")) {
            throw new IllegalArgumentException("Not a DLQ topic (must end with '.dlq'): " + topic);
        }
        int effectiveMax = Math.max(1, Math.min(max, MAX_RECORDS_CEILING));
        return dlqBrowser.peekLast(topic, effectiveMax);
    }
}
