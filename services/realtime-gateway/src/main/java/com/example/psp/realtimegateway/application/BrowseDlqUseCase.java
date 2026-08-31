package com.example.psp.realtimegateway.application;

import com.example.psp.realtimegateway.domain.model.DlqRecordView;
import com.example.psp.realtimegateway.domain.port.DlqBrowser;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * M17 page 3's generic DLQ browse: a non-destructive peek at the last {@code max} records of any
 * {@code *.dlq} topic in the cluster. Unlike {@code webhook-notifier}'s {@code ReplayDlqUseCase},
 * this never commits an offset and never republishes anything - it is read-only "what's in there
 * right now", the same distinction {@code adapters.out.kafka.KafkaDlqBrowser}'s javadoc explains
 * mechanically.
 *
 * <p>The {@code .dlq} suffix guard lives here, not in {@code adapters.in.web.ClusterOpsController},
 * so it is enforced for every caller of this use case, not just the REST edge - it throws a plain
 * {@link IllegalArgumentException}, which {@code common-web.GlobalExceptionHandler} already turns
 * into a {@code 400} with no extra code needed in this service.
 */
@Service
public class BrowseDlqUseCase {

    /**
     * Hard ceiling on {@code max}, independent of whatever the caller asks for - one peek call
     * must stay bounded regardless of the request, same guard shape as
     * {@code webhook-notifier.dlq-replay.max-batch-size}.
     */
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
