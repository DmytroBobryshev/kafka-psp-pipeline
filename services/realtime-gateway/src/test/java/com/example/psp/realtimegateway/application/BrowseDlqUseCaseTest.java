package com.example.psp.realtimegateway.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.realtimegateway.domain.model.DlqRecordView;
import com.example.psp.realtimegateway.domain.port.DlqBrowser;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Plain JUnit against {@code application/} + {@code domain/} - a fake {@link DlqBrowser}, no
 * Kafka. Exercises M17 page 3's generic DLQ browse: the {@code .dlq} suffix guard (never reaches
 * the port for a non-DLQ topic) and the {@code max} clamp (the actual bound is
 * {@code BrowseDlqUseCase}'s job, exercised here rather than against a real consumer).
 */
class BrowseDlqUseCaseTest {

    @Test
    void rejectsATopicThatDoesNotEndWithDlqAndNeverCallsThePort() {
        FakeDlqBrowser browser = new FakeDlqBrowser(List.of());
        BrowseDlqUseCase useCase = new BrowseDlqUseCase(browser);

        assertThatThrownBy(() -> useCase.peekLast("payments.payment-requested.v1", 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(".dlq");
        assertThat(browser.lastRequestedTopic).isNull();
    }

    @Test
    void aDlqTopicIsPassedThroughUnchanged() {
        DlqRecordView record =
                new DlqRecordView(
                        "webhooks.webhook-delivery-requested.v2.dlq",
                        0,
                        42L,
                        Instant.parse("2026-01-01T00:00:00Z"),
                        "merchant-1",
                        Map.of("kafka_dlt-exception-message", "boom"),
                        "{\"paymentId\":\"p-1\"}",
                        false);
        FakeDlqBrowser browser = new FakeDlqBrowser(List.of(record));
        BrowseDlqUseCase useCase = new BrowseDlqUseCase(browser);

        List<DlqRecordView> result = useCase.peekLast("webhooks.webhook-delivery-requested.v2.dlq", 20);

        assertThat(result).containsExactly(record);
        assertThat(browser.lastRequestedTopic).isEqualTo("webhooks.webhook-delivery-requested.v2.dlq");
        assertThat(browser.lastRequestedMax).isEqualTo(20);
    }

    @Test
    void maxIsClampedToAtLeastOne() {
        FakeDlqBrowser browser = new FakeDlqBrowser(List.of());
        BrowseDlqUseCase useCase = new BrowseDlqUseCase(browser);

        useCase.peekLast("payments.payment-status-changed.v1.ledger.dlq", 0);

        assertThat(browser.lastRequestedMax).isEqualTo(1);
    }

    @Test
    void maxIsClampedToTheCeilingRegardlessOfWhatWasRequested() {
        FakeDlqBrowser browser = new FakeDlqBrowser(List.of());
        BrowseDlqUseCase useCase = new BrowseDlqUseCase(browser);

        useCase.peekLast("payments.payment-status-changed.v1.ledger.dlq", 100_000);

        assertThat(browser.lastRequestedMax).isEqualTo(500);
    }

    private static final class FakeDlqBrowser implements DlqBrowser {
        private final List<DlqRecordView> records;
        private String lastRequestedTopic;
        private int lastRequestedMax = -1;

        private FakeDlqBrowser(List<DlqRecordView> records) {
            this.records = records;
        }

        @Override
        public List<DlqRecordView> peekLast(String topic, int max) {
            lastRequestedTopic = topic;
            lastRequestedMax = max;
            return records;
        }
    }
}
