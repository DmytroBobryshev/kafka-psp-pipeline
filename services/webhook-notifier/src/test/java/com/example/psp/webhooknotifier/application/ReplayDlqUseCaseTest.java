package com.example.psp.webhooknotifier.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.webhooknotifier.domain.model.DlqRecord;
import com.example.psp.webhooknotifier.domain.model.RetryChain;
import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import com.example.psp.webhooknotifier.domain.port.DlqReader;
import com.example.psp.webhooknotifier.domain.port.WebhookDeliveryPublisher;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * Plain JUnit against {@code application/} + {@code domain/} - fakes for both ports, no Kafka.
 * Exercises M8 requirement #7: the replay endpoint republishes DLQ records to the base delivery
 * topic and stamps ADR-0006's replay provenance headers ({@code x-replayed-from}/
 * {@code x-replay-count}), and honours the requested batch size as a pass-through to
 * {@link DlqReader#pollBatch} (the actual bound is {@code adapters.out.kafka.KafkaDlqReader}'s
 * job, exercised separately since it needs a real consumer factory).
 */
class ReplayDlqUseCaseTest {

    private static final RetryChain CHAIN =
            new RetryChain("base", List.of(new RetryChain.Tier("retry5s", Duration.ofSeconds(5))), "dlq");

    @Test
    void replayRepublishesEachRecordToTheBaseTopicWithReplayProvenanceStamped() {
        RetryEnvelope envelopeFromDlq =
                new RetryEnvelope(
                        4,
                        "base",
                        0,
                        7L,
                        Instant.parse("2026-01-01T00:00:00Z"),
                        "merchant-5xx-or-timeout",
                        "exhausted retry chain",
                        null,
                        null);
        DlqRecord record = new DlqRecord("merchant-1", command(), envelopeFromDlq);
        FakeDlqReader reader = new FakeDlqReader(List.of(record));
        RecordingPublisher publisher = new RecordingPublisher();
        ReplayDlqUseCase useCase = new ReplayDlqUseCase(reader, publisher, CHAIN);

        int replayedCount = useCase.replay(10);

        assertThat(replayedCount).isEqualTo(1);
        assertThat(reader.lastRequestedMax).isEqualTo(10);

        assertThat(publisher.published).hasSize(1);
        Published published = publisher.published.get(0);
        assertThat(published.topic()).isEqualTo("base");
        assertThat(published.command()).isEqualTo(record.command());
        // Original attempt-chain provenance is PRESERVED, not reset...
        assertThat(published.envelope().attemptCount()).isEqualTo(4);
        assertThat(published.envelope().originalTopic()).isEqualTo("base");
        // ...and replay provenance is ADDED on top.
        assertThat(published.envelope().replayedFrom()).isEqualTo("dlq");
        assertThat(published.envelope().replayCount()).isEqualTo(1);
    }

    @Test
    void secondReplayOfTheSameLogicalDeliveryIncrementsReplayCount() {
        RetryEnvelope alreadyReplayedOnce =
                new RetryEnvelope(4, "base", 0, 7L, Instant.parse("2026-01-01T00:00:00Z"), "x", "y", "dlq", 1);
        DlqRecord record = new DlqRecord("merchant-1", command(), alreadyReplayedOnce);
        FakeDlqReader reader = new FakeDlqReader(List.of(record));
        RecordingPublisher publisher = new RecordingPublisher();
        ReplayDlqUseCase useCase = new ReplayDlqUseCase(reader, publisher, CHAIN);

        useCase.replay(10);

        assertThat(publisher.published.get(0).envelope().replayCount()).isEqualTo(2);
    }

    @Test
    void emptyDlqReplaysNothing() {
        FakeDlqReader reader = new FakeDlqReader(List.of());
        RecordingPublisher publisher = new RecordingPublisher();
        ReplayDlqUseCase useCase = new ReplayDlqUseCase(reader, publisher, CHAIN);

        assertThat(useCase.replay(10)).isZero();
        assertThat(publisher.published).isEmpty();
    }

    private static WebhookDeliveryCommand command() {
        return new WebhookDeliveryCommand(
                UUID.randomUUID(), "merchant-1", BigDecimal.TEN, "EUR", "SUCCEEDED", null, UUID.randomUUID(), "trace-1", "corr-1");
    }

    private record Published(String topic, WebhookDeliveryCommand command, RetryEnvelope envelope) {}

    private static final class FakeDlqReader implements DlqReader {
        private final List<DlqRecord> records;
        private int lastRequestedMax = -1;

        private FakeDlqReader(List<DlqRecord> records) {
            this.records = records;
        }

        @Override
        public List<DlqRecord> pollBatch(int maxRecords) {
            lastRequestedMax = maxRecords;
            return records;
        }
    }

    private static final class RecordingPublisher implements WebhookDeliveryPublisher {
        private final List<Published> published = new ArrayList<>();

        @Override
        public CompletableFuture<Void> publishNow(String topic, WebhookDeliveryCommand command, RetryEnvelope envelope) {
            published.add(new Published(topic, command, envelope));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> publishDelayed(
                String topic, WebhookDeliveryCommand command, RetryEnvelope envelope, Duration delay) {
            throw new UnsupportedOperationException("replay never delays");
        }
    }
}
