package com.example.psp.pspconnector.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.pspconnector.domain.model.DlqHeader;
import com.example.psp.pspconnector.domain.model.DlqRecord;
import com.example.psp.pspconnector.domain.port.DlqReader;
import com.example.psp.pspconnector.domain.port.DlqRepublisher;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ReplayDlqUseCaseTest {

    @Test
    void replayRepublishesEachRecordByteForByteUnchanged() {
        byte[] value = "avro-wire-bytes".getBytes(StandardCharsets.UTF_8);
        DlqHeader header = new DlqHeader("event-id", "abc-123".getBytes(StandardCharsets.UTF_8));
        DlqRecord record = new DlqRecord("payment-1", value, List.of(header));
        FakeDlqReader reader = new FakeDlqReader(List.of(record));
        RecordingRepublisher republisher = new RecordingRepublisher();
        ReplayDlqUseCase useCase = new ReplayDlqUseCase(reader, republisher);

        int replayedCount = useCase.replay(10);

        assertThat(replayedCount).isEqualTo(1);
        assertThat(reader.lastRequestedMax).isEqualTo(10);

        assertThat(republisher.published).hasSize(1);
        DlqRecord published = republisher.published.get(0);
        assertThat(published.key()).isEqualTo("payment-1");
        assertThat(published.value()).isEqualTo(value);
        assertThat(published.headers()).containsExactly(header);
        assertThat(published)
                .isEqualTo(
                        new DlqRecord(
                                "payment-1",
                                "avro-wire-bytes".getBytes(StandardCharsets.UTF_8),
                                List.of(new DlqHeader("event-id", "abc-123".getBytes(StandardCharsets.UTF_8)))));
    }

    @Test
    void emptyDlqReplaysNothing() {
        FakeDlqReader reader = new FakeDlqReader(List.of());
        RecordingRepublisher republisher = new RecordingRepublisher();
        ReplayDlqUseCase useCase = new ReplayDlqUseCase(reader, republisher);

        assertThat(useCase.replay(10)).isZero();
        assertThat(republisher.published).isEmpty();
    }

    @Test
    void replaysMultipleRecordsInOrder() {
        DlqRecord first = new DlqRecord("payment-1", "a".getBytes(StandardCharsets.UTF_8), List.of());
        DlqRecord second = new DlqRecord("payment-2", "b".getBytes(StandardCharsets.UTF_8), List.of());
        FakeDlqReader reader = new FakeDlqReader(List.of(first, second));
        RecordingRepublisher republisher = new RecordingRepublisher();
        ReplayDlqUseCase useCase = new ReplayDlqUseCase(reader, republisher);

        assertThat(useCase.replay(10)).isEqualTo(2);
        assertThat(republisher.published).containsExactly(first, second);
    }

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

    private static final class RecordingRepublisher implements DlqRepublisher {
        private final List<DlqRecord> published = new ArrayList<>();

        @Override
        public void republish(DlqRecord record) {
            published.add(record);
        }
    }
}
