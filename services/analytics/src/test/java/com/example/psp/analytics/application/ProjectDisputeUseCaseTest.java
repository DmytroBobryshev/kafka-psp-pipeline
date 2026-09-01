package com.example.psp.analytics.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.analytics.domain.model.DisputeProjection;
import com.example.psp.analytics.domain.port.DisputeDocumentFetcher;
import com.example.psp.analytics.domain.port.DisputeProjectionRepository;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProjectDisputeUseCaseTest {

    private final RecordingFetcher fetcher = new RecordingFetcher();
    private final RecordingRepository repository = new RecordingRepository();
    private final ProjectDisputeUseCase useCase = new ProjectDisputeUseCase(fetcher, repository);

    @Test
    void anInlineDisputeIsHashedDirectlyAndNeverCallsTheFetcher() {
        byte[] bytes = "hello dispute".getBytes();
        ProjectDisputeCommand command =
                new ProjectDisputeCommand(
                        "dispute-1", "payment-1", "acme", "reason", false, bytes, null, null, 0);

        useCase.execute(command);

        assertThat(fetcher.calls).isEmpty();
        assertThat(repository.saved).hasSize(1);
        DisputeProjection projection = repository.saved.get(0);
        assertThat(projection.claimChecked()).isFalse();
        assertThat(projection.sizeBytes()).isEqualTo(bytes.length);
        assertThat(projection.sha256()).isEqualTo(sha256Hex(bytes));
    }

    @Test
    void aClaimCheckedDisputeDereferencesTheObjectAndHashesTheFetchedBytes() {
        byte[] fetchedBytes = "the real document content".getBytes();
        fetcher.nextResponse = fetchedBytes;
        ProjectDisputeCommand command =
                new ProjectDisputeCommand(
                        "dispute-2",
                        "payment-2",
                        "acme",
                        "reason",
                        true,
                        null,
                        "disputes",
                        "dispute-2",
                        fetchedBytes.length);

        useCase.execute(command);

        assertThat(fetcher.calls).containsExactly("disputes/dispute-2");
        DisputeProjection projection = repository.saved.get(0);
        assertThat(projection.claimChecked()).isTrue();
        assertThat(projection.bucket()).isEqualTo("disputes");
        assertThat(projection.objectKey()).isEqualTo("dispute-2");
        assertThat(projection.sizeBytes()).isEqualTo(fetchedBytes.length);
        assertThat(projection.sha256()).isEqualTo(sha256Hex(fetchedBytes));
    }

    @Test
    void theProjectedSizeIsTheMeasuredSizeNotTheClaimedSizeOnMismatch() {
        byte[] fetchedBytes = new byte[100];
        fetcher.nextResponse = fetchedBytes;
        ProjectDisputeCommand command =
                new ProjectDisputeCommand(
                        "dispute-3", "payment-3", "acme", "reason", true, null, "disputes", "dispute-3", 999L);

        useCase.execute(command);

        assertThat(repository.saved.get(0).sizeBytes()).isEqualTo(100L);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class RecordingFetcher implements DisputeDocumentFetcher {
        private final List<String> calls = new ArrayList<>();
        private byte[] nextResponse = new byte[0];

        @Override
        public byte[] fetch(String bucket, String objectKey) {
            calls.add(bucket + "/" + objectKey);
            return nextResponse;
        }
    }

    private static final class RecordingRepository implements DisputeProjectionRepository {
        private final List<DisputeProjection> saved = new ArrayList<>();

        @Override
        public void save(DisputeProjection projection) {
            saved.add(projection);
        }
    }
}
