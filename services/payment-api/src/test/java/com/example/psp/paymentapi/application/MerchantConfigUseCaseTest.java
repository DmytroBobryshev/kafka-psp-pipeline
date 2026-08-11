package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.paymentapi.domain.model.MerchantConfig;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.port.MerchantConfigPublisher;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * M10. Pure unit test of the merchant-config use cases against a recording fake - no Spring, no
 * Kafka, no Schema Registry. The point of interest is not the upsert (it is a field copy) but the
 * shape of the delete: the use case emits a <b>tombstone instruction</b>, not an "update with
 * status=DELETED", and the fake below records the two as distinguishable events so a future
 * refactor that quietly turns a tombstone into a flag fails here.
 */
class MerchantConfigUseCaseTest {

    private final RecordingPublisher publisher = new RecordingPublisher();
    private final MerchantConfigUseCase useCase = new MerchantConfigUseCase(publisher);

    @Test
    void upsertPublishesTheCompleteSnapshot() {
        UpsertMerchantConfigCommand command =
                new UpsertMerchantConfigCommand(
                        "acme", "ACME Corp", MerchantStatus.ACTIVE, "EUR", "https://acme.test/hook", 1500);

        MerchantConfig published = useCase.upsert(command);

        assertThat(publisher.upserts).containsExactly(published);
        assertThat(publisher.tombstones).isEmpty();
        assertThat(published.merchantId()).isEqualTo("acme");
        assertThat(published.declineRateAlertThresholdBps()).isEqualTo(1500);
    }

    @Test
    void deletePublishesATombstoneAndNotAStatusUpdate() {
        useCase.delete("acme");

        assertThat(publisher.tombstones).containsExactly("acme");
        // The assertion that actually matters: nothing at all was published as a VALUE. A
        // "deleted" state expressed as a value would survive compaction forever and leave every
        // downstream GlobalKTable holding a live row for a merchant that no longer exists.
        assertThat(publisher.upserts).isEmpty();
    }

    @Test
    void deleteIsIdempotent() {
        useCase.delete("acme");
        useCase.delete("acme");

        // Two tombstones, identical effect. This is why DELETE can be retried blindly after a
        // 5xx: compaction resolves a key to its last record, and both records say "no value".
        assertThat(publisher.tombstones).containsExactly("acme", "acme");
    }

    @Test
    void domainInvariantsAreEnforcedByTheUseCaseNotOnlyByTheWebDto() {
        UpsertMerchantConfigCommand invalid =
                new UpsertMerchantConfigCommand("acme", "ACME Corp", MerchantStatus.ACTIVE, "EUR", null, 20_000);

        assertThatThrownBy(() -> useCase.upsert(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("declineRateAlertThresholdBps");

        assertThat(publisher.upserts).isEmpty();
    }

    /** Records upserts and tombstones separately - conflating them is the bug under test. */
    private static final class RecordingPublisher implements MerchantConfigPublisher {

        private final List<MerchantConfig> upserts = new ArrayList<>();
        private final List<String> tombstones = new ArrayList<>();

        @Override
        public void publishConfigChanged(MerchantConfig config) {
            upserts.add(config);
        }

        @Override
        public void publishConfigDeleted(String merchantId) {
            tombstones.add(merchantId);
        }
    }
}
