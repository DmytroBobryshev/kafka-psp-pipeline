package com.example.psp.paymentapi.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * M13. Pure unit test of the claim-check decision - no Spring, no Kafka, no MinIO. The only
 * behaviour worth pinning down is the boundary: a document exactly at the threshold is still
 * inlined (the threshold names the largest size still worth inlining, not the smallest size that
 * must be claim-checked - see the class javadoc).
 */
class ClaimCheckPolicyTest {

    private static final long THRESHOLD = 524_288L; // 512 KiB, matches application.yml's default

    @Test
    void documentBelowThresholdIsInlined() {
        assertThat(ClaimCheckPolicy.requiresClaimCheck(THRESHOLD - 1, THRESHOLD)).isFalse();
    }

    @Test
    void documentExactlyAtThresholdIsInlined() {
        assertThat(ClaimCheckPolicy.requiresClaimCheck(THRESHOLD, THRESHOLD)).isFalse();
    }

    @Test
    void documentOneByteOverThresholdIsClaimChecked() {
        assertThat(ClaimCheckPolicy.requiresClaimCheck(THRESHOLD + 1, THRESHOLD)).isTrue();
    }

    @Test
    void emptyDocumentIsInlined() {
        assertThat(ClaimCheckPolicy.requiresClaimCheck(0, THRESHOLD)).isFalse();
    }

    @Test
    void aFiveMegabyteDisputeDocumentIsClaimChecked() {
        // The README's measured-demo example - see services/payment-api/README.md's "M13: claim
        // check, measured" section.
        long fiveMib = 5L * 1024 * 1024;
        assertThat(ClaimCheckPolicy.requiresClaimCheck(fiveMib, THRESHOLD)).isTrue();
    }
}
