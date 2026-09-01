package com.example.psp.paymentapi.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

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
        long fiveMib = 5L * 1024 * 1024;
        assertThat(ClaimCheckPolicy.requiresClaimCheck(fiveMib, THRESHOLD)).isTrue();
    }
}
