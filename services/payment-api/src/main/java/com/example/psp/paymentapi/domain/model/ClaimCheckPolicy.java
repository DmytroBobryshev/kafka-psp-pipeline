package com.example.psp.paymentapi.domain.model;

/**
 * M13's claim-check decision: pure business rule, zero Kafka/Spring/MinIO knowledge (ADR-0007) -
 * this is the one line that decides whether a dispute document travels inline in the Kafka
 * record or as a reference to an object already uploaded to MinIO.
 *
 * <p>The rule itself is deliberately trivial ({@code sizeBytes > thresholdBytes}); what matters
 * is that it lives here, testable with no Spring context and no running broker
 * ({@code ClaimCheckPolicyTest}), rather than being buried as an inline {@code if} inside a
 * Kafka-publishing adapter where a change to the threshold's meaning (inclusive vs exclusive,
 * say) could not be verified without standing up the whole use case.
 *
 * <p><b>Boundary is exclusive of the threshold</b>: a document exactly {@code thresholdBytes}
 * long is still inlined. The threshold names the largest size still worth inlining, not the
 * smallest size that must be claim-checked.
 */
public final class ClaimCheckPolicy {

    private ClaimCheckPolicy() {}

    /**
     * @param sizeBytes      the document's byte length.
     * @param thresholdBytes {@code payment-api.disputes.claim-check-threshold-bytes} - the
     *                       largest document size still sent inline.
     * @return {@code true} when the document must go through MinIO and the event must carry a
     *         {@code ClaimCheckReference} instead of an {@code InlineDocument}.
     */
    public static boolean requiresClaimCheck(long sizeBytes, long thresholdBytes) {
        return sizeBytes > thresholdBytes;
    }
}
