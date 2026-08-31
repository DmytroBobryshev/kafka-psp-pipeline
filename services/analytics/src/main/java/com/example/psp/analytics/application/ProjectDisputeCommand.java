package com.example.psp.analytics.application;

/**
 * Application-layer input model for {@link ProjectDisputeUseCase} (M13) - the Avro-free copy of
 * {@code disputes.dispute-opened.v1}'s {@code document} union
 * ({@code adapters.in.kafka.DisputeOpenedMapper} does the translation; ArchUnit's {@code
 * onlyTheTopologyMayDependOnGeneratedAvro} rule is what forces the translation to happen there and
 * not here).
 *
 * <p>Exactly one of {@code inlineBytes} or {@code (bucket, objectKey)} is populated, mirroring the
 * union it came from: {@code claimChecked} says which. {@code referenceSizeBytes} is only
 * meaningful when {@code claimChecked} is true - it is what payment-api CLAIMED the object's size
 * was, kept separate from whatever size {@link ProjectDisputeUseCase} measures after actually
 * fetching the bytes, so the two can be compared (see that class's javadoc).
 */
public record ProjectDisputeCommand(
        String disputeId,
        String paymentId,
        String merchantId,
        String reason,
        boolean claimChecked,
        byte[] inlineBytes,
        String bucket,
        String objectKey,
        long referenceSizeBytes) {
}
