package com.example.psp.paymentapi.application;

import java.util.UUID;

/**
 * What {@link OpenDisputeUseCase} hands back to the web layer (M13). {@code claimChecked} plus
 * the nullable {@code bucket}/{@code objectKey} pair let {@code adapters.in.web.DisputeResponse}
 * show the caller exactly which path this dispute took without re-deciding anything - the use
 * case already knows, and re-deriving it from {@code sizeBytes} a second time at the web layer
 * would be the threshold check duplicated in two places with the threshold value itself only
 * ever configured in one of them.
 */
public record DisputeOutcome(
        UUID disputeId,
        UUID paymentId,
        String merchantId,
        long sizeBytes,
        boolean claimChecked,
        String bucket,
        String objectKey) {
}
