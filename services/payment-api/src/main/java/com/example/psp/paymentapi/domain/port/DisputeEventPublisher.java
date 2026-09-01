package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.DocumentReference;
import java.util.UUID;

public interface DisputeEventPublisher {

    void publishInline(
            UUID disputeId,
            UUID paymentId,
            String merchantId,
            String reason,
            byte[] documentBytes,
            String contentType);

    void publishClaimChecked(
            UUID disputeId, UUID paymentId, String merchantId, String reason, DocumentReference reference);
}
