package com.example.psp.analytics.domain.port;

/**
 * Outbound port for dereferencing a claim-checked document (M13) - implemented by {@code
 * adapters.out.s3.S3DisputeDocumentFetcher} against MinIO. Only called when the event's {@code
 * document} union was a {@code ClaimCheckReference}; an inline document's bytes are already in
 * hand and never reach this port - see {@code application.ProjectDisputeUseCase}.
 */
public interface DisputeDocumentFetcher {

    /** Fetches the object's full bytes. This IS the claim-check round trip's "check-out" half. */
    byte[] fetch(String bucket, String objectKey);
}
