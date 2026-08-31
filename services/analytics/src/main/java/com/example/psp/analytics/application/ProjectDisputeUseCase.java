package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.DisputeProjection;
import com.example.psp.analytics.domain.port.DisputeDocumentFetcher;
import com.example.psp.analytics.domain.port.DisputeProjectionRepository;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * M13's "check-out" half of the claim-check round trip. For a claim-checked dispute, this use
 * case actually calls {@link DisputeDocumentFetcher#fetch} - GetObject against MinIO - and hashes
 * what comes back; for an inline dispute it hashes the bytes the event already carried. Either
 * way, {@link DisputeProjection#sha256}/{@link DisputeProjection#sizeBytes} are computed from
 * BYTES ANALYTICS ACTUALLY HAS, never copied from the event's claimed size - that distinction is
 * the whole proof this module exists to produce (see the class-level summary in
 * services/payment-api/README.md's "M13: claim check, measured" section: "the reference
 * dereferences", not "the reference looks plausible").
 *
 * <p>{@code referenceSizeBytes} from the command is only used for a log-line cross-check, never
 * for the projection's own {@code sizeBytes} - if payment-api's claimed size and MinIO's actual
 * object size ever disagreed, trusting the claim would silently hide exactly the bug a claim-check
 * consumer exists to catch.
 */
@Service
public class ProjectDisputeUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProjectDisputeUseCase.class);

    private final DisputeDocumentFetcher documentFetcher;
    private final DisputeProjectionRepository repository;

    public ProjectDisputeUseCase(
            DisputeDocumentFetcher documentFetcher, DisputeProjectionRepository repository) {
        this.documentFetcher = documentFetcher;
        this.repository = repository;
    }

    public void execute(ProjectDisputeCommand command) {
        byte[] bytes;
        if (command.claimChecked()) {
            bytes = documentFetcher.fetch(command.bucket(), command.objectKey());
            if (bytes.length != command.referenceSizeBytes()) {
                log.warn(
                        "disputeId={} MinIO object size ({} bytes) does not match the size the event"
                                + " claimed ({} bytes) - projecting the MEASURED size, not the claimed one",
                        command.disputeId(),
                        bytes.length,
                        command.referenceSizeBytes());
            }
        } else {
            bytes = command.inlineBytes();
        }

        DisputeProjection projection =
                new DisputeProjection(
                        command.disputeId(),
                        command.paymentId(),
                        command.merchantId(),
                        command.reason(),
                        bytes.length,
                        sha256Hex(bytes),
                        command.claimChecked(),
                        command.bucket(),
                        command.objectKey());

        repository.save(projection);

        log.info(
                "Projected dispute disputeId={} claimChecked={} sizeBytes={} sha256={}",
                projection.disputeId(),
                projection.claimChecked(),
                projection.sizeBytes(),
                projection.sha256());
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a JDK-mandatory algorithm (java.security.MessageDigest's own spec) - this
            // can only mean a broken JVM, not a runtime condition this method's caller could
            // recover from.
            throw new IllegalStateException("SHA-256 MessageDigest unavailable", e);
        }
    }
}
