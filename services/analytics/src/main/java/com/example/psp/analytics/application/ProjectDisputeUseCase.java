package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.DisputeProjection;
import com.example.psp.analytics.domain.port.DisputeDocumentFetcher;
import com.example.psp.analytics.domain.port.DisputeProjectionRepository;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
            throw new IllegalStateException("SHA-256 MessageDigest unavailable", e);
        }
    }
}
