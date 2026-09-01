package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.DisputeOutcome;
import com.example.psp.paymentapi.application.OpenDisputeCommand;
import java.util.Base64;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class DisputeWebMapper {

    public OpenDisputeCommand toCommand(UUID paymentId, OpenDisputeRequest request) {
        byte[] documentBytes = Base64.getDecoder().decode(request.documentBase64());
        return new OpenDisputeCommand(paymentId, request.reason(), documentBytes, request.contentType());
    }

    public DisputeResponse toResponse(DisputeOutcome outcome) {
        return new DisputeResponse(
                outcome.disputeId(),
                outcome.paymentId(),
                outcome.merchantId(),
                outcome.sizeBytes(),
                outcome.claimChecked(),
                outcome.bucket(),
                outcome.objectKey());
    }
}
