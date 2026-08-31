package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.DisputeOutcome;
import com.example.psp.paymentapi.application.OpenDisputeCommand;
import java.util.Base64;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Web-boundary mapper for the dispute hexagon (M13). A plain {@code @Component}, not a MapStruct
 * {@code @Mapper}: the only real conversion here is base64 decode, which MapStruct has no builtin
 * for and would need a custom method registered anyway - the same "not worth an annotation
 * processor for one conversion" call {@code adapters.out.outbox.PaymentAvroEventFactory}'s
 * javadoc makes for its UUID&lt;-&gt;String conversion.
 *
 * <p>Base64 decoding happens HERE, at the inbound web adapter, and nowhere else - by the time a
 * value reaches {@code application.OpenDisputeCommand} it is raw bytes; the application and
 * domain layers have no idea the wire form was ever text.
 */
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
