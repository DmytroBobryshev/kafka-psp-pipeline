package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.application.ProjectDisputeCommand;
import com.example.psp.common.events.avro.ClaimCheckReference;
import com.example.psp.common.events.avro.DisputeOpened;
import com.example.psp.common.events.avro.InlineDocument;
import org.springframework.stereotype.Component;

@Component
public class DisputeOpenedMapper {

    public ProjectDisputeCommand toCommand(DisputeOpened event) {
        Object document = event.getDocument();

        if (document instanceof InlineDocument inline) {
            return new ProjectDisputeCommand(
                    event.getDisputeId(),
                    event.getPaymentId(),
                    event.getMerchantId(),
                    event.getReason(),
                    false,
                    inline.getBytes().array(),
                    null,
                    null,
                    inline.getSizeBytes());
        }

        if (document instanceof ClaimCheckReference reference) {
            return new ProjectDisputeCommand(
                    event.getDisputeId(),
                    event.getPaymentId(),
                    event.getMerchantId(),
                    event.getReason(),
                    true,
                    null,
                    reference.getBucket(),
                    reference.getObjectKey(),
                    reference.getSizeBytes());
        }

        throw new IllegalStateException(
                "disputes.dispute-opened.v1 record for disputeId=" + event.getDisputeId()
                        + " carried an unrecognised document type: " + document.getClass());
    }
}
