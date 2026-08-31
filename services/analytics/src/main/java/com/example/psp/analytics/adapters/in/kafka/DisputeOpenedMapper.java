package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.application.ProjectDisputeCommand;
import com.example.psp.common.events.avro.ClaimCheckReference;
import com.example.psp.common.events.avro.DisputeOpened;
import com.example.psp.common.events.avro.InlineDocument;
import org.springframework.stereotype.Component;

/**
 * THE dereference mapper (M13): where the Avro {@code document} union stops being a wire
 * contract and becomes plain Java. ArchUnit's {@code onlyTheTopologyMayDependOnGeneratedAvro}
 * rule confines every {@code com.example.psp.common.events.avro..} import to {@code adapters}/
 * {@code config} - this class, and only this class, is where {@code application.
 * ProjectDisputeUseCase} would otherwise have had to know an Avro union exists.
 *
 * <p>A plain {@code @Component}, not a MapStruct {@code @Mapper}: the union field is generated as
 * {@code Object} (see {@code DisputeOpened.Builder#setDocument}), so the branch has to be picked
 * with an {@code instanceof} check MapStruct has no builtin for - the same "not worth an
 * annotation processor for one conversion" call {@code payment-api}'s hand-written Avro-factory
 * classes make.
 */
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

        // Avro's union validates on the wire - a DisputeOpened record cannot deserialize with a
        // document that is neither branch. This is defensive, not a reachable production path.
        throw new IllegalStateException(
                "disputes.dispute-opened.v1 record for disputeId=" + event.getDisputeId()
                        + " carried an unrecognised document type: " + document.getClass());
    }
}
