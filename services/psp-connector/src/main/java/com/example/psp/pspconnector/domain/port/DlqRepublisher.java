package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.DlqRecord;

/**
 * Outbound port for republishing one DLQ record, byte-for-byte unchanged, back onto
 * {@code payments.payment-requested.v1}. Implemented by
 * {@code adapters.out.kafka.KafkaDlqRepublisher}.
 *
 * <p>Safe to call even for a record that has already been successfully processed some other way:
 * M5's level 1 dedup ({@code application.ProcessPaymentRequestUseCase}, keyed on the inbound
 * {@code EventEnvelope.eventId}) makes a replay of an already-processed record a no-op on the
 * consuming side - it republishes the stored status event and never re-authorizes the payment
 * (see that use case's javadoc, "LEVEL 1"). A record that genuinely was never successfully
 * processed - the actual reason it is in the DLQ - is instead processed fresh, exactly as if it had
 * just arrived on {@code payments.payment-requested.v1} for the first time.
 */
public interface DlqRepublisher {

    void republish(DlqRecord record);
}
