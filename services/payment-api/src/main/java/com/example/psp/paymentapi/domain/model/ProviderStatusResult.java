package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.UUID;

/**
 * The answer to "what is the provider status of this payment, right now?" (M12 request-reply,
 * ADR-0004's synchronous-need carve-out). Pure Java, no framework dependency (ADR-0007) - built by
 * {@code adapters.out.kafka.ProviderStatusRequestGateway} from the Avro
 * {@code ProviderStatusReply} it receives over {@code psp.provider-status-reply.v1}, so that
 * {@code domain/} and {@code application/} never see an Avro type (same rule every other hexagon
 * boundary in this codebase enforces).
 *
 * @param paymentId         the payment that was asked about.
 * @param merchantId         the owning merchant.
 * @param found              whether psp-connector held any attempt record for this payment at the
 *                           moment it answered - {@code false} does not mean the payment failed,
 *                           it means no provider call has been recorded yet.
 * @param status             the latest {@code ProviderOutcome} name (APPROVED / DECLINED /
 *                           TIMEOUT), or {@code null} when {@code found} is {@code false}.
 * @param providerReference the provider's own reference for the latest attempt, or {@code null}
 *                           when {@code found} is {@code false}.
 * @param checkedAt          when psp-connector performed the lookup.
 * @param roundTripMillis    wall-clock time this gateway measured between sending the query and
 *                           receiving the reply - the observable latency of the whole
 *                           request-reply round trip, not just psp-connector's own processing time.
 */
public record ProviderStatusResult(
        UUID paymentId,
        String merchantId,
        boolean found,
        String status,
        String providerReference,
        Instant checkedAt,
        long roundTripMillis) {
}
