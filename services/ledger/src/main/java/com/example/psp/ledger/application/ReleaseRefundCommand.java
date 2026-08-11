package com.example.psp.ledger.application;

import java.util.UUID;

/**
 * Application-layer input for {@link ReleaseRefundUseCase} - built from the inbound
 * {@code refunds.refund-failed.v1} event. This is the compensation trigger (ADR-0008): the same
 * topic is published to by two different services at two different points in the saga (the
 * ledger itself, on insufficient balance; psp-connector, on a provider decline) - which of those
 * produced any given record is irrelevant here, because the guarded transition in
 * {@code domain.port.RefundRepository#tryRelease} decides purely from the CURRENT saga state, not
 * from anything in the event. See services/ledger/README.md's M11 section.
 *
 * @param inboundEventId the inbound envelope's own {@code eventId} - the dedup key for this
 *                       specific consumption of refunds.refund-failed.v1.
 * @param refundId       the saga's correlation id.
 * @param reason         the failure reason carried on the event ({@code INSUFFICIENT_BALANCE} or
 *                       {@code PROVIDER_DECLINED}) - persisted as the released state's reason.
 * @param traceId        propagated from the inbound envelope onto the outbound
 *                       {@code refunds.reservation-released.v1}, when the transition actually
 *                       releases something.
 * @param correlationId  propagated from the inbound envelope.
 */
public record ReleaseRefundCommand(
        UUID inboundEventId, UUID refundId, String reason, String traceId, String correlationId) {
}
