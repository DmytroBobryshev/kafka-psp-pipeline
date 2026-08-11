package com.example.psp.webhooknotifier.domain.model;

/**
 * The result of one attempt to call a merchant's webhook endpoint, classified per ADR-0006 as
 * applied to an <b>outbound HTTP call</b> rather than a Kafka listener - see this package's
 * {@code RetryChain} javadoc for why the same taxonomy still applies.
 *
 * <ul>
 *   <li>{@link #SUCCESS} - 2xx response. Terminal; nothing more to do.
 *   <li>{@link #RETRYABLE_FAILURE} - ADR-0006 category A: merchant 5xx, connection reset, or a
 *       client-side read/connect timeout. The merchant endpoint (or the network) is having a bad
 *       moment; the same request will very plausibly succeed on retry. Routed through the
 *       {@code RetryChain}.
 *   <li>{@link #NON_RETRYABLE_FAILURE} - the outbound-HTTP analogue of ADR-0006 category C
 *       (contract violation / poison pill): a merchant 4xx. The request itself is rejected -
 *       bad payload, unknown merchant, endpoint gone (410) - and retrying an unchanged request
 *       against an unchanged endpoint will not produce a different answer, exactly as "the bytes
 *       will not improve" for a deserialization failure. Routed straight to the DLQ, never
 *       through a retry topic.
 * </ul>
 *
 * <p>There is deliberately no "business outcome" (ADR-0006 category B) value here: unlike
 * psp-connector's {@code ProviderOutcome} (where a card decline is a normal answer the domain
 * must see), a webhook delivery has no domain-meaningful "the merchant said no" outcome - the
 * merchant's HTTP response is purely acknowledgement-or-not of a notification. Category D
 * (unknown/bug) is not a value here either: an exception this adapter cannot classify (a
 * malformed base-url, an NPE) is left to propagate out of the listener uncaught, exactly like
 * psp-connector's {@code ProviderTimeoutException} does for category A - see
 * {@code adapters.out.http.RestClientMerchantWebhookClient} and
 * {@code config.KafkaConsumerConfig}'s container-level {@code DeadLetterPublishingRecoverer} for
 * where that unclassified case is actually handled (straight to DLQ, zero retries).
 */
public enum DeliveryOutcome {
    SUCCESS,
    RETRYABLE_FAILURE,
    NON_RETRYABLE_FAILURE
}
