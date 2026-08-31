package com.example.psp.realtimegateway.domain.exception;

/**
 * Thrown when a call to the Kafka broker on behalf of M17's cluster-ops endpoints
 * ({@code adapters.out.kafka.KafkaClusterInspector}, {@code adapters.out.kafka.KafkaDlqBrowser})
 * times out or otherwise fails. Same category as payment-api's
 * {@code ProviderStatusTimeoutException} - a real network call to a dependency that did not
 * answer as expected, not a bug in this service.
 *
 * <p>{@code adapters.in.web.ClusterOpsController} maps this to {@code 502 Bad Gateway}: the
 * server did nothing wrong, the Kafka cluster (or the AdminClient's bounded wait for it) did.
 */
public class ClusterOperationException extends RuntimeException {

    public ClusterOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
