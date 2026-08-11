package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.AuthorizationLatency;
import com.example.psp.analytics.domain.port.AuthorizationLatencyProjectionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Persists one authorization-latency result to MongoDB (M13). Called from the M13 join's
 * terminal {@code foreach} in {@code adapters.in.kafka.AnalyticsTopology}, once per matched
 * (payment-requested, payment-status-changed) pair - unlike M10's windowed projection, there is
 * no record cache smoothing this: a stream-stream join emits once per match, not once per commit
 * interval, so call volume here is exactly the payment volume, not a collapsed rate.
 *
 * <p>{@code application/} depends on the port only (ADR-0007), and stays free of both Kafka and
 * Mongo types - the same discipline {@link ProjectWindowMetricsUseCase} follows for M10.
 */
@Service
public class ProjectAuthorizationLatencyUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProjectAuthorizationLatencyUseCase.class);

    private final AuthorizationLatencyProjectionRepository repository;

    public ProjectAuthorizationLatencyUseCase(AuthorizationLatencyProjectionRepository repository) {
        this.repository = repository;
    }

    public void project(AuthorizationLatency latency) {
        repository.save(latency);

        // INFO, not DEBUG (unlike M10's projection log line): this is the one real,
        // human-checkable proof that the join fired, and it is genuinely low-volume (once per
        // decided payment, not once per commit interval), so it is not log spam.
        log.info(
                "Authorization latency paymentId={} merchantId={} providerReference={} status={} "
                        + "requestedAt={} decidedAt={} latencyMillis={}",
                latency.paymentId(),
                latency.merchantId(),
                latency.providerReference(),
                latency.status(),
                latency.requestedAt(),
                latency.decidedAt(),
                latency.latencyMillis());
    }
}
