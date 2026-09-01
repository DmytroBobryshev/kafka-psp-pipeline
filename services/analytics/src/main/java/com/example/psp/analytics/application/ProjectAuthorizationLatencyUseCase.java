package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.AuthorizationLatency;
import com.example.psp.analytics.domain.port.AuthorizationLatencyProjectionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProjectAuthorizationLatencyUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProjectAuthorizationLatencyUseCase.class);

    private final AuthorizationLatencyProjectionRepository repository;

    public ProjectAuthorizationLatencyUseCase(AuthorizationLatencyProjectionRepository repository) {
        this.repository = repository;
    }

    public void project(AuthorizationLatency latency) {
        repository.save(latency);

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
