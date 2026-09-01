package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.AuthorizationLatency;

public interface AuthorizationLatencyProjectionRepository {

    void save(AuthorizationLatency latency);
}
