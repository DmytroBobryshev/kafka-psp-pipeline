package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.DisputeProjection;

public interface DisputeProjectionRepository {

    void save(DisputeProjection projection);
}
