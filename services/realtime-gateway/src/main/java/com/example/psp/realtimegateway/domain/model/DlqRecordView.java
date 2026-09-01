package com.example.psp.realtimegateway.domain.model;

import java.time.Instant;
import java.util.Map;

public record DlqRecordView(
        String topic,
        int partition,
        long offset,
        Instant timestamp,
        String keyString,
        Map<String, String> headers,
        String valuePreview,
        boolean valueBase64) {}
