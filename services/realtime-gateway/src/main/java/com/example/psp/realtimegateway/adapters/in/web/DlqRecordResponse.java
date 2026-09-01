package com.example.psp.realtimegateway.adapters.in.web;

import java.time.Instant;
import java.util.Map;

public record DlqRecordResponse(
        String topic,
        int partition,
        long offset,
        Instant timestamp,
        String keyString,
        Map<String, String> headers,
        String valuePreview,
        boolean valueBase64) {}
