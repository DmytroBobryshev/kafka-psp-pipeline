package com.example.psp.realtimegateway.adapters.in.web;

import java.time.Instant;
import java.util.Map;

/**
 * One record inside {@link DlqRecordsResponse} - the wire shape of
 * {@code domain.model.DlqRecordView} (M17 page 3's generic DLQ browse). See that record's javadoc
 * for what {@code valuePreview}/{@code valueBase64} mean and why {@code keyString} and
 * {@code headers} are decoded leniently while the value is not.
 */
public record DlqRecordResponse(
        String topic,
        int partition,
        long offset,
        Instant timestamp,
        String keyString,
        Map<String, String> headers,
        String valuePreview,
        boolean valueBase64) {}
