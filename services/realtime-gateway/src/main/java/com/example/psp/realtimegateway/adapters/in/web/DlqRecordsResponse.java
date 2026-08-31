package com.example.psp.realtimegateway.adapters.in.web;

import java.util.List;

/**
 * Response body for {@code GET /api/realtime/cluster/dlq/{topic}/records} (M17 page 3).
 *
 * @param count {@code records.size()} - echoed so a caller can tell "fewer than max exist" apart
 *              from "the response got truncated somewhere", without counting the array itself.
 */
public record DlqRecordsResponse(String topic, int count, List<DlqRecordResponse> records) {}
