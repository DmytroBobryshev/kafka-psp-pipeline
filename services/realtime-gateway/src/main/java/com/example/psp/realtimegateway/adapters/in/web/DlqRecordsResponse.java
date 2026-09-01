package com.example.psp.realtimegateway.adapters.in.web;

import java.util.List;

public record DlqRecordsResponse(String topic, int count, List<DlqRecordResponse> records) {}
