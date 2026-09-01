package com.example.psp.realtimegateway.adapters.in.web;

import java.util.List;

public record ConsumerGroupLagResponse(String groupId, long totalLag, List<PartitionLagResponse> partitions) {}
