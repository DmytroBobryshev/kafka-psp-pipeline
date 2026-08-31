package com.example.psp.realtimegateway.adapters.in.web;

import java.util.List;

/**
 * Response body for {@code GET /api/realtime/cluster/groups/{groupId}/lag} (M17 page 5).
 *
 * @param totalLag the sum of every partition's {@code lag} - computed once here so the caller
 *                 does not have to re-sum {@code partitions} client-side.
 */
public record ConsumerGroupLagResponse(String groupId, long totalLag, List<PartitionLagResponse> partitions) {}
