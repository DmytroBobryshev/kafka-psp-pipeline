package com.example.psp.realtimegateway.adapters.in.web;

/** Response body for one row of {@code GET /api/realtime/cluster/topics} (M17 page 5). */
public record ClusterTopicResponse(String name, int partitionCount, int replicationFactor) {}
