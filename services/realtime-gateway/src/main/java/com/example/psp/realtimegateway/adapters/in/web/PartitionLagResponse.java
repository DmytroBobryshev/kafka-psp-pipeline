package com.example.psp.realtimegateway.adapters.in.web;

public record PartitionLagResponse(String topic, int partition, long currentOffset, long endOffset, long lag) {}
