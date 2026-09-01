package com.example.psp.realtimegateway.domain.model;

public record PartitionLag(String topic, int partition, long currentOffset, long endOffset, long lag) {}
