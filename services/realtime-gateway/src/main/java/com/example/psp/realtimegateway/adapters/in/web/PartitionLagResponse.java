package com.example.psp.realtimegateway.adapters.in.web;

/** One partition row inside {@link ConsumerGroupLagResponse}. */
public record PartitionLagResponse(String topic, int partition, long currentOffset, long endOffset, long lag) {}
