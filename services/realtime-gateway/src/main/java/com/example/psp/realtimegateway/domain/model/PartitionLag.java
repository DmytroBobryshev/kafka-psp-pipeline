package com.example.psp.realtimegateway.domain.model;

/**
 * One partition's row of M17's per-group lag view (page 5) -
 * {@code domain.port.ClusterInspector#consumerGroupLag}.
 *
 * @param currentOffset the group's last committed offset for this partition.
 * @param endOffset     the partition's current high-water (end) offset.
 * @param lag           {@code endOffset - currentOffset}, floored at zero - a committed offset can
 *                       momentarily read past the end offset around a leader change or a group
 *                       that just reset forward, and this never reports negative lag for that.
 */
public record PartitionLag(String topic, int partition, long currentOffset, long endOffset, long lag) {}
