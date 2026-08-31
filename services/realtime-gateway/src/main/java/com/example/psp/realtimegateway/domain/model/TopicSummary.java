package com.example.psp.realtimegateway.domain.model;

/**
 * One row of M17's "Cluster ops" topic list (page 5) -
 * {@code domain.port.ClusterInspector#listTopics}. Internal topics ({@code __*}, e.g.
 * {@code __consumer_offsets}) are filtered out before this record is ever built - see that port's
 * javadoc and {@code adapters.out.kafka.KafkaClusterInspector}.
 */
public record TopicSummary(String name, int partitionCount, int replicationFactor) {}
