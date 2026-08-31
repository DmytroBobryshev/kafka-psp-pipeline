package com.example.psp.realtimegateway.domain.model;

/**
 * One row of M17's "Cluster ops" consumer-group list (page 5) -
 * {@code domain.port.ClusterInspector#listConsumerGroups}.
 *
 * <p>{@code state} is Kafka's own group state rendered as a string ({@code STABLE},
 * {@code EMPTY}, {@code DEAD}, ...) rather than re-modelled as an enum here - {@code domain/} must
 * not depend on Kafka (ADR-0007), and the raw broker string is exactly what an operator wants to
 * read on the M17 dashboard.
 */
public record ConsumerGroupSummary(String groupId, String state, int memberCount) {}
