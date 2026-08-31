package com.example.psp.realtimegateway.domain.port;

import com.example.psp.realtimegateway.domain.model.ConsumerGroupSummary;
import com.example.psp.realtimegateway.domain.model.PartitionLag;
import com.example.psp.realtimegateway.domain.model.TopicSummary;
import java.util.List;

/**
 * Outbound port for M17's "Cluster ops" page (page 5): read-only broker metadata, backed by
 * {@code adapters.out.kafka.KafkaClusterInspector} (an
 * {@code org.apache.kafka.clients.admin.Admin} AdminClient) - {@code domain/} never imports that
 * type directly (ADR-0007).
 *
 * <p>Every method here is satisfied by Kafka's {@code Describe} ACL operation alone - none of
 * them read a single record's payload, only metadata (topic/partition layout, group membership,
 * committed and end offsets). That is exactly the grant
 * {@code infra/k8s/kafka/users/15-realtime-gateway.yaml} adds for this port: {@code Describe} on
 * every topic and every consumer group, nothing more. {@code domain.port.DlqBrowser} is the one
 * port in this service that needs {@code Read}.
 */
public interface ClusterInspector {

    /** Every non-internal topic ({@code __*} excluded), with its partition count and replication factor. */
    List<TopicSummary> listTopics();

    /** Every consumer group visible to this principal, with its state and current member count. */
    List<ConsumerGroupSummary> listConsumerGroups();

    /**
     * Per-partition lag for {@code groupId}: the group's last committed offset vs. each assigned
     * topic-partition's current end (high-water) offset. Returns an empty list for a group with no
     * committed offsets - including one that has never existed - rather than throwing; Kafka's own
     * {@code listConsumerGroupOffsets} makes no distinction between the two cases, so neither does
     * this port.
     */
    List<PartitionLag> consumerGroupLag(String groupId);
}
