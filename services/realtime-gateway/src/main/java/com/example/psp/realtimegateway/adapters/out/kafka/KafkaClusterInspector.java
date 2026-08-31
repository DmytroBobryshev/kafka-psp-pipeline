package com.example.psp.realtimegateway.adapters.out.kafka;

import com.example.psp.realtimegateway.domain.exception.ClusterOperationException;
import com.example.psp.realtimegateway.domain.model.ConsumerGroupSummary;
import com.example.psp.realtimegateway.domain.model.PartitionLag;
import com.example.psp.realtimegateway.domain.model.TopicSummary;
import com.example.psp.realtimegateway.domain.port.ClusterInspector;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * Real {@link ClusterInspector} adapter for M17's "Cluster ops" page (page 5), backed by an
 * {@code org.apache.kafka.clients.admin.Admin} AdminClient - see {@code config.ClusterAdminConfig}
 * for how it is built and authenticated.
 *
 * <p>Every request issued here carries {@code config.ClusterAdminConfig}'s
 * {@code ADMIN_REQUEST_TIMEOUT_MS}, and every {@link ExecutionException}/{@link TimeoutException}/
 * interrupt is translated to {@link ClusterOperationException} - {@code domain/} and
 * {@code application/} never see a raw Kafka future failure, and
 * {@code adapters.in.web.ClusterOpsController} turns that into a {@code 502} (this class's javadoc
 * is the mechanical half of that story; the exception's javadoc is the "why 502" half).
 */
@Component
public class KafkaClusterInspector implements ClusterInspector {

    private static final long TIMEOUT_MS = 10_000L;
    private static final String INTERNAL_TOPIC_PREFIX = "__";

    private final Admin admin;

    public KafkaClusterInspector(Admin admin) {
        this.admin = admin;
    }

    @Override
    public List<TopicSummary> listTopics() {
        try {
            ListTopicsOptions listOptions = new ListTopicsOptions().timeoutMs((int) TIMEOUT_MS);
            List<String> externalNames =
                    admin.listTopics(listOptions).names().get(TIMEOUT_MS, TimeUnit.MILLISECONDS).stream()
                            .filter(name -> !name.startsWith(INTERNAL_TOPIC_PREFIX))
                            .sorted()
                            .toList();
            if (externalNames.isEmpty()) {
                return List.of();
            }

            DescribeTopicsOptions describeOptions = new DescribeTopicsOptions().timeoutMs((int) TIMEOUT_MS);
            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(externalNames, describeOptions)
                            .allTopicNames()
                            .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            return descriptions.values().stream()
                    .map(KafkaClusterInspector::toTopicSummary)
                    .sorted(Comparator.comparing(TopicSummary::name))
                    .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClusterOperationException("listTopics interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new ClusterOperationException("listTopics failed against the Kafka cluster", e);
        }
    }

    private static TopicSummary toTopicSummary(TopicDescription description) {
        int partitionCount = description.partitions().size();
        // Every partition of a healthy topic carries the same replication factor - the first
        // partition's replica count is representative, and partitions() is never empty for a
        // topic that describeTopics actually returned.
        int replicationFactor =
                description.partitions().isEmpty() ? 0 : description.partitions().get(0).replicas().size();
        return new TopicSummary(description.name(), partitionCount, replicationFactor);
    }

    @Override
    public List<ConsumerGroupSummary> listConsumerGroups() {
        try {
            Collection<ConsumerGroupListing> listings =
                    admin.listConsumerGroups(new ListConsumerGroupsOptions().timeoutMs((int) TIMEOUT_MS))
                            .valid()
                            .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            List<String> groupIds = listings.stream().map(ConsumerGroupListing::groupId).sorted().toList();
            if (groupIds.isEmpty()) {
                return List.of();
            }

            Map<String, ConsumerGroupDescription> descriptions =
                    admin.describeConsumerGroups(
                                    groupIds, new DescribeConsumerGroupsOptions().timeoutMs((int) TIMEOUT_MS))
                            .all()
                            .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            return groupIds.stream()
                    .map(descriptions::get)
                    .filter(Objects::nonNull)
                    .map(d -> new ConsumerGroupSummary(d.groupId(), d.state().toString(), d.members().size()))
                    .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClusterOperationException("listConsumerGroups interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new ClusterOperationException("listConsumerGroups failed against the Kafka cluster", e);
        }
    }

    @Override
    public List<PartitionLag> consumerGroupLag(String groupId) {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(
                                    groupId, new ListConsumerGroupOffsetsOptions().timeoutMs((int) TIMEOUT_MS))
                            .partitionsToOffsetAndMetadata()
                            .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (committed.isEmpty()) {
                return List.of();
            }

            Map<TopicPartition, OffsetSpec> endOffsetSpecs =
                    committed.keySet().stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(endOffsetSpecs, new ListOffsetsOptions().timeoutMs((int) TIMEOUT_MS))
                            .all()
                            .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<PartitionLag> lags = new ArrayList<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                TopicPartition partition = entry.getKey();
                long currentOffset = entry.getValue() == null ? 0L : entry.getValue().offset();
                ListOffsetsResult.ListOffsetsResultInfo endInfo = endOffsets.get(partition);
                long endOffset = endInfo == null ? currentOffset : endInfo.offset();
                long lag = Math.max(0L, endOffset - currentOffset);
                lags.add(new PartitionLag(partition.topic(), partition.partition(), currentOffset, endOffset, lag));
            }
            lags.sort(Comparator.comparing(PartitionLag::topic).thenComparingInt(PartitionLag::partition));
            return lags;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClusterOperationException("consumerGroupLag interrupted for groupId=" + groupId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new ClusterOperationException(
                    "consumerGroupLag failed against the Kafka cluster for groupId=" + groupId, e);
        }
    }
}
