package com.example.psp.realtimegateway.application;

import com.example.psp.realtimegateway.domain.model.ConsumerGroupSummary;
import com.example.psp.realtimegateway.domain.model.PartitionLag;
import com.example.psp.realtimegateway.domain.model.TopicSummary;
import com.example.psp.realtimegateway.domain.port.ClusterInspector;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * M17 "Cluster ops" (page 5): read-only topic/consumer-group/lag inspection, delegated straight
 * to {@link ClusterInspector} - see that port's javadoc for what each query actually does against
 * the broker and why every one of them needs only {@code Describe}, never {@code Read}.
 *
 * <p>Grouped into one use case, same shape as {@code ManageSubscriptionUseCase} and analytics'
 * {@code QueryWindowMetricsUseCase}: three related read-only queries over one bounded context, not
 * three single-method classes.
 */
@Service
public class InspectClusterUseCase {

    private final ClusterInspector clusterInspector;

    public InspectClusterUseCase(ClusterInspector clusterInspector) {
        this.clusterInspector = clusterInspector;
    }

    public List<TopicSummary> listTopics() {
        return clusterInspector.listTopics();
    }

    public List<ConsumerGroupSummary> listConsumerGroups() {
        return clusterInspector.listConsumerGroups();
    }

    public List<PartitionLag> consumerGroupLag(String groupId) {
        return clusterInspector.consumerGroupLag(groupId);
    }
}
