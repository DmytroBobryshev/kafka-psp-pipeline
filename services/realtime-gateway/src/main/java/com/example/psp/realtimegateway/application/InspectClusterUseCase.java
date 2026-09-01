package com.example.psp.realtimegateway.application;

import com.example.psp.realtimegateway.domain.model.ConsumerGroupSummary;
import com.example.psp.realtimegateway.domain.model.PartitionLag;
import com.example.psp.realtimegateway.domain.model.TopicSummary;
import com.example.psp.realtimegateway.domain.port.ClusterInspector;
import java.util.List;
import org.springframework.stereotype.Service;

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
