package com.example.psp.realtimegateway.domain.port;

import com.example.psp.realtimegateway.domain.model.ConsumerGroupSummary;
import com.example.psp.realtimegateway.domain.model.PartitionLag;
import com.example.psp.realtimegateway.domain.model.TopicSummary;
import java.util.List;

public interface ClusterInspector {

    List<TopicSummary> listTopics();

    List<ConsumerGroupSummary> listConsumerGroups();

    List<PartitionLag> consumerGroupLag(String groupId);
}
