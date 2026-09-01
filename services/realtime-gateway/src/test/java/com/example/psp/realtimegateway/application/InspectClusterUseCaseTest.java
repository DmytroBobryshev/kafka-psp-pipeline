package com.example.psp.realtimegateway.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.realtimegateway.domain.model.ConsumerGroupSummary;
import com.example.psp.realtimegateway.domain.model.PartitionLag;
import com.example.psp.realtimegateway.domain.model.TopicSummary;
import com.example.psp.realtimegateway.domain.port.ClusterInspector;
import java.util.List;
import org.junit.jupiter.api.Test;

class InspectClusterUseCaseTest {

    @Test
    void listTopicsReturnsWhateverThePortReturns() {
        List<TopicSummary> topics =
                List.of(new TopicSummary("payments.payment-requested.v1", 6, 3), new TopicSummary("refunds.refund-requested.v1", 6, 3));
        InspectClusterUseCase useCase = new InspectClusterUseCase(new FakeClusterInspector(topics, List.of(), List.of()));

        assertThat(useCase.listTopics()).isEqualTo(topics);
    }

    @Test
    void listConsumerGroupsReturnsWhateverThePortReturns() {
        List<ConsumerGroupSummary> groups =
                List.of(new ConsumerGroupSummary("psp-connector", "STABLE", 2));
        InspectClusterUseCase useCase = new InspectClusterUseCase(new FakeClusterInspector(List.of(), groups, List.of()));

        assertThat(useCase.listConsumerGroups()).isEqualTo(groups);
    }

    @Test
    void consumerGroupLagPassesTheGroupIdThroughAndReturnsThePortsResult() {
        List<PartitionLag> lag = List.of(new PartitionLag("payments.payment-status-changed.v1", 0, 10L, 15L, 5L));
        FakeClusterInspector inspector = new FakeClusterInspector(List.of(), List.of(), lag);
        InspectClusterUseCase useCase = new InspectClusterUseCase(inspector);

        List<PartitionLag> result = useCase.consumerGroupLag("ledger.v1.some-instance");

        assertThat(result).isEqualTo(lag);
        assertThat(inspector.lastRequestedGroupId).isEqualTo("ledger.v1.some-instance");
    }

    private static final class FakeClusterInspector implements ClusterInspector {
        private final List<TopicSummary> topics;
        private final List<ConsumerGroupSummary> groups;
        private final List<PartitionLag> lag;
        private String lastRequestedGroupId;

        private FakeClusterInspector(
                List<TopicSummary> topics, List<ConsumerGroupSummary> groups, List<PartitionLag> lag) {
            this.topics = topics;
            this.groups = groups;
            this.lag = lag;
        }

        @Override
        public List<TopicSummary> listTopics() {
            return topics;
        }

        @Override
        public List<ConsumerGroupSummary> listConsumerGroups() {
            return groups;
        }

        @Override
        public List<PartitionLag> consumerGroupLag(String groupId) {
            lastRequestedGroupId = groupId;
            return lag;
        }
    }
}
