package com.example.psp.realtimegateway.adapters.in.web;

import com.example.psp.realtimegateway.application.BrowseDlqUseCase;
import com.example.psp.realtimegateway.application.InspectClusterUseCase;
import com.example.psp.realtimegateway.domain.exception.ClusterOperationException;
import com.example.psp.realtimegateway.domain.model.ConsumerGroupSummary;
import com.example.psp.realtimegateway.domain.model.DlqRecordView;
import com.example.psp.realtimegateway.domain.model.PartitionLag;
import com.example.psp.realtimegateway.domain.model.TopicSummary;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.List;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * M17's cluster-ops API: page 5's "Cluster ops" (topics/groups/lag) and page 3's generic DLQ
 * browse, all under {@code /api/realtime/cluster/**} so api-gateway's existing
 * {@code /api/realtime/**} route needs no changes at all.
 *
 * <p>Deliberately thin, same shape as every other controller in this codebase - the interesting
 * work (AdminClient calls, offset arithmetic, UTF-8-vs-Base64 detection) lives in
 * {@code application/} and {@code adapters.out.kafka}; this class only maps ports to paths and
 * domain records to response DTOs.
 *
 * <table border="1">
 *   <caption>Endpoints</caption>
 *   <tr><th>Path</th><th>Backed by</th></tr>
 *   <tr><td>{@code GET /topics}</td><td>{@code Admin#listTopics}+{@code #describeTopics}</td></tr>
 *   <tr><td>{@code GET /groups}</td><td>{@code Admin#listConsumerGroups}+{@code #describeConsumerGroups}</td></tr>
 *   <tr><td>{@code GET /groups/{groupId}/lag}</td><td>{@code Admin#listConsumerGroupOffsets}+{@code #listOffsets}</td></tr>
 *   <tr><td>{@code GET /dlq/{topic}/records?max=20}</td><td>a short-lived, group-less {@code KafkaConsumer} (non-destructive)</td></tr>
 * </table>
 */
@RestController
@RequestMapping("/api/realtime/cluster")
public class ClusterOpsController {

    private static final URI CLUSTER_OPERATION_FAILED_TYPE =
            URI.create("https://psp.example.com/problems/cluster-operation-failed");

    private final InspectClusterUseCase inspectClusterUseCase;
    private final BrowseDlqUseCase browseDlqUseCase;

    public ClusterOpsController(InspectClusterUseCase inspectClusterUseCase, BrowseDlqUseCase browseDlqUseCase) {
        this.inspectClusterUseCase = inspectClusterUseCase;
        this.browseDlqUseCase = browseDlqUseCase;
    }

    @GetMapping("/topics")
    public List<ClusterTopicResponse> topics() {
        return inspectClusterUseCase.listTopics().stream().map(ClusterOpsController::toResponse).toList();
    }

    @GetMapping("/groups")
    public List<ConsumerGroupResponse> groups() {
        return inspectClusterUseCase.listConsumerGroups().stream().map(ClusterOpsController::toResponse).toList();
    }

    @GetMapping("/groups/{groupId}/lag")
    public ConsumerGroupLagResponse groupLag(@PathVariable("groupId") String groupId) {
        List<PartitionLag> partitionLags = inspectClusterUseCase.consumerGroupLag(groupId);
        List<PartitionLagResponse> partitions =
                partitionLags.stream().map(ClusterOpsController::toResponse).toList();
        long totalLag = partitionLags.stream().mapToLong(PartitionLag::lag).sum();
        return new ConsumerGroupLagResponse(groupId, totalLag, partitions);
    }

    /**
     * {@code max} defaults to 20 (the M17 spec's own example) and is clamped by
     * {@code application.BrowseDlqUseCase} regardless of what is requested here - see that class
     * for the ceiling. A {@code topic} not ending in {@code .dlq} is rejected there too, as an
     * {@link IllegalArgumentException} that {@code common-web.GlobalExceptionHandler} turns into a
     * plain {@code 400}.
     */
    @GetMapping("/dlq/{topic}/records")
    public DlqRecordsResponse dlqRecords(
            @PathVariable("topic") String topic, @RequestParam(name = "max", defaultValue = "20") int max) {
        List<DlqRecordView> records = browseDlqUseCase.peekLast(topic, max);
        List<DlqRecordResponse> responses = records.stream().map(ClusterOpsController::toResponse).toList();
        return new DlqRecordsResponse(topic, responses.size(), responses);
    }

    /**
     * {@link ClusterOperationException} maps to {@code 502 Bad Gateway}, not {@code 500}: this
     * service did nothing wrong, the Kafka cluster (or the bounded wait for it,
     * {@code config.ClusterAdminConfig}'s 10s AdminClient timeout) did not answer as expected -
     * same distinction payment-api's {@code ProviderStatusController#handleTimeout} draws for its
     * own downstream-timeout exception, applied to this service's one downstream dependency.
     */
    @ExceptionHandler(ClusterOperationException.class)
    public ProblemDetail handleClusterOperationFailure(ClusterOperationException ex, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, ex.getMessage());
        problem.setTitle("Cluster operation failed");
        problem.setType(CLUSTER_OPERATION_FAILED_TYPE);
        problem.setInstance(URI.create(request.getRequestURI()));
        String correlationId = MDC.get("correlationId");
        if (correlationId != null) {
            problem.setProperty("correlationId", correlationId);
        }
        return problem;
    }

    private static ClusterTopicResponse toResponse(TopicSummary summary) {
        return new ClusterTopicResponse(summary.name(), summary.partitionCount(), summary.replicationFactor());
    }

    private static ConsumerGroupResponse toResponse(ConsumerGroupSummary summary) {
        return new ConsumerGroupResponse(summary.groupId(), summary.state(), summary.memberCount());
    }

    private static PartitionLagResponse toResponse(PartitionLag lag) {
        return new PartitionLagResponse(
                lag.topic(), lag.partition(), lag.currentOffset(), lag.endOffset(), lag.lag());
    }

    private static DlqRecordResponse toResponse(DlqRecordView view) {
        return new DlqRecordResponse(
                view.topic(),
                view.partition(),
                view.offset(),
                view.timestamp(),
                view.keyString(),
                view.headers(),
                view.valuePreview(),
                view.valueBase64());
    }
}
