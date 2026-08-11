package com.example.psp.analytics.adapters.in.web;

import com.example.psp.analytics.application.QueryWindowMetricsUseCase;
import com.example.psp.analytics.config.AnalyticsProperties;
import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The interactive-query surface (M10).
 *
 * <p>ADR-0004 says commands enter through REST on payment-api and all inter-service communication
 * is Kafka events. This controller does not contradict it: it accepts no commands and calls no
 * other service. It exposes state this instance already holds in RocksDB to an operator or the
 * M17 dashboard - the one thing Kafka Streams offers that a plain consumer does not.
 *
 * <table border="1">
 *   <caption>Endpoints</caption>
 *   <tr><th>Path</th><th>Source</th><th>Notable</th></tr>
 *   <tr><td>{@code GET /api/analytics/windows}</td><td>window store</td>
 *       <td>every locally-held window; includes the currently OPEN one</td></tr>
 *   <tr><td>{@code GET /api/analytics/merchants/{id}/windows}</td><td>window store</td>
 *       <td>one merchant</td></tr>
 *   <tr><td>{@code GET /api/analytics/merchants/{id}/windows/projected}</td><td>MongoDB</td>
 *       <td>the durable copy; survives retention, restart and a wiped state dir</td></tr>
 *   <tr><td>{@code GET /api/analytics/merchants/{id}/config}</td><td>GlobalKTable</td>
 *       <td>404 after a tombstone</td></tr>
 *   <tr><td>{@code GET /api/analytics/state}</td><td>KafkaStreams client</td>
 *       <td>what the restore proof polls</td></tr>
 * </table>
 *
 * <p><b>503 while restoring.</b> The store-backed endpoints answer {@code 503 Service Unavailable}
 * unless the Streams client is {@code RUNNING}. That is not defensive boilerplate - it is the
 * honest answer during a state restore, and returning an empty list instead would be
 * indistinguishable from "this merchant had no traffic".
 */
@RestController
@RequestMapping("/api/analytics")
@Validated
public class AnalyticsController {

    private static final int MAX_LOOKBACK_MINUTES = 60;

    private final QueryWindowMetricsUseCase queryUseCase;
    private final AnalyticsWebMapper mapper;
    private final AnalyticsProperties properties;

    public AnalyticsController(
            QueryWindowMetricsUseCase queryUseCase,
            AnalyticsWebMapper mapper,
            AnalyticsProperties properties) {
        this.queryUseCase = queryUseCase;
        this.mapper = mapper;
        this.properties = properties;
    }

    /**
     * Every window this instance holds locally. With one instance and 12 input partitions that is
     * every merchant; with two instances it would be roughly half each - see
     * {@code domain.port.WindowMetricsQueryPort} on what makes that transparent.
     */
    @GetMapping("/windows")
    public ResponseEntity<List<WindowMetricsResponse>> windows(
            @RequestParam(name = "lookbackMinutes", defaultValue = "15")
                    @Min(1)
                    @Max(MAX_LOOKBACK_MINUTES)
                    int lookbackMinutes) {

        if (!queryUseCase.stateStoreReady()) {
            return restoring();
        }
        return ResponseEntity.ok(
                toResponses(queryUseCase.liveWindows(Duration.ofMinutes(lookbackMinutes))));
    }

    @GetMapping("/merchants/{merchantId}/windows")
    public ResponseEntity<List<WindowMetricsResponse>> merchantWindows(
            @PathVariable("merchantId") String merchantId,
            @RequestParam(name = "lookbackMinutes", defaultValue = "15")
                    @Min(1)
                    @Max(MAX_LOOKBACK_MINUTES)
                    int lookbackMinutes) {

        if (!queryUseCase.stateStoreReady()) {
            return restoring();
        }
        return ResponseEntity.ok(
                toResponses(queryUseCase.liveWindowsFor(merchantId, Duration.ofMinutes(lookbackMinutes))));
    }

    /**
     * The MongoDB projection, deliberately on its own path rather than as a silent fallback for
     * the live query: a caller asking for live state during a restore should be told the state is
     * not ready, not handed stale numbers that look current.
     */
    @GetMapping("/merchants/{merchantId}/windows/projected")
    public List<WindowMetricsResponse> projectedMerchantWindows(
            @PathVariable("merchantId") String merchantId,
            @RequestParam(name = "lookbackMinutes", defaultValue = "60") @Min(1) @Max(1440) int lookbackMinutes) {

        return toResponses(
                queryUseCase.projectedWindowsFor(merchantId, Duration.ofMinutes(lookbackMinutes)));
    }

    /**
     * The {@code GlobalKTable} lookup. 200 with the snapshot, or <b>404 once a tombstone has been
     * consumed</b> - the observable half of the tombstone proof.
     */
    @GetMapping("/merchants/{merchantId}/config")
    public ResponseEntity<MerchantConfigResponse> merchantConfig(@PathVariable("merchantId") String merchantId) {
        if (!queryUseCase.stateStoreReady()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
        return queryUseCase
                .merchantConfig(merchantId)
                .map(mapper::toResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** Always 200, even while restoring - reporting the restore IS this endpoint's job. */
    @GetMapping("/state")
    public StreamsStateResponse state() {
        return new StreamsStateResponse(
                properties.streams().applicationId(),
                properties.streams().stateDir(),
                queryUseCase.streamsClientState(),
                queryUseCase.stateStoreReady());
    }

    private List<WindowMetricsResponse> toResponses(List<MerchantMetricsWindow> windows) {
        Instant now = Instant.now();
        return windows.stream().map(window -> mapper.toResponse(window, now)).toList();
    }

    private <T> ResponseEntity<T> restoring() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
    }
}
