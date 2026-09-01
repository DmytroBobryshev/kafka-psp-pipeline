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

    @GetMapping("/merchants/{merchantId}/windows/projected")
    public List<WindowMetricsResponse> projectedMerchantWindows(
            @PathVariable("merchantId") String merchantId,
            @RequestParam(name = "lookbackMinutes", defaultValue = "60") @Min(1) @Max(1440) int lookbackMinutes) {

        return toResponses(
                queryUseCase.projectedWindowsFor(merchantId, Duration.ofMinutes(lookbackMinutes)));
    }

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
