package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.MerchantConfigSnapshot;
import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.port.MetricsProjectionRepository;
import com.example.psp.analytics.domain.port.WindowMetricsQueryPort;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;

/**
 * The read side (M10): interactive queries against the live state store, with the MongoDB
 * projection as the fallback.
 *
 * <p>Two sources, and the difference between them is the module's point:
 *
 * <table border="1">
 *   <caption>State store vs projection</caption>
 *   <tr><th></th><th>Interactive query</th><th>Mongo projection</th></tr>
 *   <tr><td>Sees the still-open window</td><td>Yes</td><td>Only once flushed downstream</td></tr>
 *   <tr><td>Survives process restart</td><td>Only after restore completes</td><td>Yes</td></tr>
 *   <tr><td>Survives a wiped state.dir</td><td>Yes, by replaying the changelog</td><td>Yes</td></tr>
 *   <tr><td>Survives store retention expiry</td><td>No</td><td>Yes</td></tr>
 *   <tr><td>Covers other instances' partitions</td><td>No</td><td>Yes</td></tr>
 * </table>
 *
 * <p>{@link #liveWindowsFor} answers from the store and never silently substitutes the
 * projection: a caller asking for live state during a restore should learn that the state is not
 * ready ({@code 503}), not receive stale numbers that look current. {@link #projectedWindowsFor}
 * is the explicit, separate endpoint for the durable copy.
 */
@Service
public class QueryWindowMetricsUseCase {

    private final WindowMetricsQueryPort queryPort;
    private final MetricsProjectionRepository projectionRepository;

    public QueryWindowMetricsUseCase(
            WindowMetricsQueryPort queryPort, MetricsProjectionRepository projectionRepository) {
        this.queryPort = queryPort;
        this.projectionRepository = projectionRepository;
    }

    /** {@code false} while the Streams client is starting, rebalancing or restoring. */
    public boolean stateStoreReady() {
        return queryPort.storeReady();
    }

    /** {@code CREATED} / {@code REBALANCING} / {@code RUNNING} / ... - see the port's javadoc. */
    public String streamsClientState() {
        return queryPort.clientState();
    }

    public List<MerchantMetricsWindow> liveWindowsFor(String merchantId, Duration lookback) {
        Instant now = Instant.now();
        return queryPort.windowsFor(merchantId, now.minus(lookback), now);
    }

    public List<MerchantMetricsWindow> liveWindows(Duration lookback) {
        Instant now = Instant.now();
        return queryPort.allWindows(now.minus(lookback), now);
    }

    public List<MerchantMetricsWindow> projectedWindowsFor(String merchantId, Duration lookback) {
        return projectionRepository.findByMerchantSince(merchantId, Instant.now().minus(lookback));
    }

    /**
     * The {@code GlobalKTable} lookup, exposed so the tombstone proof has something to assert on:
     * present before {@code DELETE /api/merchants/{id}/config}, empty after.
     */
    public Optional<MerchantConfigSnapshot> merchantConfig(String merchantId) {
        return queryPort.merchantConfig(merchantId);
    }
}
