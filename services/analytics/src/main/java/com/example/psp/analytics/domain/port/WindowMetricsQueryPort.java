package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.MerchantConfigSnapshot;
import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Outbound port for <b>interactive queries</b> - reading the live Kafka Streams state stores
 * directly, with no database in the path (M10).
 *
 * <p>This is the capability that distinguishes Streams from a plain consumer: the aggregation's
 * working state is not an internal detail, it is a queryable, embedded key/value store. A read
 * through this port sees the <i>current, still-open</i> 1-minute window - a value that does not
 * exist anywhere else yet, because it has not been emitted downstream and has not reached the
 * Mongo projection.
 *
 * <p><b>Local means local.</b> Every implementation of this port answers from the partitions
 * assigned to <i>this</i> instance. With one instance and 12 input partitions that is everything;
 * with two instances each would answer for roughly half the merchants and return nothing for the
 * rest. Making that transparent needs {@code KafkaStreams#queryMetadataForKey} plus an RPC hop to
 * the owning instance, which is what the {@code application.server} config exists to advertise -
 * it is set (see {@code application.yml}) but the RPC hop itself is deferred, and the deferral is
 * recorded in the README's "Compromises".
 *
 * <p>Reads may also legitimately fail while the instance is {@code REBALANCING} or restoring
 * state from the changelog - hence {@link #storeReady()}, which the REST layer uses to answer
 * {@code 503} instead of throwing.
 */
public interface WindowMetricsQueryPort {

    /** True when the Streams client is {@code RUNNING} and its local stores can be queried. */
    boolean storeReady();

    /**
     * The Streams client's lifecycle state as a plain string ({@code CREATED},
     * {@code REBALANCING}, {@code RUNNING}, {@code ERROR}, ...), or {@code NOT_STARTED}.
     *
     * <p>A {@code String} rather than the Kafka {@code KafkaStreams.State} enum on purpose: this
     * is a domain port, and ADR-0007 keeps {@code org.apache.kafka} out of {@code domain/}. The
     * REST layer surfaces it because {@code REBALANCING} is exactly what an operator sees for the
     * duration of a state restore, and "why is my query 503-ing" has a one-word answer here.
     */
    String clientState();

    /** Windows for one merchant whose start is in {@code [from, to)}, oldest first. */
    List<MerchantMetricsWindow> windowsFor(String merchantId, Instant from, Instant to);

    /** Every locally-held window whose start is in {@code [from, to)}. */
    List<MerchantMetricsWindow> allWindows(Instant from, Instant to);

    /**
     * The {@code GlobalKTable} lookup. {@link Optional#empty()} means "no value for this key",
     * which is what a tombstone leaves behind - see {@link MerchantConfigSnapshot}.
     */
    Optional<MerchantConfigSnapshot> merchantConfig(String merchantId);
}
