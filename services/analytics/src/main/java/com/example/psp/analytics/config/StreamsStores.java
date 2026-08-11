package com.example.psp.analytics.config;

/**
 * The names of this application's Kafka Streams state stores (M10).
 *
 * <p>Shared constants because two adapters need the same string and must not drift: the topology
 * ({@code adapters.in.kafka.AnalyticsTopology}) creates the stores, and the interactive-query
 * adapter ({@code adapters.out.statestore}) looks them up by name. A typo would not fail the
 * build - it would fail at query time with {@code UnknownStateStoreException}.
 *
 * <p>These names are not cosmetic. A store's changelog topic is
 * {@code <application.id>-<storeName>-changelog}, so renaming a store renames its changelog and
 * abandons all existing state, and the names here are the ones docs/diagrams/topic-map.md
 * records for lag dashboards and M18 ACLs.
 */
public final class StreamsStores {

    /**
     * The windowed aggregate. Backed by a RocksDB window store and, because logging is on (the
     * default), by {@code analytics-streams.v1-merchant-metrics-1m-changelog}.
     */
    public static final String MERCHANT_METRICS = "merchant-metrics-1m";

    /**
     * The {@code GlobalKTable}'s store. Backed by RocksDB and by <b>no changelog at all</b> - the
     * compacted source topic {@code merchants.merchant-config-changed.v1} already <i>is</i> a
     * changelog (last value per key, retained forever), so Streams marks global stores
     * non-logged. See the README's "Internal topics" section; this is why the topic-map's
     * originally-predicted {@code ...-merchant-config-store-changelog} never appears.
     */
    public static final String MERCHANT_CONFIG = "merchant-config-store";

    private StreamsStores() {
    }
}
