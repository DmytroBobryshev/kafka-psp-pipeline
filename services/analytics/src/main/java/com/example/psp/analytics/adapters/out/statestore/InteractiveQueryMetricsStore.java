package com.example.psp.analytics.adapters.out.statestore;

import com.example.psp.analytics.config.StreamsStores;
import com.example.psp.analytics.domain.model.MerchantConfigSnapshot;
import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.model.MerchantWindowMetrics;
import com.example.psp.analytics.domain.port.WindowMetricsQueryPort;
import com.example.psp.common.events.avro.MerchantConfigChanged;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * Interactive queries: reads this instance's live Kafka Streams state stores (M10).
 *
 * <p>This is the adapter behind {@link WindowMetricsQueryPort}. It answers from RocksDB in this
 * JVM - no broker round-trip, no database - which is why it can show the <b>currently open</b>
 * 1-minute window, a value that exists nowhere else yet.
 *
 * <h2>Two store types, two very different queries</h2>
 *
 * <ul>
 *   <li>{@link StreamsStores#MERCHANT_METRICS} is a <b>window store</b>. Its key space is
 *       (key, windowStart), so every read takes a time range: {@code fetch(key, from, to)} for one
 *       merchant, {@code fetchAll(from, to)} for all of them. It is local: it only holds the
 *       partitions assigned to this instance.</li>
 *   <li>{@link StreamsStores#MERCHANT_CONFIG} is the <b>GlobalKTable</b>'s key/value store. A
 *       plain {@code get(key)}, and - unlike the window store - it is complete on every instance,
 *       because a global store replicates every partition everywhere.</li>
 * </ul>
 *
 * <h2>Why every method checks the state first</h2>
 *
 * <p>{@code KafkaStreams#store} throws {@code InvalidStateStoreException} whenever the store is
 * not currently available: before {@code start()}, during a rebalance, and - the interesting
 * case - for the entire duration of a <b>state restore</b>, when the instance is replaying
 * {@code analytics-streams.v1-merchant-metrics-1m-changelog} into a fresh RocksDB directory.
 * That window is exactly what the "state restore proof" makes visible, so this adapter reports it
 * as "not ready" and lets the REST layer answer 503, rather than throwing or - worse - returning
 * an empty list that looks like "no traffic".
 */
@Component
public class InteractiveQueryMetricsStore implements WindowMetricsQueryPort {

    private static final Logger log = LoggerFactory.getLogger(InteractiveQueryMetricsStore.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    /**
     * The window size the topology was built with. {@code ReadOnlyWindowStore} hands back only a
     * window's START (the size is a property of the topology, not of the stored record), so
     * {@code windowEnd} in a response has to be reconstructed - and it must be reconstructed from
     * the same configured value the aggregation used, not a hard-coded minute.
     */
    private final java.time.Duration windowSize;

    public InteractiveQueryMetricsStore(
            StreamsBuilderFactoryBean streamsBuilderFactoryBean,
            com.example.psp.analytics.config.AnalyticsProperties properties) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.windowSize = properties.windows().size();
    }

    @Override
    public boolean storeReady() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        return streams != null && streams.state() == KafkaStreams.State.RUNNING;
    }

    @Override
    public String clientState() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        return streams == null ? "NOT_STARTED" : streams.state().name();
    }

    @Override
    public List<MerchantMetricsWindow> windowsFor(String merchantId, Instant from, Instant to) {
        ReadOnlyWindowStore<String, MerchantWindowMetrics> store = windowStore();
        List<MerchantMetricsWindow> results = new ArrayList<>();

        // WindowStoreIterator<V> yields KeyValue<Long windowStartMs, V> - the key is implicit
        // because it was supplied. Iterators hold an open RocksDB snapshot and MUST be closed.
        try (WindowStoreIterator<MerchantWindowMetrics> iterator = store.fetch(merchantId, from, to)) {
            while (iterator.hasNext()) {
                KeyValue<Long, MerchantWindowMetrics> entry = iterator.next();
                results.add(toWindow(merchantId, entry.key, entry.value));
            }
        }
        results.sort(Comparator.comparing(MerchantMetricsWindow::windowStart));
        return results;
    }

    @Override
    public List<MerchantMetricsWindow> allWindows(Instant from, Instant to) {
        ReadOnlyWindowStore<String, MerchantWindowMetrics> store = windowStore();
        List<MerchantMetricsWindow> results = new ArrayList<>();

        try (KeyValueIterator<Windowed<String>, MerchantWindowMetrics> iterator =
                store.fetchAll(from, to)) {
            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, MerchantWindowMetrics> entry = iterator.next();
                results.add(
                        toWindow(
                                entry.key.key(),
                                entry.key.window().start(),
                                entry.value));
            }
        }
        results.sort(
                Comparator.comparing(MerchantMetricsWindow::windowStart)
                        .reversed()
                        .thenComparing(MerchantMetricsWindow::merchantId));
        return results;
    }

    @Override
    public Optional<MerchantConfigSnapshot> merchantConfig(String merchantId) {
        KafkaStreams streams = requireRunningStreams();
        ReadOnlyKeyValueStore<String, MerchantConfigChanged> store =
                streams.store(
                        StoreQueryParameters.fromNameAndType(
                                StreamsStores.MERCHANT_CONFIG, QueryableStoreTypes.keyValueStore()));

        // A null here is the whole tombstone story in one line: payment-api published a record
        // with this key and a null value, Streams deleted the row, and there is nothing to return.
        // No "deleted" flag is consulted, because none exists.
        MerchantConfigChanged config = store.get(merchantId);
        if (config == null) {
            log.debug("GlobalKTable lookup miss for merchantId={} (never configured, or tombstoned)", merchantId);
            return Optional.empty();
        }

        return Optional.of(
                new MerchantConfigSnapshot(
                        config.getMerchantId(),
                        config.getDisplayName(),
                        config.getStatus(),
                        config.getPayoutCurrency(),
                        config.getWebhookUrl(),
                        config.getDeclineRateAlertThresholdBps()));
    }

    private ReadOnlyWindowStore<String, MerchantWindowMetrics> windowStore() {
        return requireRunningStreams()
                .store(
                        StoreQueryParameters.fromNameAndType(
                                StreamsStores.MERCHANT_METRICS, QueryableStoreTypes.windowStore()));
    }

    private KafkaStreams requireRunningStreams() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            throw new IllegalStateException(
                    "Kafka Streams is not RUNNING (state="
                            + (streams == null ? "NOT_STARTED" : streams.state())
                            + ") - local state stores are not queryable while starting, rebalancing "
                            + "or restoring from the changelog");
        }
        return streams;
    }

    private MerchantMetricsWindow toWindow(String merchantId, long windowStartMillis, MerchantWindowMetrics metrics) {
        Instant start = Instant.ofEpochMilli(windowStartMillis);
        return new MerchantMetricsWindow(merchantId, start, start.plus(windowSize), metrics);
    }
}
