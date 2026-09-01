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

@Component
public class InteractiveQueryMetricsStore implements WindowMetricsQueryPort {

    private static final Logger log = LoggerFactory.getLogger(InteractiveQueryMetricsStore.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

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
