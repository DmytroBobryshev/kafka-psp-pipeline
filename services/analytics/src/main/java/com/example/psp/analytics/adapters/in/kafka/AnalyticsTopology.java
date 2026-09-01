package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.application.ProjectAuthorizationLatencyUseCase;
import com.example.psp.analytics.application.ProjectWindowMetricsUseCase;
import com.example.psp.analytics.config.AnalyticsProperties;
import com.example.psp.analytics.config.StreamsStores;
import com.example.psp.analytics.domain.model.AuthorizationLatency;
import com.example.psp.analytics.domain.model.MerchantWindowMetrics;
import com.example.psp.analytics.domain.model.PaymentOutcome;
import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import java.time.Clock;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsTopology {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsTopology.class);

    private static final String DECLINED = "DECLINED";

    public AnalyticsTopology(
            StreamsBuilder streamsBuilder,
            AnalyticsProperties properties,
            io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde<PaymentStatusChanged>
                    paymentStatusChangedSerde,
            io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde<MerchantConfigChanged>
                    merchantConfigChangedSerde,
            io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde<PaymentRequested>
                    paymentRequestedSerde,
            ProjectWindowMetricsUseCase projectWindowMetricsUseCase,
            ProjectAuthorizationLatencyUseCase projectAuthorizationLatencyUseCase,
            Clock clock) {

        define(
                streamsBuilder,
                properties,
                paymentStatusChangedSerde,
                merchantConfigChangedSerde,
                paymentRequestedSerde,
                projectWindowMetricsUseCase,
                projectAuthorizationLatencyUseCase,
                clock);

        log.info(
                "Topology defined: window={} grace={} storeRetention={} authJoinWindow={} "
                        + "authJoinGrace={} stores=[{}, {}]",
                properties.windows().size(),
                properties.windows().grace(),
                properties.windows().storeRetention(),
                properties.authorizationJoin().window(),
                properties.authorizationJoin().grace(),
                StreamsStores.MERCHANT_METRICS,
                StreamsStores.MERCHANT_CONFIG);
    }

    public static void define(
            StreamsBuilder builder,
            AnalyticsProperties properties,
            Serde<PaymentStatusChanged> paymentStatusChangedSerde,
            Serde<MerchantConfigChanged> merchantConfigChangedSerde,
            Serde<PaymentRequested> paymentRequestedSerde,
            ProjectWindowMetricsUseCase projectWindowMetricsUseCase,
            ProjectAuthorizationLatencyUseCase projectAuthorizationLatencyUseCase,
            Clock clock) {

        Serde<PaymentOutcome> paymentOutcomeSerde =
                new JsonSerde<>(PaymentOutcome.class).noTypeInfo();
        Serde<MerchantWindowMetrics> metricsSerde =
                new JsonSerde<>(MerchantWindowMetrics.class).noTypeInfo();

        // ---- source 1: the compacted config topic, as a fully-replicated global table ----------
        GlobalKTable<String, MerchantConfigChanged> merchantConfig =
                builder.globalTable(
                        properties.kafka().merchantConfigChangedTopic(),
                        Consumed.with(Serdes.String(), merchantConfigChangedSerde)
                                .withName("merchant-config-source"),
                        Materialized.<String, MerchantConfigChanged, KeyValueStore<Bytes, byte[]>>as(
                                        StreamsStores.MERCHANT_CONFIG)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(merchantConfigChangedSerde));

        // ---- source 2: the payment status stream, on EVENT time --------------------------------
        KStream<String, PaymentStatusChanged> payments =
                builder.stream(
                        properties.kafka().paymentStatusChangedTopic(),
                        Consumed.with(Serdes.String(), paymentStatusChangedSerde)
                                .withTimestampExtractor(new EnvelopeEventTimeExtractor())
                                .withName("payment-status-changed-source"));

        KStream<String, PaymentStatusChanged> terminalPayments =
                payments.filter(
                        (merchantId, status) -> isTerminalStatus(status.getStatus()),
                        Named.as("terminal-status-only"));

        KStream<String, PaymentOutcome> outcomes =
                terminalPayments.leftJoin(
                        merchantConfig,
                        (merchantId, payment) -> merchantId,
                        (payment, config) -> toOutcome(payment, config, clock),
                        Named.as("merchant-config-join"));

        // ---- the windowed aggregation ----------------------------------------------------------
        KTable<Windowed<String>, MerchantWindowMetrics> windowed =
                outcomes
                        // groupByKey, NOT groupBy - see the class javadoc, section 3.
                        .groupByKey(
                                Grouped.with("merchant-outcomes", Serdes.String(), paymentOutcomeSerde))
                        .windowedBy(
                                TimeWindows.ofSizeAndGrace(
                                        properties.windows().size(), properties.windows().grace()))
                        .aggregate(
                                MerchantWindowMetrics::empty,
                                (merchantId, outcome, aggregate) -> aggregate.plus(outcome),
                                Named.as("merchant-metrics-1m-aggregate"),
                                Materialized
                                        .<String, MerchantWindowMetrics, WindowStore<Bytes, byte[]>>as(
                                                StreamsStores.MERCHANT_METRICS)
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(metricsSerde)
                                        .withRetention(properties.windows().storeRetention()));

        // ---- terminal: project to MongoDB -------------------------------------------------------
        windowed
                .toStream(Named.as("merchant-metrics-1m-to-stream"))
                .foreach(
                        (windowedKey, metrics) -> {
                            if (metrics == null) {
                                return;
                            }
                            projectWindowMetricsUseCase.project(
                                    windowedKey.key(),
                                    windowedKey.window().startTime(),
                                    windowedKey.window().endTime(),
                                    metrics);
                        },
                        Named.as("mongo-projection-sink"));

        KStream<String, PaymentRequested> requested =
                builder.stream(
                        properties.kafka().paymentRequestedTopic(),
                        Consumed.with(Serdes.String(), paymentRequestedSerde)
                                .withTimestampExtractor(new EnvelopeEventTimeExtractor())
                                .withName("payment-requested-source"));

        KStream<String, PaymentStatusChanged> statusByPaymentId =
                terminalPayments.selectKey(
                        (merchantId, status) -> status.getPaymentId(),
                        Named.as("rekey-status-changed-by-payment-id"));

        KStream<String, AuthorizationLatency> authorizationLatency =
                requested.join(
                        statusByPaymentId,
                        (request, status) -> toAuthorizationLatency(request, status),
                        JoinWindows.ofTimeDifferenceAndGrace(
                                        properties.authorizationJoin().window(),
                                        properties.authorizationJoin().grace())
                                .before(java.time.Duration.ZERO),
                        StreamJoined.<String, PaymentRequested, PaymentStatusChanged>with(
                                        Serdes.String(), paymentRequestedSerde, paymentStatusChangedSerde)
                                .withName("authorization-latency-join")
                                .withStoreName("authorization-latency-join"));

        authorizationLatency.foreach(
                (paymentId, latency) -> projectAuthorizationLatencyUseCase.project(latency),
                Named.as("authorization-latency-projection-sink"));
    }

    private static boolean isTerminalStatus(String status) {
        return "SUCCEEDED".equalsIgnoreCase(status)
                || "DECLINED".equalsIgnoreCase(status)
                || "FAILED".equalsIgnoreCase(status);
    }

    private static PaymentOutcome toOutcome(
            PaymentStatusChanged payment, MerchantConfigChanged config, Clock clock) {

        long occurredAtMillis =
                payment.getEnvelope() != null && payment.getEnvelope().getOccurredAt() != null
                        ? payment.getEnvelope().getOccurredAt().toEpochMilli()
                        : clock.millis();
        long latencyMillis = Math.max(0L, clock.millis() - occurredAtMillis);

        return new PaymentOutcome(
                payment.getMerchantId(),
                DECLINED.equalsIgnoreCase(payment.getStatus()),
                latencyMillis,
                config == null ? null : config.getDisplayName(),
                config == null ? null : config.getDeclineRateAlertThresholdBps());
    }

    private static AuthorizationLatency toAuthorizationLatency(
            PaymentRequested request, PaymentStatusChanged status) {
        return AuthorizationLatency.of(
                request.getPaymentId(),
                status.getMerchantId(),
                status.getProviderReference(),
                status.getStatus(),
                request.getEnvelope().getOccurredAt(),
                status.getEnvelope().getOccurredAt());
    }
}
