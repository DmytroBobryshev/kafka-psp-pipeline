package com.example.psp.analytics.domain.model;

/**
 * One payment outcome, already enriched with the merchant's configuration by the
 * {@code GlobalKTable} join - the unit the 1-minute windows aggregate over (M10).
 *
 * <p>Pure Java (ADR-0007): no Kafka, no Avro. {@code adapters.in.kafka} maps the generated
 * {@code PaymentStatusChanged}/{@code MerchantConfigChanged} Avro records onto this type inside
 * the join's {@code ValueJoiner}, so everything downstream of the join - the aggregation, the
 * REST projection, the Mongo document - is expressed in domain terms.
 *
 * <p>This value is never serialized: it only ever exists between the join node and the
 * aggregation node, and because the stream is already keyed by {@code merchantId} (ADR-0003)
 * there is no repartition topic between them to serialize it into. See the README's "Internal
 * topics" section.
 *
 * @param merchantId              partition key of the source topic and the grouping key.
 * @param declined                {@code true} when the status was {@code DECLINED}.
 * @param pipelineLatencyMillis   wall-clock milliseconds between the event's domain occurrence
 *                                time ({@code envelope.occurredAt}, ADR-0002) and the moment
 *                                this service processed it. Read the name literally: this is
 *                                <b>pipeline</b> latency (psp-connector emit {@code ->} broker
 *                                {@code ->} analytics), NOT payment authorization latency.
 *                                Authorization latency needs
 *                                {@code payments.payment-requested.v1 x
 *                                payments.payment-status-changed.v1}, a stream-stream join across
 *                                two differently-keyed topics - that is M13, and it is the join
 *                                that will finally force a repartition topic into this
 *                                application.
 * @param merchantDisplayName     from the joined config, or {@code null} on a join miss.
 * @param declineRateAlertThresholdBps from the joined config, or {@code null} on a join miss.
 */
public record PaymentOutcome(
        String merchantId,
        boolean declined,
        long pipelineLatencyMillis,
        String merchantDisplayName,
        Integer declineRateAlertThresholdBps) {

    /** True when the {@code GlobalKTable} lookup found a config for this merchant. */
    public boolean merchantConfigKnown() {
        return merchantDisplayName != null;
    }
}
