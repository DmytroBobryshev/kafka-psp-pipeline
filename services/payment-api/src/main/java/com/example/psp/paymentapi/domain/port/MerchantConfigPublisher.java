package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.MerchantConfig;

/**
 * Outbound port for the merchant-configuration write path (M10). Two operations, because the
 * compacted topic {@code merchants.merchant-config-changed.v1} has exactly two kinds of record:
 * a value (upsert) and a <b>null value</b> (tombstone / delete).
 *
 * <h2>Why this port does NOT go through the M6 outbox</h2>
 *
 * <p>{@link PaymentEventPublisher} is backed by {@code adapters.out.outbox.
 * OutboxPaymentEventPublisher} - a row in {@code outbox_event}, relayed to Kafka by Debezium -
 * because {@code POST /api/payments} performs <b>two writes</b> (the {@code payments} row and the
 * event) that must commit atomically. That is the dual-write problem, and the outbox is its fix.
 *
 * <p>This path has <b>one</b> write. There is no {@code merchant_config} table (see
 * {@link MerchantConfig}): the compacted topic is the system of record, so "persist the config"
 * and "publish the event" are the same act. With nothing to be atomic <i>with</i>, an outbox row
 * would add a second store that then needs reconciling against the topic - it would create the
 * consistency problem it exists to solve. Three further, concrete reasons, all verifiable in this
 * repo:
 *
 * <ol>
 *   <li><b>The outbox physically cannot carry a tombstone as built.</b>
 *       {@code db/migration/V2__create_outbox_event_table.sql} declares {@code payload ... NOT
 *       NULL} and {@code V3} retypes it {@code BYTEA} - still {@code NOT NULL}. Debezium's
 *       {@code EventRouter} only emits a null-valued record when
 *       {@code route.tombstone.on.empty.payload=true}, which
 *       {@code infra/compose/connect/payment-outbox-connector.json} does not set. Supporting
 *       DELETE through the outbox means a schema migration plus a connector reconfiguration, to
 *       reach a record shape a two-line {@code KafkaTemplate.send(topic, key, null)} produces
 *       directly.</li>
 *   <li><b>The single connector is hard-wired to one topic.</b> That connector sets
 *       {@code "transforms.outbox.route.topic.replacement": "payments.payment-requested.v1"} -
 *       a literal, not {@code ${routedByValue}}. A second aggregate type flowing through it means
 *       editing and re-registering the live connector that carries M6's and M9 Phase 1's
 *       end-to-end evidence, for a feature that does not need it.</li>
 *   <li><b>The failure mode the outbox protects against does not apply.</b> If a merchant-config
 *       write fails, nothing has been committed anywhere and the caller gets a 5xx and retries
 *       the {@code PUT}. Compaction makes that retry free: the operation is a whole-state upsert
 *       under a fixed key, so replaying it N times converges to the same last value. The outbox
 *       exists for writes that <i>cannot</i> be safely re-driven by the caller because a local
 *       row already committed.</li>
 * </ol>
 *
 * <p>What is lost by publishing directly: the send is a real network call inside the request, so
 * a broker outage surfaces as a failed HTTP call rather than a queued row. That is the correct
 * trade here - a config change that the operator believes succeeded but that never reached the
 * topic would be worse than an honest 503.
 */
public interface MerchantConfigPublisher {

    /**
     * Publishes the merchant's complete configuration as the new last-value for its key. Keyed by
     * {@code merchantId} (ADR-0003) - on a compacted topic the key is not a routing hint, it is
     * the identity compaction dedupes on.
     */
    void publishConfigChanged(MerchantConfig config);

    /**
     * Publishes a <b>tombstone</b>: a record with the merchant's key and a {@code null} value.
     *
     * <p>This is not a stylistic choice, it is the only mechanism that works. Log compaction's
     * contract is "retain at least the last value for each key"; the only way to make it retain
     * nothing is to give it a record that has no value. A {@code deleted=true} flag inside the
     * value <i>is</i> a value, so compaction keeps it forever, every {@code GlobalKTable} keeps a
     * live row for the merchant, and every consumer in the fleet has to remember to check the
     * flag - a rule that is only ever one new consumer away from being forgotten. A null value
     * removes the key from the log and, because {@code null} is also the Kafka Streams KTable
     * delete signal, removes it from every downstream table automatically, with no cooperation
     * from the consumer's code at all.
     */
    void publishConfigDeleted(String merchantId);
}
