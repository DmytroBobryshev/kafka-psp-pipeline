package com.example.psp.analytics.adapters.in.web;

/**
 * Wire contract for {@code GET /api/analytics/state} (M10) - the endpoint the state-restore proof
 * polls.
 *
 * @param applicationId the Streams {@code application.id}; also the consumer group and the prefix
 *                      of every internal topic (see {@code config.KafkaStreamsConfig}).
 * @param stateDir      where RocksDB writes. Echoed here because "delete the state directory and
 *                      restart" is a step in the restore proof and getting the path wrong makes
 *                      the proof silently prove nothing.
 * @param clientState   {@code CREATED} / {@code REBALANCING} / {@code RUNNING} / {@code ERROR} /
 *                      {@code NOT_STARTED}. A restart with a wiped {@code stateDir} sits in
 *                      {@code REBALANCING} while it replays
 *                      {@code analytics-streams.v1-merchant-metrics-1m-changelog}, then flips to
 *                      {@code RUNNING} - without re-reading a single record of
 *                      {@code payments.payment-status-changed.v1}.
 * @param storeReady    convenience: {@code clientState == RUNNING}.
 */
public record StreamsStateResponse(
        String applicationId, String stateDir, String clientState, boolean storeReady) {
}
