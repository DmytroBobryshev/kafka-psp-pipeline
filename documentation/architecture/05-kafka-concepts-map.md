# Kafka concepts map — what lives where

| Concept | Where to look |
|---|---|
| Producer fundamentals (acks, idempotence, batching, keys) | `payment-api` outbound config |
| Consumer groups, manual ack, rebalancing | `psp-connector` listeners |
| At-least-once + idempotent consumer (dedup table) | `psp-connector` attempt log (M5) |
| Transactional outbox + CDC | `payment-api` outbox + Debezium connector (M6) |
| Exactly-once semantics, `transactional.id` fencing | `ledger` (M7), `LedgerEosIT` |
| Non-blocking retries + DLQ + replay | `webhook-notifier` (M8), DLQ console in the UI |
| Avro, Schema Registry, compatibility | `libs/common-events` (M9) — 15 schemas |
| Compacted topics & tombstones | `merchants.merchant-config-changed.v1` (M10/M13) |
| Kafka Streams: windows, grace, suppression, RocksDB, interactive queries | `analytics` topology (M10) |
| GlobalKTable join (no co-partitioning) | `analytics` merchant-config join (M13) |
| Stream-stream join + repartition topics | `analytics` authorization-latency join (M13) |
| Choreographed saga + compensation | refund flow across payment-api/ledger/psp-connector (M11) |
| Request-reply over Kafka | provider-status query/reply via `realtime-gateway` (M12) |
| Claim check (large payloads via object storage) | disputes + MinIO (M13) |
| Client quotas & throttling | `KafkaUser.spec.quotas`, measured drill (M13) |
| Kafka Connect sink (zero-code consumer) | mongo audit sink |
| SASL/SCRAM + deny-by-default ACLs | `infra/k8s/kafka/users/*`, `infra/compose/kafka-init/init-security.sh` (M14) |
| Metrics & lag monitoring | Strimzi metricsConfig + kafka-exporter + Prometheus/Grafana (M15) |
| Lag-based autoscaling | KEDA ScaledObject on psp-connector (M18); pause with `autoscaling.keda.sh/paused-replicas` |
| Failure semantics: ISR, unclean election, rebalance cost, offset resets | `docs/M19-failure-drills*.md` — measured |
| Lifecycle event sourcing & deterministic republish | status trails + expiration (`docs/M20-*.md`) |
