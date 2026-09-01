# M20+ — Lifecycle trails, merchant-tunable expiration, honest analytics

Everything below was verified live on the kind cluster (`kafka-psp`); the timestamps in the
evidence blocks are real captures from those runs, not illustrations.

## 1. Five-stage payment trail

The payment status topic (`payments.payment-status-changed.v1`) now carries the full lifecycle,
not just the terminal verdict:

| Stage | Status on the wire | Publisher | When |
|---|---|---|---|
| 1 | `CREATED` (synthesized) | payment-api | row insert, no event |
| 2 | `PENDING` | psp-connector | before the provider call |
| 3 | `IPN_RECEIVED` (+`providerReference`) | psp-connector | provider returned (skipped on TIMEOUT — no IPN ever arrived) |
| 4 | `VERIFIED` | psp-connector | after the level-2 dedup check + `tryRecord` |
| 5 | `SUCCEEDED` / `DECLINED` | psp-connector | terminal, unchanged |

Design rules that keep the state machine honest:

- **History-only statuses.** `IPN_RECEIVED`/`VERIFIED` insert `payment_status_history` rows
  (V10 added `provider_reference`) but never touch `payments.status` — the mapper resolves
  them to a `null` domain status. The payment state machine stays
  `CREATED → PENDING → SUCCEEDED/FAILED/EXPIRED`.
- **Replay-safe.** The emissions sit on the happy path before `tryRecord`; the level-1 replay
  branch returns before `authorize()` is ever called. `RebalanceLossIT` counted exactly
  30×4 events for 30 payments across a rebalance storm with 5 duplicate terminal deliveries —
  zero duplicated non-terminal events.
- **Webhook allowlist.** webhook-notifier plans deliveries only for terminal statuses, so
  trail events never spam merchants.

Evidence (live, `GET /api/payments/{id}/history`):

```
CREATED       00:20:28.387  payment-api
PENDING       00:20:29.429  psp-connector
IPN_RECEIVED  00:20:31.164  psp-connector · provider ref 647b1500…
VERIFIED      00:20:31.266  psp-connector
SUCCEEDED     00:20:31.280  psp-connector
→ exactly one webhook, SUCCESS, to the merchant's configured URL
```

## 2. Six-stage refund trail

The refund saga (`refund-requested → funds-reserved → provider call → refund-completed/failed`)
got the same treatment via a new topic `refunds.refund-status-changed.v1` (avro 15):
psp-connector's `ExecuteRefundUseCase` emits `PENDING` / `IPN_RECEIVED` / `VERIFIED` around the
provider call; payment-api records them plus `FUNDS_RESERVED` (from the ledger's event) and the
terminals into `refund_status_history` (V12) through four history-only listeners, and serves

`GET /api/payments/{paymentId}/refunds/{refundId}/history` (404 when the refund does not belong
to the payment).

Evidence (live):

```
REQUESTED       16:16:15.815  payment-api
FUNDS_RESERVED  16:16:16.437  ledger
PENDING         16:16:16.452  psp-connector
IPN_RECEIVED    16:16:19.023  psp-connector · provider ref 16ad1b31…
VERIFIED        16:16:19.047  psp-connector
COMPLETED       16:16:19.064  psp-connector
```

A `.13`-magic refund produced the same trail ending in `FAILED`; a plain 15.00 EUR refund
genuinely lost the simulator's ~10% decline dice during the drill — the trail showed its
honest path to `FAILED` too.

## 3. Merchant-tunable payment expiration

`paymentExpirationSeconds` (30..86400, default 900) lives in the merchant config: avro 06,
`PUT /api/merchants/{id}/config`, the compacted topic, and payment-api's projection (V11).

The sweep (`@Scheduled` every 5 s in payment-api) selects payments still `CREATED`/`PENDING`
past their merchant's window (`LEFT JOIN merchant_configs`, `COALESCE(…, 900)`) and publishes a
real `EXPIRED` event to the payment status topic — with a **deterministic eventId**
(`UUID.nameUUIDFromBytes("expired:" + paymentId)`), so re-sweeps republish the same id and the
history UNIQUE constraint dedups. The event is applied by payment-api's own listener:
`EXPIRED` only lands from `CREATED`/`PENDING`; a late provider terminal still overwrites it
(the provider is authoritative). Merchants get a webhook; the ledger ignores it; analytics
excludes it from decline-rate windows.

Two live-run bugs worth remembering:

- `p.created_at < :now - interval` fails on Postgres — the bind parameter's type cannot be
  inferred in `param - interval` and defaults to `interval`
  (`ERROR: operator does not exist: timestamp with time zone < interval`). Fix:
  `CAST(:now AS timestamptz)`.
- payment-api's KafkaUser needed `Write` on the status topic (deny-by-default ACLs) — granted
  in both the Strimzi `KafkaUser` and the compose init script.

Evidence (live, 30 s window, `.66` timeout payment):

```
CREATED  01:43:04.183  payment-api
PENDING  01:43:04.735  psp-connector
EXPIRED  01:43:35.158  payment-api      (sweep; 30s window + ≤5s sweep delay)
→ webhook PAYMENT_STATUS_CHANGED SUCCESS
PUT with paymentExpirationSeconds=10 → 400 (min 30)
```

## 4. Analytics: count a payment once

`AnalyticsTopology` fed **every** status event into the windowed metrics and the latency join —
so each payment counted twice from the moment `PENDING` was introduced, and would have counted
four times with the trail events (and non-terminal statuses counted as "successes" in the
decline-rate math, since `succeeded = !DECLINED`). A named stateless filter
(`terminal-status-only`, statuses `SUCCEEDED/DECLINED/FAILED`) now sits between the source and
both joins; stateful node names are untouched, so the existing state stores and internal topics
survived the upgrade (streams transitioned straight to `RUNNING`).

## 5. Operational notes

- Fresh-cluster Debezium race: `payment-outbox-connector`'s task can start before Flyway
  creates the publication — `kubectl annotate kafkaconnector payment-outbox-connector
  strimzi.io/restart-task=0` once payment-api is up.
- The UI has no background polling: queries cache for 30 s, refresh on navigation, mutations
  and the header button. `ui/e2e/*.cjs` are Playwright smoke scripts asserting zero layout
  shift across all six pages (144 header-geometry samples during navigation).
