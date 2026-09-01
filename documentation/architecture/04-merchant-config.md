# Merchant configuration

One PUT, one compacted topic, many independent read models.

## The write

`PUT /api/merchants/{id}/config` (payment-api) validates and publishes the full config to
**`merchants.merchant-config-changed.v1`** — a **compacted** topic keyed by merchantId. The 200
means "on the topic" (the write blocks on the broker ack). `DELETE` publishes a **tombstone**:
after compaction the merchant ceases to exist everywhere, with no cleanup code anywhere.

| Field | Meaning |
|---|---|
| `displayName`, `status` | Only `ACTIVE` merchants can take payments (`SUSPENDED` is refused at POST time). |
| `allowedCurrencies` (1..3) | Payments/refunds in any other currency are rejected with a 4xx naming the allowed set. |
| `payoutCurrency` | Must be one of the allowed currencies. |
| `webhookUrl` | Where webhook-notifier actually delivers — resolved from the projection **at delivery time**, so an update applies to the next delivery immediately. |
| `declineRateAlertThresholdBps` | Feeds the analytics windows' alert flag. |
| `paymentExpirationSeconds` (30..86400, default 900) | CREATED/PENDING payments older than this become `EXPIRED`. |
| `refundExpirationSeconds` (30..86400, default 900) | Refunds with no terminal outcome older than this become `EXPIRED`. |

## The readers (each rebuilds itself from the topic alone)

| Reader | Mechanism | Used for |
|---|---|---|
| payment-api `merchant_configs` | tombstone-aware listener → Postgres projection | currency gate, expiration windows, `GET /api/merchants` |
| analytics | **GlobalKTable** (fully replicated, non-logged store) | enriching windowed metrics with display name + alert threshold — a join with no repartition |
| webhook-notifier `merchant_webhooks` | listener → Mongo projection | per-merchant delivery URL |

This is the compaction lesson in one feature: config is a *table* disguised as a stream — the
latest value per key is the state, tombstones are deletes, and every consumer owns its local
copy at its own pace.
