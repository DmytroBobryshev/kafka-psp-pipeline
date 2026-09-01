# The payment flow

## The journey

```
merchant/UI ─POST /api/payments─▶ payment-api ─outbox─▶ payments.payment-requested.v1
                                                              │
                                                        psp-connector
                                              PENDING ─▶ provider call ─▶ IPN_RECEIVED ─▶ VERIFIED
                                                              │
                                             payments.payment-status-changed.v1 (terminal)
                                       ┌──────────────┬───────┴────────┬─────────────────┐
                                    ledger        webhook-notifier   analytics      payment-api
                                 (EOS balance)   (merchant webhook)  (windows)     (state+history)
```

## Stage by stage (what the merchant sees in History)

| Stage | Who | What actually happened |
|---|---|---|
| `CREATED` | payment-api | Row inserted + `payment-requested` written to the **outbox** in the same DB transaction. Debezium tails the WAL and publishes it — the dual-write problem solved (M6). Currency is validated against the merchant's `allowedCurrencies` first. |
| `PENDING` | psp-connector | Consumed the request, about to call the provider. Emitted as a real event; payment-api applies it **only from CREATED** (a late PENDING can never downgrade a terminal). |
| `IPN_RECEIVED` | psp-connector | The provider answered (the simulated IPN), carrying the **provider reference** (external id). On a provider TIMEOUT this stage honestly never appears. |
| `VERIFIED` | psp-connector | The outcome passed the dedup/verification step (`providerEventId` uniqueness in the attempt log — M5's level-2 check). |
| `SUCCEEDED` / `DECLINED` | psp-connector | Terminal. Published with a **deterministic** `statusEventId`, so redeliveries republish the identical event and every consumer dedups. |
| `EXPIRED` | payment-api | Only if no terminal arrived within the merchant's `paymentExpirationSeconds`: a 5s sweep publishes a real EXPIRED event (deterministic eventId ⇒ re-sweeps can't duplicate). Applied only from CREATED/PENDING; a late provider terminal still overwrites it — the provider is authoritative. |

`IPN_RECEIVED`/`VERIFIED` are **history-only**: they land in `payment_status_history` but never
touch `payments.status`. The state machine stays small (`CREATED → PENDING →
SUCCEEDED/FAILED/EXPIRED`); the trail stays complete.

## Reliability mechanics on this path

- **At-least-once + idempotency** (M5): psp-connector records every attempt keyed by
  `inboundEventId`; a redelivered request replays the recorded outcome instead of re-charging.
- **Blocking sends**: status publishes use `send().get()` — a crash between "processed" and
  "published" can't silently drop the outcome (this exact loss window was found by a failure
  drill and regression-proven fixed; see `docs/M19-failure-drills-part2.md`).
- **Exactly-once in ledger** (M7): consuming a terminal status and writing the balance entry
  happen inside one Kafka transaction; `transactional.id` fencing kills zombies.
- **Webhooks** (M8): the notifier plans a delivery only for terminal statuses, retries through
  non-blocking retry topics, and parks poison messages in a DLQ with a replay console.
- **Lag autoscaling** (M18): KEDA scales psp-connector 1→6 on consumer lag — measured live.

## Simulating outcomes

The provider is simulated with tunable randomness (~10% declines, ~5% timeouts) plus **magic
amount endings** (the Stripe/Adyen sandbox convention): `.13` forces a decline, `.66` forces a
timeout. The UI's simulator exposes these as outcome chips.
