# The refund flow (a choreographed saga)

A refund moves money *back*, so it must first be **reserved** against the merchant balance —
a multi-service transaction with no distributed lock. It is choreographed: each service reacts
to events, and every step has a compensation.

## The saga

```
POST /api/payments/{id}/refunds
      │ payment-api ─▶ refunds.refund-requested.v1
      ▼
   ledger: reserve funds (balance check!) ─▶ refunds.funds-reserved.v1
      │                                          (insufficient ⇒ refund-failed, saga ends)
      ▼
   psp-connector: PENDING ─▶ provider refund call ─▶ IPN_RECEIVED ─▶ VERIFIED
      │                    (stages on refunds.refund-status-changed.v1)
      ▼
   refunds.refund-completed.v1 ──▶ ledger commits the reservation
   refunds.refund-failed.v1    ──▶ ledger RELEASES the reservation (compensation)
      │
      ▼
   webhook-notifier ─▶ merchant webhook (REFUND_COMPLETED / REFUND_FAILED / REFUND_EXPIRED)
```

## The six stages in History

| Stage | Source | Meaning |
|---|---|---|
| `REQUESTED` | payment-api | Accepted and published (synthesized from the refund row). |
| `FUNDS_RESERVED` | ledger | Money set aside; the saga is committed to trying. |
| `PENDING` | psp-connector | Sent to the provider. |
| `IPN_RECEIVED` | psp-connector | Provider answered, with the provider reference. |
| `VERIFIED` | psp-connector | Passed dedup/verification. |
| `COMPLETED` / `FAILED` | psp-connector | Terminal; ledger commits or releases accordingly. |
| `EXPIRED` | payment-api | Only when no terminal arrived within `refundExpirationSeconds`. |

Served by `GET /api/payments/{paymentId}/refunds/{refundId}/history` (404 if the refund does
not belong to that payment). The refund aggregate in payment-api deliberately stays `REQUESTED`
— its truth is the trail plus the ledger's saga state (`GET /api/refunds/{id}`).

## Two safety nets, different jobs

- **Ledger reservation TTL (PT2M)**: a reservation neither committed nor released in time is
  swept and released (`reservation-released`, reason TIMEOUT). Protects the *money*.
- **Refund expiration (merchant-tunable)**: a refund with no terminal outcome within the
  merchant's window gets an `EXPIRED` trail event and a `REFUND_EXPIRED` webhook. Protects the
  *merchant's picture of the world*. A late real outcome still lands in the trail afterwards —
  verified live with the executor deliberately stopped (KEDA paused), see
  `docs/M20-lifecycle-trails-and-expiration.md`.

Magic refund endings (`.13`, `.01`, `.05`, `.55`, `.65`, `.75` — forced failure) let the UI
simulator exercise the compensation path on demand.
