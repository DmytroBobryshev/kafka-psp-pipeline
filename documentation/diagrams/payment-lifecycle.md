# Payment lifecycle — the five stages

What the merchant sees in the History panel, and where each stage comes from.

```mermaid
sequenceDiagram
    participant M as Merchant
    participant API as payment-api
    participant PSP as psp-connector
    participant P as Provider

    M->>API: create payment
    Note over API: 1. CREATED
    API->>PSP: payment requested (via Kafka)
    Note over PSP: 2. PENDING - sent to provider
    PSP->>P: charge
    P-->>PSP: answer + reference id
    Note over PSP: 3. IPN_RECEIVED - provider answered
    Note over PSP: 4. VERIFIED - duplicate check passed
    PSP->>API: 5. SUCCEEDED or DECLINED (via Kafka)
    API-->>M: webhook notification
```

Three details that matter:

- **Stuck payment?** If no final answer arrives within the merchant's `paymentExpirationSeconds`,
  payment-api marks it `EXPIRED` and the merchant gets a webhook. A late provider answer still
  wins — the provider is the source of truth.
- **Timeout?** Stage 3 honestly never appears: no answer, no IPN.
- The webhook is delivered by **webhook-notifier** (retries + DLQ), and **ledger** /
  **analytics** consume the same final status for balances and metrics.
