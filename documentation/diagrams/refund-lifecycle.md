# Refund lifecycle — the six stages

A refund moves money back, so the ledger must reserve it first. Every step is an event; if the
provider says no, the reservation is released — nothing is lost, nothing double-refunded.

```mermaid
sequenceDiagram
    participant M as Merchant
    participant API as payment-api
    participant L as ledger
    participant PSP as psp-connector
    participant P as Provider

    M->>API: request refund
    Note over API: 1. REQUESTED
    API->>L: refund requested (via Kafka)
    L->>L: check balance, set money aside
    Note over L: 2. FUNDS_RESERVED
    L->>PSP: funds reserved (via Kafka)
    Note over PSP: 3. PENDING - sent to provider
    PSP->>P: refund
    P-->>PSP: answer + reference id
    Note over PSP: 4. IPN_RECEIVED, then 5. VERIFIED
    alt provider approved
        PSP->>L: refund completed - money goes out
        Note over API: 6. COMPLETED
    else provider declined
        PSP->>L: refund failed - reservation released
        Note over API: 6. FAILED
    end
    API-->>M: webhook notification
```

Safety nets for a refund that never finishes:

- **ledger** releases any reservation older than 2 minutes — the money is always safe.
- **payment-api** marks the refund `EXPIRED` after the merchant's `refundExpirationSeconds`
  and sends a `REFUND_EXPIRED` webhook — the merchant is never left guessing.
