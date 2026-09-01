# Refund saga (six stages)

```mermaid
sequenceDiagram
    autonumber
    participant M as Merchant/UI
    participant PA as payment-api
    participant K as Kafka
    participant L as ledger
    participant PC as psp-connector
    participant PR as Provider (simulated)
    participant W as webhook-notifier

    M->>PA: POST /api/payments/{id}/refunds
    PA->>K: refund-requested
    Note over PA: REQUESTED
    K->>L: reserve funds (balance check)
    L->>K: funds-reserved
    Note over L: FUNDS_RESERVED
    K->>PC: consume reservation
    PC->>K: refund status: PENDING
    PC->>PR: refund()
    PR-->>PC: outcome + provider ref
    PC->>K: refund status: IPN_RECEIVED, VERIFIED
    alt approved
        PC->>K: refund-completed
        K->>L: commit reservation
    else declined
        PC->>K: refund-failed
        K->>L: RELEASE reservation (compensation)
    end
    K->>W: plan webhook (COMPLETED / FAILED)
    W->>M: POST webhook
    Note over PA: no terminal within refundExpirationSeconds -><br/>EXPIRED + REFUND_EXPIRED webhook;<br/>ledger TTL (PT2M) frees stuck reservations independently
```
