# How a payment travels through the system

```mermaid
flowchart LR
    M["Merchant<br/>(UI / API)"] -- "1 create payment" --> API[payment-api]
    API -- "2 payment requested" --> K([Kafka])
    K -- "3" --> PSP[psp-connector]
    PSP -- "4 charge" --> P["Payment provider"]
    PSP -- "5 status updates" --> K
    K -- "6" --> L["ledger<br/>updates balances"]
    K -- "6" --> W["webhook-notifier"]
    K -- "6" --> A["analytics<br/>dashboard metrics"]
    W -- "7 webhook" --> M
```

That's the whole idea: **services never call each other** — every arrow in the middle is an
event on a Kafka topic. Each consumer reacts at its own pace and keeps its own database.

Also in the picture, but off the happy path:

- **realtime-gateway** — streams the same events to the browser, so the UI updates live.
- **api-gateway** — the single REST entrance in front of payment-api.
- **audit-trail** — a Kafka Connect sink that copies events into MongoDB with zero code.
