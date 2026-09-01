# Merchant settings — one save, three consumers

```mermaid
flowchart LR
    UI["Save merchant settings<br/>in the UI"] --> API[payment-api]
    API -- "one event per save" --> T(["Kafka topic<br/>keeps latest value per merchant"])
    T --> C1["payment-api<br/>allowed currencies,<br/>expiration windows"]
    T --> C2["analytics<br/>merchant names and<br/>alerts on the dashboard"]
    T --> C3["webhook-notifier<br/>where to deliver webhooks"]
```

The topic is *compacted*: Kafka keeps only the newest event per merchant, so it behaves like a
table. Deleting a merchant publishes an empty event (a *tombstone*) — the merchant disappears
from all three consumers with no cleanup code anywhere.
