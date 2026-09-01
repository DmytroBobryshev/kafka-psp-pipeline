# Documentation

How the Kafka PSP Pipeline works — what each piece does, how the pieces talk, and why they are
built the way they are.

| Section | Contents |
|---|---|
| [architecture/01-overview.md](architecture/01-overview.md) | The system at a glance: services, topics, design principles |
| [architecture/02-payment-flow.md](architecture/02-payment-flow.md) | A payment's full journey: five lifecycle stages, outbox, idempotency, EOS, expiration |
| [architecture/03-refund-flow.md](architecture/03-refund-flow.md) | The refund saga: reservation, six lifecycle stages, compensations, expiration |
| [architecture/04-merchant-config.md](architecture/04-merchant-config.md) | Merchant configuration: the compacted topic, projections, currencies, webhooks, expiration knobs |
| [architecture/05-kafka-concepts-map.md](architecture/05-kafka-concepts-map.md) | Which Kafka concept lives where — a map for learning |
| [diagrams/](diagrams/) | Mermaid diagrams: system overview, payment/refund sequences, config propagation, deployment |

Related, elsewhere in the repo:

- [`README.md`](../README.md) — how to run everything from a machine with nothing installed.
- [`docs/PLAN.md`](../docs/PLAN.md) — the module-by-module learning plan (M1..M19).
- [`docs/adr/`](../docs/adr/) — architecture decision records.
- [`docs/M19-failure-drills.md`](../docs/M19-failure-drills.md), [`part 2`](../docs/M19-failure-drills-part2.md) — measured failure drills against the live cluster.
- [`docs/M20-lifecycle-trails-and-expiration.md`](../docs/M20-lifecycle-trails-and-expiration.md) — lifecycle trails and expiration, with live evidence.
