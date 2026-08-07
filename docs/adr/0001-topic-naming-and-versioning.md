# ADR-0001: Topic naming convention and version-suffix strategy

- **Status:** Accepted
- **Date:** 2026-08-08
- **Affects:** every module; hard to change after M2 (topics become GitOps CRs in M18)

## Context

Topic names are the public API of the system: they end up in ACLs (M14), `KafkaTopic` CRs
(M18), consumer configs, dashboards, and Streams application IDs. Renaming a topic later is a
migration, not an edit. We need a convention that is (a) greppable, (b) survives breaking
schema changes, and (c) leaves room for retry/DLQ chains (M8) without a second convention.

Kafka allows `[a-zA-Z0-9._-]`, max 249 chars, and warns on mixing `.` and `_` (metric-name
collision). We therefore use `.` as the only separator and never `_`.

## Decision

**Base topics:** `<context>.<event-name>.v<major>`

- `<context>` — bounded context, plural noun: `payments`, `refunds`, `ledger`, `webhooks`,
  `merchants`, `psp`.
- `<event-name>` — `<aggregate>-<past-participle>`, kebab-case. Events are facts, so the verb
  MUST be past tense: `payment-requested`, `payment-status-changed`, `funds-reserved`.
- `v<major>` — schema **major** version. Always present, even on v1.

Examples: `payments.payment-requested.v1`, `refunds.funds-reserved.v1`,
`merchants.merchant-config-changed.v1`.

**Retry / DLQ topics:** `<base-topic>[.<consumer-app>].retry.<delay>` and
`<base-topic>[.<consumer-app>].dlq`, where `<delay>` is `5s` / `1m` / `15m`. The
`<consumer-app>` segment MUST be present when the base topic has more than one consumer group
(a poison pill for one consumer is not poison for another) and MUST be omitted when it has
exactly one. So: `payments.payment-status-changed.v1.ledger.dlq`, but
`webhooks.webhook-delivery-requested.v1.retry.5s`.

**Request-reply topics** (M12) use the same base rule with a noun instead of a past
participle, because they are commands/queries, not facts: `psp.provider-status-query.v1`,
`psp.provider-status-reply.v1`.

**Versioning rule.** Schema Registry compatibility is `BACKWARD` (ADR-0006's sibling concern,
enforced in M9). Therefore:

- **Compatible change** (add optional field with default, add enum symbol at the end, widen
  doc): evolve the subject in Schema Registry. **Topic name does not change.**
- **Breaking change** (remove/rename a field, change a type, change semantics of an existing
  field): create `...v2` as a **new topic**. Producers dual-write v1 and v2 for one migration
  window; consumers move one at a time; v1 is deleted only after its retention has elapsed
  with zero consumer lag and no traffic.

Renaming a field is a breaking change even when the wire format survives — semantics moved.
There is no `v1.1`; the suffix carries the major only.

## Consequences

**Positive**
- Grep for `payments.` finds every payment topic including its retry chain and DLQ.
- A breaking change never requires coordinated big-bang deployment: v1 and v2 coexist.
- ACLs can be written as prefix rules (`payments.*` for realtime-gateway, M14).

**Negative / accepted costs**
- Names are long (`webhooks.webhook-delivery-requested.v1.retry.15m` = 47 chars). Acceptable
  against the 249-char limit; the Streams `application.id` prefix is the only real pressure.
- The optional `<consumer-app>` segment is ambiguity waiting to happen: if a second consumer
  group is added to a topic that already has a bare `.dlq`, that DLQ must be renamed. Adding a
  consumer to an existing topic is therefore a topic-map review item.
- Dual-write windows during a v2 migration mean temporary double storage and double produce
  cost.

**Follow-ups**
- M2: topic creation script uses these names. M18: same names become `KafkaTopic` CR
  `metadata.name` (lowercase + `.` are legal in k8s object names).
- Subject naming strategy is `TopicNameStrategy`, so subjects derive as
  `<topic>-value` / `<topic>-key` for free (M9).

## Alternatives considered

**No version suffix; rely on Schema Registry alone.** Works until the first breaking change,
at which point the only options are a big-bang cutover or a `NONE` compatibility level. The
suffix costs nine characters and buys a rollback path.

**Version inside the subject only (`payments.payment-requested-value-v2`).** Keeps topic count
down, but consumers cannot subscribe to "only v2" — they must deserialize and discard, and
offsets are shared between versions. Rejected.

**`<company>.<domain>.<event>` with an org prefix.** Correct for a multi-tenant cluster; pure
noise for a single-system learning cluster. Rejected as premature.

**Snake_case or `_` separators.** Kafka's own metrics mangle mixed `.`/`_` names. Rejected.
