# ui (M17 slice 1 — create-payment form + live event timeline)

The minimal first slice of `docs/PLAN.md`'s M17, pulled forward per the plan's "Order
exceptions allowed" note ("pull a minimal M17 (payment form + live timeline) forward right
after M12 for motivation"). Exactly two things, built well:

1. A **create-payment form** (merchantId, amount, currency) that `POST`s to payment-api.
2. The **live event timeline** — every event for that payment's id appears in real time over
   SSE, in the order it actually happened, as pushed by realtime-gateway.

Nothing else from the full M17 brief is here yet — see "What's not built yet" below.

## Stack

Vite 6, React 19, TypeScript 5.9 (strict), TanStack Query 5, Tailwind CSS 4
(`@tailwindcss/vite`, no PostCSS config needed). No shadcn/ui, no TanStack Router, no
`openapi-typescript` codegen — a single page needs none of them; see "What's not built yet".

## How to run

Prerequisites: the Kafka/Postgres/Connect stack (`infra/compose`) already running, and
**payment-api** (8085) + **realtime-gateway** (8090) started — see their READMEs for exact
commands. For a fully live timeline (payment-requested *and* payment-status-changed), also
start **psp-connector** (8086), which consumes `payments.payment-requested.v1` and produces
`payments.payment-status-changed.v1`.

```bash
cd ui
pnpm install
pnpm dev       # http://localhost:5173
```

```bash
pnpm build     # tsc -b && vite build -> dist/
pnpm preview   # serve the production build locally
```

## What this demonstrates

Submitting the form `POST`s to `/api/payments` (payment-api), then opens
`GET /api/realtime/events?paymentId=<id>` (realtime-gateway) over SSE and renders every event
that arrives, in arrival order, as a vertical timeline. Each card shows:

- the event type and the producing Kafka topic's timestamp (`occurredAt`, wall-clock **and**
  elapsed-since-payment-created, so the psp-connector's simulated 100 ms–5 s provider latency
  is visible, not just asserted)
- the envelope's `eventId`
- `causationId`, when the gateway supplies one (see "Compromises" — it currently doesn't)
- `source`, `topic`, `partition`, `key`, if the gateway ever adds them to its SSE payload
  (typed and rendered conditionally, again see "Compromises")
- the domain fields that differ per event type (`status`, `reason`, `providerReference`,
  `refundId`), skipping whichever are null so the card stays scannable

The connection state is always visible, not inferred: **Idle → Connecting… → Live →
Reconnecting…/Closed**, with a manual **Reconnect** action once the browser's own SSE retry
gives up.

## Why a proxy, not CORS

`vite.config.ts` proxies `/api/payments/*` → `http://localhost:8085` and `/api/realtime/*` →
`http://localhost:8090`. The browser only ever calls same-origin `/api/*`; Vite forwards the
request server-side.

The alternative — turning on CORS in payment-api and realtime-gateway — was rejected because
CORS is purely a browser-origin concern, and neither service has any other reason to accept
cross-origin requests: per ADR-0004, payment-api is the only externally-reachable service and
everything else talks Kafka internally; a browser calling either service directly is itself a
dev-only convenience that api-gateway (M16) will eventually front properly. Adding
`Access-Control-Allow-Origin` to two backend services' production config to satisfy a *dev
server's* cross-port requirement would be solving a Vite problem inside the backend. The proxy
keeps this entirely inside `ui/`, touches no backend code, and disappears in production once a
real gateway sits in front of both services on one origin.

## SSE lifecycle

Owned by `src/hooks/useEventStream.ts`. One `EventSource` per active `paymentId`:

- **Connect on submit** — the hook only opens a connection once a `paymentId` exists (i.e.
  after the form's mutation succeeds); it is not connected on page load.
- **Named events, not `onmessage`** — `PaymentTimelineController` sends every SSE event with
  an explicit `event:` name equal to its Avro event type (never the default unnamed
  `"message"`), so the hook attaches one listener per known event type
  (`api/types.ts KNOWN_EVENT_TYPES`, all 7 topics realtime-gateway consumes) rather than a
  single generic handler that would silently see nothing.
- **Visible state, not silent failure** — `idle` → `connecting` → `live` on `onopen`. On
  `onerror`, the native `EventSource`'s own `readyState` decides what the user sees: still
  `CONNECTING` means the browser is auto-retrying, surfaced as `reconnecting`; `CLOSED` means
  the browser has given up for good (e.g. the initial request failed, or the gateway completed
  the emitter after its 5-minute idle timeout), surfaced as `closed` with a manual
  **Reconnect** button — deliberately *not* auto-retried again, because an infinite silent
  retry loop would hide a genuinely dead connection exactly as badly as no error handling at
  all.
- **Close on unmount / on payment change** — the `useEffect` cleanup calls
  `source.close()` unconditionally, both on unmount and just before a new `paymentId` opens
  its own connection, so no stale subscription is ever left registered against the gateway's
  `InMemorySseConnectionRegistry`.

## RFC 7807 error rendering

`libs/common-web`'s `GlobalExceptionHandler` (shared by every service) returns `400` with a
`ProblemDetail` body; for a `@Valid @RequestBody` failure it adds a per-field `errors` map
(`{"merchantId": "...", "amount": "...", "currency": "..."}`). `api/paymentApi.ts` throws a
`PaymentApiError` carrying the parsed problem body; `PaymentForm` maps `errors` onto the
matching input's inline message and falls back to the top-level `detail`/`title` for any
non-field problem (e.g. a `500`).

## Compromises

- **`causationId`/`source`/topic/partition/key are typed but not currently populated.** ADR-0002's
  `EventEnvelope` carries `source` and `causationId` on every event, and the task brief asks
  for both plus Kafka topic/partition/key "if the gateway exposes them." Checked against the
  live source: `realtime-gateway`'s `RealtimeEventMapper`/`RealtimeEvent` (its wire DTO) only
  forwards `eventId`, `eventType`, `occurredAt`, and the domain fields — `envelope.source` and
  `envelope.causationId` are read off the Avro record inside payment-api/psp-connector but
  never copied into the flattened `RealtimeEvent` the gateway serializes to SSE, and no
  topic/partition/key are attached at all. This is a real, verified gap in the current
  `realtime-gateway` build, not a UI shortcoming — fixing it means editing
  `services/realtime-gateway` Java source, which is out of scope for a `ui/`-only slice per
  the task brief. `src/api/types.ts` types all five fields as optional and every UI surface
  (`EventCard`) renders them the moment they're present, so no UI change will be needed if/when
  the gateway starts including them — today they simply don't appear, and the causationId row
  explicitly reads "not exposed by gateway" instead of silently omitting the field, so a viewer
  isn't left wondering why it's missing.
- **No `openapi-typescript` codegen.** `src/api/types.ts` hand-writes the payment-api and
  realtime-gateway wire shapes with a comment marking where
  `openapi-typescript http://localhost:8085/v3/api-docs` would plug in. Skipped per the task
  brief to keep this first slice dependency-light; worth doing once more than one page needs
  the same generated types.
- **One payment at a time.** Submitting a new payment closes any existing SSE connection and
  starts a fresh timeline; there's no multi-payment history view. In scope for the full M17
  merchant dashboard, not this slice.
- **Currency is a fixed EUR/USD/GBP `<select>`**, not free text validated against ISO-4217
  client-side — the server validates the real constraint (`[A-Z]{3}`) and the form renders
  whatever it rejects, so this is a UX nicety, not a correctness gap.

## What's not built yet

Per the task's scope, deliberately absent (full M17, later): merchant dashboard (live windowed
metrics), DLQ console, merchant config editor, cluster ops (consumer lag / topic list), refund
tracker (saga visualization). Also not built: TanStack Router (one page doesn't need routing),
shadcn/ui (Tailwind alone was enough for this surface), and the `openapi-typescript` codegen
step noted above.

## Verified against the live stack

Ran `pnpm build` (clean, zero TypeScript errors) and `pnpm dev`, confirmed `index.html` /
`main.tsx` render. Then started payment-api, realtime-gateway, and psp-connector against the
already-running `infra/compose` stack and drove a real payment through the Vite proxy
end-to-end (`curl` standing in for the browser's `fetch`/`EventSource` — same `/api/*` paths,
same origin, same proxy config the UI code actually uses):

```
POST http://localhost:5173/api/payments {"merchantId":"merchant-ui-final-proof","amount":199.99,"currency":"EUR"}
-> 201 {"id":"75b23417-75b2-4f9a-9fcc-9829b9282516", ..., "createdAt":"2026-08-11T22:33:01.161270Z"}

GET http://localhost:5173/api/realtime/events?paymentId=75b23417-75b2-4f9a-9fcc-9829b9282516

id:019ff2f5-29ea-7efb-b150-0552eff4502c
event:payments.payment-requested.v1
data:{"eventId":"019ff2f5-29ea-7efb-b150-0552eff4502c","eventType":"payments.payment-requested.v1",
      "occurredAt":"2026-08-11T22:33:01.162Z","paymentId":"75b23417-...","merchantId":"merchant-ui-final-proof",
      "refundId":null,"status":"CREATED","reason":null,"providerReference":null}

id:019ff2f5-45e1-7f51-ba64-e1ae6d1d9cb5
event:payments.payment-status-changed.v1
data:{"eventId":"019ff2f5-45e1-7f51-ba64-e1ae6d1d9cb5","eventType":"payments.payment-status-changed.v1",
      "occurredAt":"2026-08-11T22:33:08.321Z","paymentId":"75b23417-...","merchantId":"merchant-ui-final-proof",
      "refundId":null,"status":"SUCCEEDED","reason":null,"providerReference":"3e4ebe4c-13fa-43f7-96ba-c113a377bcce"}
```

Two events, same connection, in order: `payments.payment-requested.v1` ~1 ms after
`createdAt` (outbox → Debezium → Kafka → gateway → SSE), then
`payments.payment-status-changed.v1` ~7.2 s later (psp-connector's simulated provider latency
window). Neither carries a `causationId` — confirming the gap documented above empirically, not
just by reading the source. Also verified the RFC 7807 path through the same proxy: an invalid
payload (`{"merchantId":"","amount":-5,"currency":"eur"}`) returned `400` with
`content-type: application/problem+json` and an `errors` object keyed exactly `merchantId`,
`amount`, `currency` — the same keys `PaymentForm`'s fields use, so each message lands under
the right input.

All three services were stopped after verification; nothing outside `ui/` was modified.
