// Hand-written wire types for payment-api and realtime-gateway.
//
// GENERATION NOTE: PLAN.md's M17 stack calls for these to be generated from payment-api's
// springdoc OpenAPI document via `openapi-typescript` (e.g.
// `openapi-typescript http://localhost:8085/v3/api-docs -o src/api/generated.ts`). This minimal
// first slice hand-writes the handful of shapes it needs instead - see ui/README.md "What's not
// built yet" for why that step is deferred rather than skipped.

/** POST /api/payments request body (payment-api's CreatePaymentRequest). */
export interface CreatePaymentRequest {
  merchantId: string;
  amount: number;
  currency: string;
}

/** POST /api/payments 201 response body (payment-api's PaymentResponse). */
export interface PaymentResponse {
  id: string;
  merchantId: string;
  amount: number;
  currency: string;
  status: string;
  createdAt: string;
  /** When the outcome landed (status listener's stamp); null while CREATED / for legacy rows. */
  statusUpdatedAt?: string | null;
}

/**
 * RFC 7807 Problem Details, as produced by every service's shared
 * `libs/common-web` GlobalExceptionHandler.
 *
 * `errors` is a per-field validation-message map and is only ever present on a 400 raised by a
 * `MethodArgumentNotValidException` (a `@Valid @RequestBody` failure) - other problem responses
 * (404, 500, etc.) omit it entirely.
 */
export interface ProblemDetail {
  type?: string;
  title?: string;
  status?: number;
  detail?: string;
  instance?: string;
  correlationId?: string;
  errors?: Record<string, string>;
}

/**
 * The flattened event realtime-gateway pushes over SSE
 * (`domain.model.RealtimeEvent` in services/realtime-gateway).
 *
 * KNOWN GAP (see ui/README.md "Compromises"): the gateway's mapper does not currently propagate
 * `envelope.source` or `envelope.causationId` (both present on every ADR-0002 EventEnvelope)
 * into this flattened shape, and does not expose the Kafka topic/partition/key the record
 * arrived on. Those fields are typed optional below so this UI renders them automatically the
 * moment a future gateway build starts including them - no UI change required - rather than
 * inventing data the backend does not actually send.
 */
export interface RealtimeEvent {
  eventId: string;
  eventType: string;
  occurredAt: string;
  paymentId: string;
  merchantId: string;
  refundId?: string | null;
  status?: string | null;
  reason?: string | null;
  providerReference?: string | null;

  // Present in the ADR-0002 envelope; not yet forwarded by realtime-gateway's RealtimeEvent DTO.
  source?: string;
  causationId?: string;

  // Present only if a future gateway build annotates events with their Kafka coordinates.
  topic?: string;
  partition?: number;
  key?: string;
}

/** Every Avro event type realtime-gateway consumes and re-emits as a named SSE event. */
export const KNOWN_EVENT_TYPES = [
  "payments.payment-requested.v1",
  "payments.payment-status-changed.v1",
  "refunds.refund-requested.v1",
  "refunds.funds-reserved.v1",
  "refunds.refund-completed.v1",
  "refunds.refund-failed.v1",
  "refunds.reservation-released.v1",
] as const;

// ---------------------------------------------------------------------------------------------
// M17 full build: wire types for the five remaining pages. payment-api and analytics now serve
// /v3/api-docs (springdoc added in this module) - `pnpm gen:api` regenerates
// src/api/generated/*.ts from the live services, and these hand-written mirrors are kept for
// the services without springdoc (ledger, webhook-notifier, realtime-gateway's ops API).
// ---------------------------------------------------------------------------------------------

/** analytics GET /api/analytics/windows and /merchants/{id}/windows[?/projected] element. */
export interface WindowMetricsResponse {
  merchantId: string;
  merchantDisplayName: string | null;
  windowStart: string;
  windowEnd: string;
  open: boolean;
  totalCount: number;
  declinedCount: number;
  declineRate: number;
  declineRateBps: number;
  avgPipelineLatencyMillis: number | null;
  declineRateAlertThresholdBps: number | null;
  declineRateAlert: boolean;
}

/** analytics GET /api/analytics/state - 503-aware "is the store queryable" probe. */
export interface StreamsStateResponse {
  applicationId: string;
  stateDir: string;
  clientState: string;
  storeReady: boolean;
}

export type MerchantStatus = "ACTIVE" | "SUSPENDED";

/** payment-api PUT body /api/merchants/{merchantId}/config (UpsertMerchantConfigRequest). */
export interface UpsertMerchantConfigRequest {
  displayName: string;
  status: MerchantStatus;
  payoutCurrency: string;
  /** 1..3 ISO codes; payments are accepted only in these. */
  allowedCurrencies: string[];
  webhookUrl?: string | null;
  declineRateAlertThresholdBps: number;
  /** 30..86400; payments still CREATED/PENDING after this become EXPIRED. Absent = 900. */
  paymentExpirationSeconds?: number;
}

/** Shared by payment-api's PUT 200 and analytics' GET /merchants/{id}/config. */
export interface MerchantConfigResponse {
  merchantId: string;
  displayName: string;
  status: MerchantStatus;
  payoutCurrency: string;
  webhookUrl: string | null;
  declineRateAlertThresholdBps: number;
}

/** payment-api POST /api/payments/{paymentId}/refunds body. */
export interface RequestRefundRequest {
  amount: number;
  currency: string;
  reason?: string;
}

/** payment-api POST refund 202 body. */
export interface RefundResponse {
  id: string;
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  status: string;
  reason: string | null;
  createdAt: string;
}

/** ledger GET /api/refunds/{refundId} - the saga's CURRENT state (single row, not history). */
export interface RefundStateResponse {
  refundId: string;
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  status: string;
  reason: string | null;
  createdAt: string;
  updatedAt: string;
}

/** realtime-gateway GET /api/realtime/cluster/topics element. */
export interface TopicInfo {
  name: string;
  partitionCount: number;
  replicationFactor: number;
}

/** realtime-gateway GET /api/realtime/cluster/groups element. */
export interface ConsumerGroupInfo {
  groupId: string;
  state: string;
  memberCount: number;
}

/** realtime-gateway GET /api/realtime/cluster/groups/{groupId}/lag. */
export interface PartitionLag {
  topic: string;
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
}
export interface GroupLagResponse {
  groupId: string;
  totalLag: number;
  partitions: PartitionLag[];
}

/** realtime-gateway GET /api/realtime/cluster/dlq/{topic}/records element - a non-destructive peek. */
export interface DlqRecordView {
  topic: string;
  partition: number;
  offset: number;
  timestamp: string;
  keyString: string | null;
  headers: Record<string, string>;
  valuePreview: string;
  valueBase64: boolean;
}

/** webhook-notifier / psp-connector / ledger POST .../dlq/replay response. */
export interface DlqReplayResponse {
  replayedCount: number;
  dlqTopic: string;
  republishedToTopic: string;
}
