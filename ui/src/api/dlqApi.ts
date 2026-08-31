import { apiFetch } from "./client";
import type { DlqReplayResponse } from "./types";

/**
 * Replay endpoints live with the service that OWNS each DLQ (its error handler wrote it, its
 * idempotency makes the replay safe) - browsing is centralized (clusterApi.peekDlq), replaying
 * is not. The Connect sink's DLQ has no owning Spring service, hence no replay entry here.
 */
export const DLQ_TOPICS = [
  {
    topic: "webhooks.webhook-delivery-requested.v2.dlq",
    owner: "webhook-notifier",
    replayPath: "/api/webhooks/dlq/replay",
  },
  {
    topic: "payments.payment-requested.v1.psp-connector.dlq",
    owner: "psp-connector",
    replayPath: "/api/psp-connector/dlq/replay",
  },
  {
    topic: "payments.payment-status-changed.v1.ledger.dlq",
    owner: "ledger",
    replayPath: "/api/ledger/dlq/replay",
  },
  {
    topic: "ledger.ledger-entry-recorded.v1.mongo-audit-sink.dlq",
    owner: "kafka-connect (no replay API - sink DLQ has no owning service)",
    replayPath: null,
  },
] as const;

export const replayDlq = (replayPath: string, maxRecords: number) =>
  apiFetch<DlqReplayResponse>(`${replayPath}?maxRecords=${maxRecords}`, { method: "POST" });
