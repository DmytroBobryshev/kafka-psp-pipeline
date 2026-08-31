import { apiFetch } from "./client";
import type { RefundResponse, RefundStateResponse, RequestRefundRequest } from "./types";

/** POST returns 202 - the saga runs asynchronously; the tracker page watches it over SSE. */
export const requestRefund = (paymentId: string, body: RequestRefundRequest) =>
  apiFetch<RefundResponse>(`/api/payments/${encodeURIComponent(paymentId)}/refunds`, {
    method: "POST",
    body: JSON.stringify(body),
  });

/** Ledger's local view of the saga - current status only, 404 until ledger consumes step 1. */
export const getRefundState = (refundId: string) =>
  apiFetch<RefundStateResponse>(`/api/refunds/${encodeURIComponent(refundId)}`);
