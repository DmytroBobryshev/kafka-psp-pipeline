import { apiFetch } from "./client";
import type { PaymentResponse, RefundResponse } from "./types";

/**
 * The transactions panel's API surface. List/get/refunds come from payment-api (whose payments
 * table is now kept honest by its payment-status-changed listener); provider-status is M12's
 * request-reply over Kafka; deliveries come from webhook-notifier's Mongo attempt log. The two
 * "unknown row" shapes (provider status, delivery) are rendered generically by the page, so a
 * backend field rename degrades to a label change, never a blank screen.
 */
export interface PaymentListResponse {
  items: PaymentResponse[];
  page: number;
  size: number;
  total: number;
}

export const listPayments = (params: { merchantId?: string; status?: string; page: number; size: number }) => {
  const q = new URLSearchParams();
  if (params.merchantId) q.set("merchantId", params.merchantId);
  if (params.status) q.set("status", params.status);
  q.set("page", String(params.page));
  q.set("size", String(params.size));
  return apiFetch<PaymentListResponse>(`/api/payments?${q}`);
};

export const getPayment = (id: string) => apiFetch<PaymentResponse>(`/api/payments/${id}`);

export interface StatusHistoryEntry {
  status: "CREATED" | "PENDING" | "IPN_RECEIVED" | "VERIFIED" | "SUCCEEDED" | "FAILED" | string;
  occurredAt: string;
  eventId: string | null;
  source: string;
  providerReference?: string | null;
}

export const getPaymentHistory = (id: string) =>
  apiFetch<{ items: StatusHistoryEntry[] }>(`/api/payments/${id}/history`).then((r) => r.items);

export const getRefundHistory = (paymentId: string, refundId: string) =>
  apiFetch<{ items: StatusHistoryEntry[] }>(
    `/api/payments/${paymentId}/refunds/${refundId}/history`,
  ).then((r) => r.items);

export const getPaymentRefunds = (id: string) =>
  apiFetch<RefundResponse[]>(`/api/payments/${id}/refunds`);

export const getProviderStatus = (id: string) =>
  apiFetch<Record<string, unknown>>(`/api/payments/${id}/provider-status`);

/** One LOGICAL delivery (attempts grouped by causation event), newest first. */
export interface WebhookDeliveryView {
  id: string;
  eventType: "PAYMENT_STATUS_CHANGED" | "REFUND_COMPLETED" | "REFUND_FAILED" | string;
  paymentId: string;
  refundId: string | null;
  merchantId: string;
  url: string;
  status: "SUCCESS" | "RETRYABLE_FAILURE" | "NON_RETRYABLE_FAILURE" | string;
  attempts: number;
  lastAttemptAt: string;
  createdAt: string;
}

export const getWebhookDeliveries = (params: { paymentId?: string; refundId?: string; merchantId?: string }) => {
  const q = new URLSearchParams();
  if (params.paymentId) q.set("paymentId", params.paymentId);
  if (params.refundId) q.set("refundId", params.refundId);
  if (params.merchantId) q.set("merchantId", params.merchantId);
  q.set("limit", "25");
  return apiFetch<WebhookDeliveryView[]>(`/api/webhooks/deliveries?${q}`);
};
