import { apiFetch } from "./client";
import type { MerchantStatus } from "./types";

export interface MerchantView {
  merchantId: string;
  displayName: string;
  status: MerchantStatus;
  payoutCurrency: string;
  allowedCurrencies: string[];
  webhookUrl: string | null;
  declineRateAlertThresholdBps: number;
  paymentExpirationSeconds?: number;
  updatedAt: string;
}

export interface MerchantListResponse {
  items: MerchantView[];
  page: number;
  size: number;
  total: number;
}

export const listMerchants = (params: { status?: string; page?: number; size?: number } = {}) => {
  const q = new URLSearchParams();
  if (params.status) q.set("status", params.status);
  q.set("page", String(params.page ?? 0));
  q.set("size", String(params.size ?? 50));
  return apiFetch<MerchantListResponse>(`/api/merchants?${q}`);
};

export const getMerchant = (id: string) =>
  apiFetch<MerchantView>(`/api/merchants/${encodeURIComponent(id)}`);
