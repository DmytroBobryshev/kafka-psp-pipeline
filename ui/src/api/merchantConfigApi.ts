import { apiFetch } from "./client";
import type { MerchantConfigResponse, UpsertMerchantConfigRequest } from "./types";

/**
 * Write side of the config editor. Writes land on the compacted topic
 * merchants.merchant-config-changed.v1 (payment-api blocks on the broker ack, so a 200 means
 * "on the topic"); the READ side lives on analytics (analyticsApi.getMerchantConfigView),
 * because payment-api deliberately has no GET - reading config means reading the topic.
 */
export const upsertMerchantConfig = (merchantId: string, body: UpsertMerchantConfigRequest) =>
  apiFetch<MerchantConfigResponse>(`/api/merchants/${encodeURIComponent(merchantId)}/config`, {
    method: "PUT",
    body: JSON.stringify(body),
  });

/** 202: publishes a tombstone - downstream GlobalKTables drop the merchant on compaction. */
export const deleteMerchantConfig = (merchantId: string) =>
  apiFetch<void>(`/api/merchants/${encodeURIComponent(merchantId)}/config`, { method: "DELETE" });
