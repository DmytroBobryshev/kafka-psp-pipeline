import { apiFetch } from "./client";
import type { MerchantConfigResponse, StreamsStateResponse, WindowMetricsResponse } from "./types";

/**
 * Read side of two pages: the merchant dashboard (windowed metrics from the Kafka Streams
 * RocksDB store / Mongo projection) and the config editor (the GlobalKTable view of the
 * compacted topic - reading config means reading the topic, per M10's design).
 *
 * Store-backed endpoints return 503 while the Streams client is restoring - callers surface
 * that state explicitly (the dashboard renders a "restoring" banner) instead of masking it.
 */
export const getStreamsState = () => apiFetch<StreamsStateResponse>("/api/analytics/state");

export const getAllWindows = (lookbackMinutes: number) =>
  apiFetch<WindowMetricsResponse[]>(`/api/analytics/windows?lookbackMinutes=${lookbackMinutes}`);

export const getMerchantWindows = (merchantId: string, lookbackMinutes: number) =>
  apiFetch<WindowMetricsResponse[]>(
    `/api/analytics/merchants/${encodeURIComponent(merchantId)}/windows?lookbackMinutes=${lookbackMinutes}`,
  );

export const getProjectedWindows = (merchantId: string, lookbackMinutes: number) =>
  apiFetch<WindowMetricsResponse[]>(
    `/api/analytics/merchants/${encodeURIComponent(merchantId)}/windows/projected?lookbackMinutes=${lookbackMinutes}`,
  );

export const getMerchantConfigView = (merchantId: string) =>
  apiFetch<MerchantConfigResponse>(`/api/analytics/merchants/${encodeURIComponent(merchantId)}/config`);
