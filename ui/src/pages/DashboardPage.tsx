import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getAllWindows, getMerchantWindows, getProjectedWindows, getStreamsState } from "../api/analyticsApi";
import { ApiError } from "../api/client";
import type { WindowMetricsResponse } from "../api/types";

const LOOKBACKS = [5, 15, 60] as const;

/**
 * Page 2: merchant dashboard - live windowed metrics straight from the Kafka Streams store
 * (interactive queries), or the Mongo projection when "projected" is on. Poll-based (5 s):
 * realtime-gateway deliberately does not stream the metrics changelog, so the store IS the API.
 * A 503 from any store endpoint means the Streams client is restoring - rendered as a banner,
 * never masked, because "the store is rebuilding from the changelog" is exactly the Kafka
 * lesson this page exists to show.
 */
export function DashboardPage() {
  const [merchantId, setMerchantId] = useState("");
  const [lookback, setLookback] = useState<number>(15);
  const [projected, setProjected] = useState(false);
  const trimmed = merchantId.trim();

  const streamsState = useQuery({
    queryKey: ["streams-state"],
    queryFn: getStreamsState,
    refetchInterval: 5000,
  });

  const windows = useQuery<WindowMetricsResponse[], Error>({
    queryKey: ["windows", trimmed, lookback, projected],
    queryFn: () =>
      trimmed
        ? projected
          ? getProjectedWindows(trimmed, lookback)
          : getMerchantWindows(trimmed, lookback)
        : getAllWindows(lookback),
    refetchInterval: 5000,
    enabled: !projected || trimmed.length > 0,
  });

  const restoring =
    (streamsState.data && !streamsState.data.storeReady) ||
    (windows.error instanceof ApiError && windows.error.status === 503);

  return (
    <main className="mx-auto max-w-6xl px-6 py-8">
      <div className="mb-6 flex flex-wrap items-end gap-4">
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Merchant ID (empty = all)</span>
          <input
            value={merchantId}
            onChange={(e) => setMerchantId(e.target.value)}
            placeholder="merchant-1"
            className="w-64 rounded-md border border-slate-300 px-3 py-2 font-mono text-sm"
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Lookback</span>
          <select
            value={lookback}
            onChange={(e) => setLookback(Number(e.target.value))}
            className="rounded-md border border-slate-300 px-3 py-2 text-sm"
          >
            {LOOKBACKS.map((m) => (
              <option key={m} value={m}>
                {m} min
              </option>
            ))}
          </select>
        </label>
        <label className="flex items-center gap-2 pb-2 text-sm text-slate-700">
          <input
            type="checkbox"
            checked={projected}
            onChange={(e) => setProjected(e.target.checked)}
            disabled={!trimmed}
          />
          projected (Mongo, survives restarts; per-merchant only)
        </label>
        <span className="pb-2 text-xs text-slate-400">
          {streamsState.data
            ? `streams: ${streamsState.data.clientState}`
            : "streams state unknown"}
        </span>
      </div>

      {restoring && (
        <div className="mb-4 rounded-md border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          The Streams state store is restoring from its changelog topic - interactive queries
          return 503 until the RocksDB store has caught up. This banner is that restore, live.
        </div>
      )}

      {windows.error && !restoring && (
        <p className="text-sm text-rose-600">Failed to load windows: {windows.error.message}</p>
      )}

      <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
            <tr>
              <th className="px-4 py-3">Window</th>
              <th className="px-4 py-3">Merchant</th>
              <th className="px-4 py-3 text-right">Total</th>
              <th className="px-4 py-3 text-right">Declined</th>
              <th className="px-4 py-3 text-right">Decline rate</th>
              <th className="px-4 py-3 text-right">Avg latency</th>
              <th className="px-4 py-3">Alert</th>
            </tr>
          </thead>
          <tbody>
            {(windows.data ?? []).map((w) => (
              <tr
                key={`${w.merchantId}-${w.windowStart}`}
                className="border-t border-slate-100 hover:bg-slate-50"
              >
                <td className="px-4 py-2 font-mono text-xs">
                  {new Date(w.windowStart).toLocaleTimeString()}–
                  {new Date(w.windowEnd).toLocaleTimeString()}
                  {w.open && <span className="ml-1 text-emerald-600">(open)</span>}
                </td>
                <td className="px-4 py-2">{w.merchantDisplayName ?? w.merchantId}</td>
                <td className="px-4 py-2 text-right">{w.totalCount}</td>
                <td className="px-4 py-2 text-right">{w.declinedCount}</td>
                <td className="px-4 py-2 text-right">{(w.declineRateBps / 100).toFixed(2)}%</td>
                <td className="px-4 py-2 text-right">
                  {w.avgPipelineLatencyMillis != null ? `${Math.round(w.avgPipelineLatencyMillis)} ms` : "–"}
                </td>
                <td className="px-4 py-2">
                  {w.declineRateAlert ? (
                    <span className="rounded bg-rose-100 px-2 py-0.5 text-xs font-medium text-rose-700">
                      over threshold
                    </span>
                  ) : (
                    <span className="text-xs text-slate-400">ok</span>
                  )}
                </td>
              </tr>
            ))}
            {windows.data?.length === 0 && (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center text-slate-400">
                  No windows in the lookback - create payments on the Timeline page to feed the
                  1-minute tumbling windows.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </main>
  );
}
