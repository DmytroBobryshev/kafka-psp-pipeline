import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getAllWindows, getMerchantWindows, getProjectedWindows, getStreamsState } from "../api/analyticsApi";
import { listPayments } from "../api/paymentsApi";
import { Link, useNavigate } from "@tanstack/react-router";
import { KebabMenu } from "../components/KebabMenu";
import { FALLBACK_BADGE, STATUS_BADGE } from "../lib/badges";
import { useCopy } from "../lib/clipboard";
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
  const navigate = useNavigate();
  const { copy, copiedKey } = useCopy();
  const [lookback, setLookback] = useState<number>(15);
  const [projected, setProjected] = useState(false);
  const trimmed = merchantId.trim();

  const streamsState = useQuery({
    queryKey: ["streams-state"],
    queryFn: getStreamsState,
  });

  const windows = useQuery<WindowMetricsResponse[], Error>({
    queryKey: ["windows", trimmed, lookback, projected],
    queryFn: () =>
      trimmed
        ? projected
          ? getProjectedWindows(trimmed, lookback)
          : getMerchantWindows(trimmed, lookback)
        : getAllWindows(lookback),
    enabled: !projected || trimmed.length > 0,
  });

  const totals = useQuery({
    queryKey: ["op-totals", trimmed],
    queryFn: async () => {
      const m = trimmed || undefined;
      const [all, ok, failed, created] = await Promise.all([
        listPayments({ merchantId: m, page: 0, size: 1 }),
        listPayments({ merchantId: m, status: "SUCCEEDED", page: 0, size: 1 }),
        listPayments({ merchantId: m, status: "FAILED", page: 0, size: 1 }),
        listPayments({ merchantId: m, status: "CREATED", page: 0, size: 1 }),
      ]);
      return { all: all.total, ok: ok.total, failed: failed.total, created: created.total };
    },
  });
  const latest = useQuery({
    queryKey: ["op-latest", trimmed],
    queryFn: () => listPayments({ merchantId: trimmed || undefined, page: 0, size: 5 }),
  });

  const restoring =
    (streamsState.data && !streamsState.data.storeReady) ||
    (windows.error instanceof ApiError && windows.error.status === 503);

  return (
    <main className="mx-auto max-w-[1500px] px-6 py-8">
      <h2 className="mb-1 text-base font-semibold text-slate-900">Operations overview</h2>
      <p className="mb-4 text-xs text-slate-600">
        Totals and latest transactions from payment-api; windowed metrics below come live from the
        Kafka Streams store.
      </p>
      <div className="mb-6 flex flex-wrap items-end gap-4">
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Merchant ID (empty = all)</span>
          <input
            value={merchantId}
            onChange={(e) => setMerchantId(e.target.value)}
            placeholder="merchant-1"
            className="w-64 rounded-md border border-slate-400 px-3 py-2 font-mono text-sm"
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Lookback</span>
          <select
            value={lookback}
            onChange={(e) => setLookback(Number(e.target.value))}
            className="rounded-md border border-slate-400 px-3 py-2 text-sm"
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
        <span className="pb-2 text-xs text-slate-500">
          {streamsState.data
            ? `streams: ${streamsState.data.clientState}`
            : "streams state unknown"}
        </span>
      </div>

      <div className="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-4">
        {[
          { label: "All operations", value: totals.data?.all, tone: "text-slate-900" },
          { label: "Succeeded", value: totals.data?.ok, tone: "text-emerald-700" },
          { label: "Failed", value: totals.data?.failed, tone: "text-rose-700" },
          { label: "In flight (CREATED)", value: totals.data?.created, tone: "text-amber-700" },
        ].map((c) => (
          <div key={c.label} className="rounded-lg border border-slate-300 bg-white px-4 py-3">
            <div className="text-xs text-slate-600">{c.label}</div>
            <div className={`text-2xl font-semibold ${c.tone}`}>{c.value ?? "–"}</div>
          </div>
        ))}
      </div>

      <div className="mb-6 rounded-lg border border-slate-300 bg-white">
        <div className="flex items-center justify-between px-4 py-3">
          <h3 className="text-sm font-semibold text-slate-700">Latest operations</h3>
          <Link to="/payments" search={{ merchantId: undefined, paymentId: undefined }} className="rounded-md border border-slate-400 bg-white px-2.5 py-1 text-xs font-medium text-slate-800 shadow-sm hover:bg-slate-200">
            open transactions panel →
          </Link>
        </div>
        <table className="w-full text-sm">
          <thead className="bg-slate-100 text-left text-xs uppercase tracking-wide text-slate-600">
            <tr>
              <th className="px-4 py-3">Payment</th>
              <th className="px-4 py-3">Merchant</th>
              <th className="px-4 py-3 text-right">Amount</th>
              <th className="px-4 py-3">Status</th>
              <th className="px-4 py-3">Created</th>
              <th className="w-10 px-2 py-3" />
            </tr>
          </thead>
          <tbody>
            {(latest.data?.items ?? []).map((p) => (
              <tr
                key={p.id}
                onClick={() => navigate({ to: "/payments", search: { merchantId: undefined, paymentId: p.id } })}
                className="cursor-pointer border-t border-slate-200 hover:bg-slate-100"
              >
                <td className="px-4 py-2 font-mono text-xs">{p.id.slice(0, 8)}…</td>
                <td className="px-4 py-2">{p.merchantId}</td>
                <td className="px-4 py-2 text-right tabular-nums">{p.amount} {p.currency}</td>
                <td className="px-4 py-2">
                  <span className={`rounded px-2 py-0.5 text-xs font-semibold ${STATUS_BADGE[p.status] ?? FALLBACK_BADGE}`}>
                    {p.status}
                  </span>
                </td>
                <td className="px-4 py-2 text-xs text-slate-600">{new Date(p.createdAt).toLocaleString()}</td>
                <td className="px-2 py-2 text-right">
                  <KebabMenu
                    items={[
                      {
                        label: "View in transactions →",
                        onClick: () => navigate({ to: "/payments", search: { merchantId: undefined, paymentId: p.id } }),
                      },
                      {
                        label: copiedKey === p.id ? "Copied ✓" : "Copy payment ID",
                        onClick: () => copy(p.id, p.id),
                      },
                      {
                        label: "All from this merchant",
                        onClick: () => navigate({ to: "/payments", search: { merchantId: p.merchantId, paymentId: undefined } }),
                      },
                    ]}
                  />
                </td>
              </tr>
            ))}
            {latest.data?.items.length === 0 && (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-slate-500">No operations yet.</td></tr>
            )}
          </tbody>
        </table>
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

      <h3 className="mb-2 text-sm font-semibold text-slate-700">Windowed metrics (1-minute tumbling, Kafka Streams)</h3>
      <div className="overflow-x-auto rounded-lg border border-slate-300 bg-white">
        <table className="w-full text-sm">
          <thead className="bg-slate-100 text-left text-xs uppercase tracking-wide text-slate-600">
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
                className="border-t border-slate-200 hover:bg-slate-100"
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
                    <span className="rounded bg-rose-100 px-2 py-0.5 text-xs font-semibold text-rose-800 ring-1 ring-inset ring-rose-600/40">
                      over threshold
                    </span>
                  ) : (
                    <span className="text-xs text-slate-500">ok</span>
                  )}
                </td>
              </tr>
            ))}
            {windows.data?.length === 0 && (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center text-slate-500">
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
