import { useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getMerchantConfigView } from "../api/analyticsApi";
import { deleteMerchantConfig, upsertMerchantConfig } from "../api/merchantConfigApi";
import { ApiError } from "../api/client";
import type { MerchantStatus, UpsertMerchantConfigRequest } from "../api/types";

/**
 * Page 4: merchant config editor. The write goes to payment-api (PUT/DELETE -> compacted topic
 * merchants.merchant-config-changed.v1, broker-acked before the HTTP response); the read comes
 * from analytics' GlobalKTable view of that same topic. The propagation badge measures the gap
 * between the two - "watch downstream services pick it up" from PLAN.md, as a number.
 */
export function MerchantConfigPage() {
  const queryClient = useQueryClient();
  const [merchantId, setMerchantId] = useState("merchant-1");
  const [form, setForm] = useState<UpsertMerchantConfigRequest>({
    displayName: "",
    status: "ACTIVE",
    payoutCurrency: "EUR",
    webhookUrl: "",
    declineRateAlertThresholdBps: 2500,
  });
  const [propagatedMs, setPropagatedMs] = useState<number | null>(null);
  const writeStartedAt = useRef<number>(0);
  const trimmed = merchantId.trim();

  const view = useQuery({
    queryKey: ["merchant-config", trimmed],
    queryFn: () => getMerchantConfigView(trimmed),
    enabled: trimmed.length > 0,
    retry: false,
    // After a write, poll the analytics read side every second until it reflects the change -
    // the GlobalKTable consumes the topic asynchronously, and that delay IS the demo.
    refetchInterval: (query) => {
      if (writeStartedAt.current === 0) return false;
      const fresh = query.state.dataUpdatedAt >= writeStartedAt.current;
      if (fresh && propagatedMs === null) {
        setPropagatedMs(Date.now() - writeStartedAt.current);
        writeStartedAt.current = 0;
        return false;
      }
      return 1000;
    },
  });

  const startWatchingPropagation = () => {
    setPropagatedMs(null);
    writeStartedAt.current = Date.now();
    queryClient.invalidateQueries({ queryKey: ["merchant-config", trimmed] });
  };

  const upsert = useMutation({
    mutationFn: () =>
      upsertMerchantConfig(trimmed, {
        ...form,
        webhookUrl: form.webhookUrl?.trim() ? form.webhookUrl : null,
      }),
    onSuccess: startWatchingPropagation,
  });

  const tombstone = useMutation({
    mutationFn: () => deleteMerchantConfig(trimmed),
    onSuccess: startWatchingPropagation,
  });

  const notFound = view.error instanceof ApiError && view.error.status === 404;

  return (
    <main className="mx-auto grid max-w-6xl gap-8 px-6 py-8 lg:grid-cols-2">
      <section className="rounded-lg border border-slate-200 bg-white p-6">
        <h2 className="mb-4 text-base font-semibold">Write (payment-api → compacted topic)</h2>
        <label className="mb-3 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Merchant ID</span>
          <input
            value={merchantId}
            onChange={(e) => setMerchantId(e.target.value)}
            className="w-full rounded-md border border-slate-300 px-3 py-2 font-mono text-sm"
          />
        </label>
        <label className="mb-3 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Display name</span>
          <input
            value={form.displayName}
            onChange={(e) => setForm({ ...form, displayName: e.target.value })}
            className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
          />
        </label>
        <div className="mb-3 grid grid-cols-2 gap-3">
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-slate-700">Status</span>
            <select
              value={form.status}
              onChange={(e) => setForm({ ...form, status: e.target.value as MerchantStatus })}
              className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
            >
              <option>ACTIVE</option>
              <option>SUSPENDED</option>
            </select>
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-slate-700">Payout currency</span>
            <select
              value={form.payoutCurrency}
              onChange={(e) => setForm({ ...form, payoutCurrency: e.target.value })}
              className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
            >
              <option>EUR</option>
              <option>USD</option>
              <option>GBP</option>
            </select>
          </label>
        </div>
        <label className="mb-3 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Webhook URL (optional)</span>
          <input
            value={form.webhookUrl ?? ""}
            onChange={(e) => setForm({ ...form, webhookUrl: e.target.value })}
            className="w-full rounded-md border border-slate-300 px-3 py-2 font-mono text-sm"
          />
        </label>
        <label className="mb-4 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">
            Decline-rate alert threshold (bps: {form.declineRateAlertThresholdBps} ={" "}
            {(form.declineRateAlertThresholdBps / 100).toFixed(2)}%)
          </span>
          <input
            type="range"
            min={0}
            max={10000}
            step={100}
            value={form.declineRateAlertThresholdBps}
            onChange={(e) =>
              setForm({ ...form, declineRateAlertThresholdBps: Number(e.target.value) })
            }
            className="w-full"
          />
        </label>
        <div className="flex gap-3">
          <button
            onClick={() => upsert.mutate()}
            disabled={!trimmed || !form.displayName.trim() || upsert.isPending}
            className="rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:opacity-40"
          >
            {upsert.isPending ? "Publishing…" : "PUT config"}
          </button>
          <button
            onClick={() => tombstone.mutate()}
            disabled={!trimmed || tombstone.isPending}
            className="rounded-md border border-rose-300 px-4 py-2 text-sm font-medium text-rose-700 disabled:opacity-40"
          >
            DELETE (tombstone)
          </button>
        </div>
        {(upsert.error || tombstone.error) && (
          <p className="mt-3 text-sm text-rose-600">
            {(upsert.error ?? tombstone.error)?.message}
          </p>
        )}
      </section>

      <section className="rounded-lg border border-slate-200 bg-white p-6">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-base font-semibold">Read (analytics GlobalKTable)</h2>
          {propagatedMs !== null && (
            <span className="rounded bg-emerald-100 px-2 py-0.5 text-xs font-medium text-emerald-700">
              picked up downstream in {(propagatedMs / 1000).toFixed(1)} s
            </span>
          )}
          {writeStartedAt.current > 0 && propagatedMs === null && (
            <span className="rounded bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-700">
              waiting for the GlobalKTable…
            </span>
          )}
        </div>
        {!trimmed && <p className="text-sm text-slate-400">Enter a merchant id.</p>}
        {notFound && (
          <p className="text-sm text-slate-500">
            No config on the topic for <span className="font-mono">{trimmed}</span> - either never
            written, or tombstoned and compacted away.
          </p>
        )}
        {view.data && (
          <dl className="grid grid-cols-[160px_1fr] gap-y-2 text-sm">
            <dt className="text-slate-500">displayName</dt>
            <dd>{view.data.displayName}</dd>
            <dt className="text-slate-500">status</dt>
            <dd>
              <span
                className={
                  view.data.status === "ACTIVE"
                    ? "rounded bg-emerald-100 px-2 py-0.5 text-xs font-medium text-emerald-700"
                    : "rounded bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-700"
                }
              >
                {view.data.status}
              </span>
            </dd>
            <dt className="text-slate-500">payoutCurrency</dt>
            <dd>{view.data.payoutCurrency}</dd>
            <dt className="text-slate-500">webhookUrl</dt>
            <dd className="font-mono text-xs">{view.data.webhookUrl ?? "–"}</dd>
            <dt className="text-slate-500">alert threshold</dt>
            <dd>{(view.data.declineRateAlertThresholdBps / 100).toFixed(2)}%</dd>
          </dl>
        )}
      </section>
    </main>
  );
}
