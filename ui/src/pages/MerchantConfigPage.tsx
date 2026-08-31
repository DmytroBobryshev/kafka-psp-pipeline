import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { listMerchants, type MerchantView } from "../api/merchantsApi";
import { deleteMerchantConfig, upsertMerchantConfig } from "../api/merchantConfigApi";
import type { MerchantStatus, UpsertMerchantConfigRequest } from "../api/types";

const EMPTY_FORM: UpsertMerchantConfigRequest & { merchantId: string } = {
  merchantId: "",
  displayName: "",
  status: "ACTIVE",
  payoutCurrency: "EUR",
  webhookUrl: "",
  declineRateAlertThresholdBps: 2500,
};

export function MerchantConfigPage() {
  const queryClient = useQueryClient();
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [form, setForm] = useState(EMPTY_FORM);
  const [editing, setEditing] = useState(false);

  // Writes land on the compacted topic and come back through payment-api's projection
  // asynchronously (~1s) - the short refetch interval is what makes edits appear "live".
  const merchants = useQuery({
    queryKey: ["merchants-list", statusFilter],
    queryFn: () => listMerchants({ status: statusFilter || undefined }),
    refetchInterval: 3000,
    retry: false,
  });

  const select = (m: MerchantView) => {
    setEditing(true);
    setForm({
      merchantId: m.merchantId,
      displayName: m.displayName,
      status: m.status,
      payoutCurrency: m.payoutCurrency,
      webhookUrl: m.webhookUrl ?? "",
      declineRateAlertThresholdBps: m.declineRateAlertThresholdBps,
    });
  };

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ["merchants-list"] });

  const upsert = useMutation({
    mutationFn: () =>
      upsertMerchantConfig(form.merchantId.trim(), {
        displayName: form.displayName,
        status: form.status,
        payoutCurrency: form.payoutCurrency,
        webhookUrl: form.webhookUrl?.trim() ? form.webhookUrl : null,
        declineRateAlertThresholdBps: form.declineRateAlertThresholdBps,
      }),
    onSuccess: () => {
      invalidate();
      setEditing(true);
    },
  });

  const tombstone = useMutation({
    mutationFn: () => deleteMerchantConfig(form.merchantId.trim()),
    onSuccess: () => {
      invalidate();
      setForm(EMPTY_FORM);
      setEditing(false);
    },
  });

  return (
    <main className="mx-auto grid max-w-[1500px] gap-8 px-6 py-8 xl:grid-cols-[1fr_460px]">
      <section>
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div className="flex gap-1">
            {["", "ACTIVE", "SUSPENDED"].map((s) => (
              <button
                key={s}
                onClick={() => setStatusFilter(s)}
                className={`rounded-md px-3 py-1.5 text-xs font-medium ${
                  statusFilter === s ? "bg-slate-900 text-white" : "text-slate-600 hover:bg-slate-100"
                }`}
              >
                {s || "all"}
              </button>
            ))}
          </div>
          <button
            onClick={() => {
              setForm(EMPTY_FORM);
              setEditing(false);
            }}
            className="rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white"
          >
            + new merchant
          </button>
        </div>

        {merchants.error && (
          <p className="mb-3 text-sm text-rose-600">
            Merchant list unavailable: {merchants.error.message}
          </p>
        )}

        <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="px-4 py-3">Merchant</th>
                <th className="px-4 py-3">ID</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Payout</th>
                <th className="px-4 py-3">Webhook</th>
                <th className="px-4 py-3">Updated</th>
                <th className="px-4 py-3"></th>
              </tr>
            </thead>
            <tbody>
              {(merchants.data?.items ?? []).map((m) => (
                <tr
                  key={m.merchantId}
                  onClick={() => select(m)}
                  className={`cursor-pointer border-t border-slate-100 ${
                    form.merchantId === m.merchantId ? "bg-slate-100" : "hover:bg-slate-50"
                  }`}
                >
                  <td className="px-4 py-2 font-medium">{m.displayName}</td>
                  <td className="px-4 py-2 font-mono text-xs">{m.merchantId}</td>
                  <td className="px-4 py-2">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-medium ${
                        m.status === "ACTIVE"
                          ? "bg-emerald-100 text-emerald-700"
                          : "bg-amber-100 text-amber-700"
                      }`}
                    >
                      {m.status}
                    </span>
                  </td>
                  <td className="px-4 py-2">{m.payoutCurrency}</td>
                  <td className="px-4 py-2 font-mono text-[10px] text-slate-400">
                    {m.webhookUrl ? "✓ set" : "–"}
                  </td>
                  <td className="px-4 py-2 text-xs text-slate-500">
                    {new Date(m.updatedAt).toLocaleString()}
                  </td>
                  <td className="px-4 py-2 text-right">
                    <Link
                      to="/payments"
                      search={{ merchantId: m.merchantId, paymentId: undefined }}
                      onClick={(e) => e.stopPropagation()}
                      className="text-xs text-slate-500 underline-offset-2 hover:underline"
                    >
                      transactions →
                    </Link>
                  </td>
                </tr>
              ))}
              {merchants.data?.items.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-10 text-center text-slate-400">
                    No merchants{statusFilter ? ` with status ${statusFilter}` : ""} yet — create one
                    on the right.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <aside className="rounded-lg border border-slate-200 bg-white p-6">
        <h2 className="mb-4 text-base font-semibold">
          {editing ? `Edit ${form.merchantId}` : "Create merchant"}
        </h2>
        <label className="mb-3 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Merchant ID</span>
          <input
            value={form.merchantId}
            onChange={(e) => setForm({ ...form, merchantId: e.target.value })}
            disabled={editing}
            placeholder="merchant-acme"
            className="w-full rounded-md border border-slate-300 px-3 py-2 font-mono text-sm disabled:bg-slate-50 disabled:text-slate-500"
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
          <span className="mb-1 block text-sm font-medium text-slate-700">
            Webhook URL <span className="text-xs font-normal text-slate-400">(payment + refund notifications)</span>
          </span>
          <input
            value={form.webhookUrl ?? ""}
            onChange={(e) => setForm({ ...form, webhookUrl: e.target.value })}
            className="w-full rounded-md border border-slate-300 px-3 py-2 font-mono text-xs"
          />
        </label>
        <label className="mb-4 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">
            Decline-rate alert: {(form.declineRateAlertThresholdBps / 100).toFixed(2)}%
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
            disabled={!form.merchantId.trim() || !form.displayName.trim() || upsert.isPending}
            className="flex-1 rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:opacity-40"
          >
            {upsert.isPending ? "Saving…" : editing ? "Save changes" : "Create"}
          </button>
          {editing && (
            <button
              onClick={() => tombstone.mutate()}
              disabled={tombstone.isPending}
              className="rounded-md border border-rose-300 px-4 py-2 text-sm font-medium text-rose-700 disabled:opacity-40"
            >
              Delete
            </button>
          )}
        </div>
        {(upsert.error || tombstone.error) && (
          <p className="mt-3 text-sm text-rose-600">{(upsert.error ?? tombstone.error)?.message}</p>
        )}
        <p className="mt-3 text-[10px] text-slate-400">
          Only ACTIVE merchants can take payments. Deleting publishes a tombstone — the merchant
          disappears everywhere once compaction and the projections catch up.
        </p>
      </aside>
    </main>
  );
}
