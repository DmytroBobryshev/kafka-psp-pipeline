import { useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { listMerchants, type MerchantView } from "../api/merchantsApi";
import { deleteMerchantConfig, upsertMerchantConfig } from "../api/merchantConfigApi";
import { KebabMenu } from "../components/KebabMenu";
import { useCopy } from "../lib/clipboard";
import type { MerchantStatus, UpsertMerchantConfigRequest } from "../api/types";

const CURRENCIES = ["EUR", "USD", "GBP"] as const;
const DEFAULT_EXPIRATION_SECONDS = 900;

type MerchantForm = UpsertMerchantConfigRequest & {
  merchantId: string;
  paymentExpirationSeconds: number;
};

const EMPTY_FORM: MerchantForm = {
  merchantId: "",
  displayName: "",
  status: "ACTIVE",
  payoutCurrency: "EUR",
  allowedCurrencies: ["EUR"],
  webhookUrl: "",
  declineRateAlertThresholdBps: 2500,
  paymentExpirationSeconds: DEFAULT_EXPIRATION_SECONDS,
};

const toForm = (m: MerchantView): MerchantForm => ({
  merchantId: m.merchantId,
  displayName: m.displayName,
  status: m.status,
  payoutCurrency: m.payoutCurrency,
  allowedCurrencies: m.allowedCurrencies?.length ? m.allowedCurrencies : [m.payoutCurrency],
  webhookUrl: m.webhookUrl ?? "",
  declineRateAlertThresholdBps: m.declineRateAlertThresholdBps,
  paymentExpirationSeconds:
    (m as { paymentExpirationSeconds?: number }).paymentExpirationSeconds ?? DEFAULT_EXPIRATION_SECONDS,
});

const toRequest = (form: MerchantForm): UpsertMerchantConfigRequest => ({
  displayName: form.displayName,
  status: form.status,
  payoutCurrency: form.payoutCurrency,
  allowedCurrencies: form.allowedCurrencies,
  webhookUrl: form.webhookUrl?.trim() ? form.webhookUrl : null,
  declineRateAlertThresholdBps: form.declineRateAlertThresholdBps,
  ...( { paymentExpirationSeconds: form.paymentExpirationSeconds } as Partial<UpsertMerchantConfigRequest>),
});

export function MerchantConfigPage() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const { copy, copiedKey } = useCopy();
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [form, setForm] = useState(EMPTY_FORM);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  const merchants = useQuery({
    queryKey: ["merchants-list", statusFilter],
    queryFn: () => listMerchants({ status: statusFilter || undefined }),
    retry: false,
  });

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ["merchants-list"] });

  const openEdit = (m: MerchantView) => {
    setCreating(false);
    if (expandedId === m.merchantId) {
      setExpandedId(null);
      return;
    }
    setForm(toForm(m));
    setExpandedId(m.merchantId);
  };

  const upsert = useMutation({
    mutationFn: (f: MerchantForm) => upsertMerchantConfig(f.merchantId.trim(), toRequest(f)),
    onSuccess: () => {
      invalidate();
      setCreating(false);
    },
  });

  const quickToggle = useMutation({
    mutationFn: (m: MerchantView) =>
      upsertMerchantConfig(m.merchantId, {
        ...toRequest(toForm(m)),
        status: (m.status === "ACTIVE" ? "SUSPENDED" : "ACTIVE") as MerchantStatus,
      }),
    onSuccess: invalidate,
  });

  const tombstone = useMutation({
    mutationFn: (merchantId: string) => deleteMerchantConfig(merchantId),
    onSuccess: () => {
      invalidate();
      setExpandedId(null);
    },
  });

  return (
    <main className="mx-auto max-w-[1500px] px-6 py-8">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div className="flex gap-1">
          {["", "ACTIVE", "SUSPENDED"].map((s) => (
            <button
              key={s}
              onClick={() => setStatusFilter(s)}
              className={`rounded-md px-3 py-1.5 text-xs font-medium ${
                statusFilter === s ? "bg-slate-900 text-white" : "text-slate-700 hover:bg-slate-200"
              }`}
            >
              {s || "all"}
            </button>
          ))}
        </div>
        <button
          onClick={() => {
            setForm(EMPTY_FORM);
            setExpandedId(null);
            setCreating(!creating);
          }}
          className="w-40 rounded-md bg-slate-900 px-4 py-2 text-center text-sm font-medium text-white hover:bg-slate-700"
        >
          {creating ? "close form" : "+ new merchant"}
        </button>
      </div>

      {merchants.error && (
        <p className="mb-3 text-sm text-rose-600">
          Merchant list unavailable: {merchants.error.message}
        </p>
      )}

      {creating && (
        <div className="mb-6 rounded-lg border border-slate-300 bg-white">
          <h2 className="border-b border-slate-200 px-5 py-3 text-sm font-semibold">Create merchant</h2>
          <MerchantFormFields
            form={form}
            setForm={setForm}
            editing={false}
            saving={upsert.isPending}
            onSave={() => upsert.mutate(form)}
            error={upsert.error?.message}
          />
        </div>
      )}

      <div className="rounded-lg border border-slate-300 bg-white">
        <table className="w-full text-sm">
          <thead className="bg-slate-100 text-left text-xs uppercase tracking-wide text-slate-600">
            <tr>
              <th className="px-4 py-3">Merchant</th>
              <th className="px-4 py-3">ID</th>
              <th className="px-4 py-3">Status</th>
              <th className="px-4 py-3">Currencies</th>
              <th className="px-4 py-3">Expiration</th>
              <th className="px-4 py-3">Webhook</th>
              <th className="px-4 py-3">Updated</th>
              <th className="w-10 px-2 py-3" />
            </tr>
          </thead>
          <tbody>
            {(merchants.data?.items ?? []).map((m) => (
              <MerchantRowGroup
                key={m.merchantId}
                merchant={m}
                expanded={expandedId === m.merchantId}
                onToggle={() => openEdit(m)}
                kebab={[
                  { label: expandedId === m.merchantId ? "Hide editor" : "Edit…", onClick: () => openEdit(m) },
                  {
                    label: copiedKey === m.merchantId ? "Copied ✓" : "Copy merchant ID",
                    onClick: () => copy(m.merchantId, m.merchantId),
                  },
                  {
                    label: "Transactions →",
                    onClick: () =>
                      navigate({ to: "/payments", search: { merchantId: m.merchantId, paymentId: undefined } }),
                  },
                  {
                    label: m.status === "ACTIVE" ? "Suspend" : "Activate",
                    onClick: () => quickToggle.mutate(m),
                    title:
                      m.status === "ACTIVE"
                        ? "SUSPENDED merchants cannot take payments"
                        : "only ACTIVE merchants can take payments",
                  },
                  { label: "Delete (tombstone)", onClick: () => tombstone.mutate(m.merchantId), tone: "danger" },
                ]}
                editor={
                  expandedId === m.merchantId ? (
                    <MerchantFormFields
                      form={form}
                      setForm={setForm}
                      editing
                      saving={upsert.isPending}
                      onSave={() => upsert.mutate(form)}
                      onDelete={() => tombstone.mutate(m.merchantId)}
                      deleting={tombstone.isPending}
                      error={upsert.error?.message ?? tombstone.error?.message}
                    />
                  ) : null
                }
              />
            ))}
            {merchants.data?.items.length === 0 && (
              <tr>
                <td colSpan={8} className="px-4 py-10 text-center text-slate-500">
                  No merchants{statusFilter ? ` with status ${statusFilter}` : ""} yet — create one with
                  “+ new merchant”.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <p className="mt-3 text-[11px] text-slate-500">
        Only ACTIVE merchants can take payments. Deleting publishes a tombstone — the merchant
        disappears everywhere once compaction and the projections catch up.
      </p>
    </main>
  );
}

function MerchantRowGroup({
  merchant: m,
  expanded,
  onToggle,
  kebab,
  editor,
}: {
  merchant: MerchantView;
  expanded: boolean;
  onToggle: () => void;
  kebab: Parameters<typeof KebabMenu>[0]["items"];
  editor: React.ReactNode;
}) {
  const expiration =
    (m as { paymentExpirationSeconds?: number }).paymentExpirationSeconds ?? DEFAULT_EXPIRATION_SECONDS;
  return (
    <>
      <tr
        onClick={onToggle}
        className={`cursor-pointer border-t border-slate-200 ${expanded ? "bg-slate-200" : "hover:bg-slate-100"}`}
      >
        <td className="px-4 py-2 font-medium">{m.displayName}</td>
        <td className="px-4 py-2 font-mono text-xs">{m.merchantId}</td>
        <td className="px-4 py-2">
          <span
            className={`rounded px-2 py-0.5 text-xs font-semibold ${
              m.status === "ACTIVE"
                ? "bg-emerald-100 text-emerald-800 ring-1 ring-inset ring-emerald-600/40"
                : "bg-amber-100 text-amber-800 ring-1 ring-inset ring-amber-600/40"
            }`}
          >
            {m.status}
          </span>
        </td>
        <td className="px-4 py-2 text-xs">
          {(m.allowedCurrencies?.length ? m.allowedCurrencies : [m.payoutCurrency]).join(" · ")}
        </td>
        <td className="px-4 py-2 text-xs text-slate-600">{formatExpiration(expiration)}</td>
        <td className="px-4 py-2 font-mono text-[11px] text-slate-500">{m.webhookUrl ? "✓ set" : "–"}</td>
        <td className="px-4 py-2 text-xs text-slate-600">{new Date(m.updatedAt).toLocaleString()}</td>
        <td className="px-2 py-2 text-right">
          <KebabMenu items={kebab} />
        </td>
      </tr>
      {expanded && (
        <tr className="border-t border-slate-200">
          <td colSpan={8} className="bg-slate-100/60 p-0">
            {editor}
          </td>
        </tr>
      )}
    </>
  );
}

function formatExpiration(seconds: number): string {
  if (seconds % 60 === 0) return `${seconds / 60} min`;
  return `${seconds} s`;
}

function MerchantFormFields({
  form,
  setForm,
  editing,
  saving,
  onSave,
  onDelete,
  deleting,
  error,
}: {
  form: MerchantForm;
  setForm: (f: MerchantForm) => void;
  editing: boolean;
  saving: boolean;
  onSave: () => void;
  onDelete?: () => void;
  deleting?: boolean;
  error?: string;
}) {
  return (
    <div className="p-5">
      <div className="grid gap-6 lg:grid-cols-3">
        <div className="space-y-3">
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-slate-700">Merchant ID</span>
            <input
              value={form.merchantId}
              onChange={(e) => setForm({ ...form, merchantId: e.target.value })}
              disabled={editing}
              placeholder="merchant-acme"
              className="w-full rounded-md border border-slate-400 px-3 py-2 font-mono text-sm disabled:bg-slate-100 disabled:text-slate-600"
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-slate-700">Display name</span>
            <input
              value={form.displayName}
              onChange={(e) => setForm({ ...form, displayName: e.target.value })}
              className="w-full rounded-md border border-slate-400 px-3 py-2 text-sm"
            />
          </label>
          <div className="grid grid-cols-2 gap-3">
            <label className="block">
              <span className="mb-1 block text-sm font-medium text-slate-700">Status</span>
              <select
                value={form.status}
                onChange={(e) => setForm({ ...form, status: e.target.value as MerchantStatus })}
                className="w-full rounded-md border border-slate-400 px-3 py-2 text-sm"
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
                className="w-full rounded-md border border-slate-400 px-3 py-2 text-sm"
              >
                {form.allowedCurrencies.map((c) => (
                  <option key={c}>{c}</option>
                ))}
              </select>
            </label>
          </div>
        </div>

        <div className="space-y-3">
          <div>
            <span className="mb-1 block text-sm font-medium text-slate-700">
              Accepted currencies{" "}
              <span className="text-xs font-normal text-slate-500">(1–3; payments allowed only in these)</span>
            </span>
            <div className="flex gap-3">
              {CURRENCIES.map((c) => {
                const checked = form.allowedCurrencies.includes(c);
                return (
                  <label
                    key={c}
                    className={`flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-sm ${
                      checked ? "border-slate-900 bg-slate-900 text-white" : "border-slate-400 text-slate-700"
                    }`}
                  >
                    <input
                      type="checkbox"
                      className="hidden"
                      checked={checked}
                      onChange={() => {
                        const next = checked
                          ? form.allowedCurrencies.filter((x) => x !== c)
                          : [...form.allowedCurrencies, c];
                        if (next.length === 0) return;
                        setForm({
                          ...form,
                          allowedCurrencies: next,
                          payoutCurrency: next.includes(form.payoutCurrency) ? form.payoutCurrency : next[0],
                        });
                      }}
                    />
                    {c}
                  </label>
                );
              })}
            </div>
          </div>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-slate-700">
              Webhook URL <span className="text-xs font-normal text-slate-500">(payment + refund notifications)</span>
            </span>
            <input
              value={form.webhookUrl ?? ""}
              onChange={(e) => setForm({ ...form, webhookUrl: e.target.value })}
              className="w-full rounded-md border border-slate-400 px-3 py-2 font-mono text-xs"
            />
          </label>
        </div>

        <div className="space-y-3">
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-slate-700">
              Payment expiration (seconds){" "}
              <span className="text-xs font-normal text-slate-500">
                (payments still CREATED/PENDING after this become EXPIRED)
              </span>
            </span>
            <input
              type="number"
              min={30}
              step={30}
              value={form.paymentExpirationSeconds}
              onChange={(e) =>
                setForm({ ...form, paymentExpirationSeconds: Math.max(30, Number(e.target.value) || 0) })
              }
              className="w-full rounded-md border border-slate-400 px-3 py-2 text-sm"
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-slate-700">
              Decline-rate alert: {(form.declineRateAlertThresholdBps / 100).toFixed(2)}%
            </span>
            <input
              type="range"
              min={0}
              max={10000}
              step={100}
              value={form.declineRateAlertThresholdBps}
              onChange={(e) => setForm({ ...form, declineRateAlertThresholdBps: Number(e.target.value) })}
              className="w-full"
            />
          </label>
          <div className="flex gap-3">
            <button
              onClick={onSave}
              disabled={!form.merchantId.trim() || !form.displayName.trim() || saving}
              className="flex-1 rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-700 disabled:opacity-40"
            >
              {saving ? "Saving…" : editing ? "Save changes" : "Create"}
            </button>
            {onDelete && (
              <button
                onClick={onDelete}
                disabled={deleting}
                className="rounded-md border border-rose-300 bg-white px-4 py-2 text-sm font-medium text-rose-700 hover:bg-rose-50 disabled:opacity-40"
              >
                Delete
              </button>
            )}
          </div>
          {error && <p className="text-sm text-rose-600">{error}</p>}
        </div>
      </div>
    </div>
  );
}
