import { useState } from "react";
import { useSearch } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  getPaymentHistory,
  getRefundHistory,
  getPaymentRefunds,
  getProviderStatus,
  getWebhookDeliveries,
  listPayments,
} from "../api/paymentsApi";
import { getRefundState, requestRefund } from "../api/refundApi";
import { useCopy } from "../lib/clipboard";
import { KebabMenu } from "../components/KebabMenu";
import { FALLBACK_BADGE, STATUS_BADGE } from "../lib/badges";
import type { RefundResponse } from "../api/types";
import type { PaymentResponse } from "../api/types";

const STATUSES = ["", "CREATED", "PENDING", "SUCCEEDED", "FAILED", "EXPIRED"] as const;

/**
 * The single transactions panel (payments + their refunds + deliveries in one place): every payment the platform has ever taken, straight from payment-api's
 * Postgres (now kept in sync by its payment-status-changed listener), filterable by merchant and
 * status. Each row carries a kebab menu; details expand full-width under the row: fields, refund
 * history, on-demand provider status (M12 request-reply over Kafka) and webhook deliveries.
 */
export function PaymentsPage() {
  const search = useSearch({ strict: false }) as { merchantId?: string; paymentId?: string };
  const [merchantId, setMerchantId] = useState(search.merchantId ?? "");
  const [status, setStatus] = useState("");
  const [page, setPage] = useState(0);
  const [expandedId, setExpandedId] = useState<string | null>(search.paymentId ?? null);
  const [showRefundForm, setShowRefundForm] = useState(false);
  const [showProvider, setShowProvider] = useState(false);
  const { copy, copiedKey } = useCopy();
  const size = 25;

  const payments = useQuery({
    queryKey: ["payments", merchantId.trim(), status, page],
    queryFn: () => listPayments({ merchantId: merchantId.trim() || undefined, status: status || undefined, page, size }),
    placeholderData: (prev) => prev,
  });

  const totalPages = payments.data ? Math.max(1, Math.ceil(payments.data.total / size)) : 1;

  const toggle = (id: string) => {
    setShowRefundForm(false);
    setShowProvider(false);
    setExpandedId(expandedId === id ? null : id);
  };
  const openWith = (id: string, opts: { refund?: boolean; provider?: boolean }) => {
    setExpandedId(id);
    setShowRefundForm(!!opts.refund);
    setShowProvider(!!opts.provider);
  };

  return (
    <main className="mx-auto max-w-[1500px] px-6 py-8">
      <div className="mb-5 flex flex-wrap items-end gap-4">
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Merchant</span>
          <input
            value={merchantId}
            onChange={(e) => { setMerchantId(e.target.value); setPage(0); }}
            placeholder="merchant-1 (exact)"
            className="w-56 rounded-md border border-slate-400 px-3 py-2 font-mono text-sm"
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Status</span>
          <select
            value={status}
            onChange={(e) => { setStatus(e.target.value); setPage(0); }}
            className="rounded-md border border-slate-400 px-3 py-2 text-sm"
          >
            {STATUSES.map((s) => (
              <option key={s} value={s}>{s || "any"}</option>
            ))}
          </select>
        </label>
        <span className="pb-2 text-xs text-slate-500">
          {payments.data ? `${payments.data.total} payment(s)` : payments.isPending ? "loading…" : ""}
        </span>
        {payments.error && (
          <span className="pb-2 text-xs text-rose-600">{payments.error.message}</span>
        )}
      </div>

      <div className="rounded-lg border border-slate-300 bg-white">
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
            {(payments.data?.items ?? []).map((p) => (
              <PaymentRowGroup
                key={p.id}
                payment={p}
                expanded={expandedId === p.id}
                onToggle={() => toggle(p.id)}
                onCopy={() => copy(p.id, p.id)}
                copied={copiedKey === p.id}
                onRefund={() => openWith(p.id, { refund: true })}
                onProvider={() => openWith(p.id, { provider: true })}
                showRefundForm={showRefundForm}
                setShowRefundForm={setShowRefundForm}
                showProvider={showProvider}
                setShowProvider={setShowProvider}
              />
            ))}
            {payments.data?.items.length === 0 && (
              <tr><td colSpan={6} className="px-4 py-10 text-center text-slate-500">No payments match.</td></tr>
            )}
          </tbody>
        </table>
        <div className="flex items-center justify-between border-t border-slate-200 px-4 py-2 text-xs text-slate-600">
          <button disabled={page === 0} onClick={() => setPage(page - 1)} className="rounded-md border border-slate-400 bg-white px-2.5 py-1 text-xs font-medium text-slate-800 shadow-sm hover:bg-slate-200 disabled:opacity-40">← prev</button>
          <span>page {page + 1} / {totalPages}</span>
          <button disabled={page + 1 >= totalPages} onClick={() => setPage(page + 1)} className="rounded-md border border-slate-400 bg-white px-2.5 py-1 text-xs font-medium text-slate-800 shadow-sm hover:bg-slate-200 disabled:opacity-40">next →</button>
        </div>
      </div>
    </main>
  );
}

function PaymentRowGroup(props: {
  payment: PaymentResponse;
  expanded: boolean;
  onToggle: () => void;
  onCopy: () => void;
  copied: boolean;
  onRefund: () => void;
  onProvider: () => void;
  showRefundForm: boolean;
  setShowRefundForm: (v: boolean) => void;
  showProvider: boolean;
  setShowProvider: (v: boolean) => void;
}) {
  const { payment: p, expanded } = props;
  return (
    <>
      <tr
        onClick={props.onToggle}
        className={`cursor-pointer border-t border-slate-200 ${expanded ? "bg-slate-200" : "hover:bg-slate-100"}`}
      >
        <td className="px-4 py-2 font-mono text-xs">{p.id.slice(0, 8)}…</td>
        <td className="px-4 py-2">{p.merchantId}</td>
        <td className="px-4 py-2 text-right">{p.amount} {p.currency}</td>
        <td className="px-4 py-2">
          <span className={`rounded px-2 py-0.5 text-xs font-semibold ${STATUS_BADGE[p.status] ?? FALLBACK_BADGE}`}>
            {p.status}
          </span>
        </td>
        <td className="px-4 py-2 text-xs text-slate-600">{new Date(p.createdAt).toLocaleString()}</td>
        <td className="px-2 py-2 text-right">
          <KebabMenu
            items={[
              { label: expanded ? "Hide details" : "View details", onClick: props.onToggle },
              { label: props.copied ? "Copied ✓" : "Copy payment ID", onClick: props.onCopy },
              {
                label: "Request refund…",
                onClick: props.onRefund,
                disabled: p.status !== "SUCCEEDED",
                title: p.status !== "SUCCEEDED" ? "only SUCCEEDED payments can be refunded" : undefined,
              },
              { label: "Check provider status", onClick: props.onProvider },
            ]}
          />
        </td>
      </tr>
      {expanded && (
        <tr className="border-t border-slate-200">
          <td colSpan={6} className="bg-slate-100/60 p-0">
            <PaymentDetail
              payment={p}
              showRefundForm={props.showRefundForm}
              setShowRefundForm={props.setShowRefundForm}
              showProvider={props.showProvider}
              setShowProvider={props.setShowProvider}
            />
          </td>
        </tr>
      )}
    </>
  );
}

function KeyValue({ data }: { data: Record<string, unknown> }) {
  return (
    <dl className="grid grid-cols-[140px_1fr] gap-y-1 text-xs">
      {Object.entries(data).map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="text-slate-600">{k}</dt>
          <dd className="break-all font-mono">{v == null ? "–" : String(v)}</dd>
        </div>
      ))}
    </dl>
  );
}

function LifecycleRow({ label, at, tone }: { label: string; at?: string | null; tone?: string }) {
  return (
    <div className="flex items-center justify-between border-l-2 border-slate-300 py-1 pl-3 text-xs">
      <span className={tone ?? "text-slate-700"}>{label}</span>
      <span className="font-mono text-slate-600">{at ? new Date(at).toLocaleString() : "—"}</span>
    </div>
  );
}

const REFUND_TRAIL_LABEL: Record<string, string> = {
  REQUESTED: "Refund requested",
  FUNDS_RESERVED: "Funds reserved",
  PENDING: "Sent to provider",
  IPN_RECEIVED: "IPN received",
  VERIFIED: "Status verified",
  COMPLETED: "Refund completed",
  FAILED: "Refund failed",
  EXPIRED: "Refund expired",
};

const REFUND_TRAIL_TONE: Record<string, string> = {
  COMPLETED: "text-emerald-700",
  FAILED: "text-rose-700",
  EXPIRED: "text-violet-700",
  PENDING: "text-amber-700",
  IPN_RECEIVED: "text-sky-700",
  VERIFIED: "text-sky-700",
};

/** One refund with an expandable detail: full status trail + the ledger's saga state on demand. */
function RefundRow({ paymentId, refund }: { paymentId: string; refund: RefundResponse }) {
  const [open, setOpen] = useState(false);
  const trail = useQuery({
    queryKey: ["refund-history", refund.id],
    queryFn: () => getRefundHistory(paymentId, refund.id),
    enabled: open,
    retry: false,
  });
  const ledger = useQuery({
    queryKey: ["refund-state", refund.id],
    queryFn: () => getRefundState(refund.id),
    enabled: open,
    retry: false,
  });

  return (
    <li className="rounded border border-slate-200 bg-white text-xs">
      <button onClick={() => setOpen(!open)} className="flex w-full items-center justify-between px-2 py-1.5">
        <span className="font-mono">{refund.id.slice(0, 8)}…</span>
        <span>
          {refund.amount} {refund.currency}
          <span className="ml-2 text-slate-500">{open ? "▲" : "▼"}</span>
        </span>
      </button>
      {open && (
        <div className="border-t border-slate-200 px-2 py-2">
          {trail.isPending && <p className="pl-3 text-slate-500">loading trail…</p>}
          {trail.error && <LifecycleRow label="Refund requested" at={refund.createdAt} />}
          {(trail.data ?? []).map((h, i) => (
            <div key={h.eventId ?? i} className="border-l-2 border-slate-200 py-1 pl-3">
              <div className="flex items-center justify-between">
                <span className={REFUND_TRAIL_TONE[h.status] ?? "text-slate-700"}>
                  {REFUND_TRAIL_LABEL[h.status] ?? h.status}
                </span>
                <span className="font-mono text-slate-500">
                  {new Date(h.occurredAt).toLocaleTimeString()}
                </span>
              </div>
              <div className="truncate text-[10px] text-slate-500">
                {h.source}
                {h.providerReference ? ` · provider ref ${h.providerReference.slice(0, 8)}…` : ""}
                {h.eventId ? ` · ${h.eventId.slice(0, 8)}…` : ""}
              </div>
            </div>
          ))}
          {ledger.error && <p className="pl-3 text-slate-500">ledger has no saga row yet</p>}
          {ledger.data && (
            <div className="mt-2 rounded border border-slate-200 bg-slate-100 p-2">
              <div className="mb-1 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
                Ledger's view — GET /api/refunds/{"{id}"}, full response
              </div>
              <KeyValue data={ledger.data as unknown as Record<string, unknown>} />
            </div>
          )}
        </div>
      )}
    </li>
  );
}

function InlineRefundForm({ paymentId, onDone }: { paymentId: string; onDone: () => void }) {
  const queryClient = useQueryClient();
  const [amount, setAmount] = useState("10.00");
  const [currency, setCurrency] = useState("EUR");
  const [reason, setReason] = useState("");

  const refund = useMutation({
    mutationFn: () =>
      requestRefund(paymentId, { amount: Number(amount), currency, reason: reason.trim() || undefined }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["payment-refunds", paymentId] });
      onDone();
    },
  });

  return (
    <div className="rounded border border-slate-300 bg-slate-100 p-3">
      <div className="mb-2 grid grid-cols-[1fr_90px] gap-2">
        <input
          value={amount}
          onChange={(e) => setAmount(e.target.value)}
          className="rounded-md border border-slate-400 px-2 py-1.5 text-xs"
          placeholder="amount"
        />
        <select
          value={currency}
          onChange={(e) => setCurrency(e.target.value)}
          className="rounded-md border border-slate-400 px-2 py-1.5 text-xs"
        >
          <option>EUR</option>
          <option>USD</option>
          <option>GBP</option>
        </select>
      </div>
      <input
        value={reason}
        onChange={(e) => setReason(e.target.value)}
        className="mb-2 w-full rounded-md border border-slate-400 px-2 py-1.5 text-xs"
        placeholder="reason (optional)"
      />
      <button
        onClick={() => refund.mutate()}
        disabled={refund.isPending}
        className="w-full rounded-md bg-slate-900 px-3 py-1.5 text-xs font-medium text-white disabled:opacity-40"
      >
        {refund.isPending ? "Submitting…" : "POST refund"}
      </button>
      {refund.error && <p className="mt-2 text-xs text-rose-600">{refund.error.message}</p>}
      <p className="mt-2 text-[11px] text-slate-500">
        The saga runs async - watch the refund appear in the list below (initiated → completed,
        usually ~3 s) and its webhook delivery land underneath.
      </p>
    </div>
  );
}

function PaymentDetail({
  payment,
  showRefundForm,
  setShowRefundForm,
  showProvider,
  setShowProvider,
}: {
  payment: PaymentResponse;
  showRefundForm: boolean;
  setShowRefundForm: (v: boolean) => void;
  showProvider: boolean;
  setShowProvider: (v: boolean) => void;
}) {
  const { copy, copiedKey } = useCopy();

  const refunds = useQuery({
    queryKey: ["payment-refunds", payment.id],
    queryFn: () => getPaymentRefunds(payment.id),
  });
  const deliveries = useQuery({
    queryKey: ["payment-deliveries", payment.id],
    queryFn: () => getWebhookDeliveries({ paymentId: payment.id }),
    retry: false,
  });
  const statusTrail = useQuery({
    queryKey: ["payment-history", payment.id],
    queryFn: () => getPaymentHistory(payment.id),
    retry: false,
  });
  const provider = useQuery({
    queryKey: ["provider-status", payment.id],
    queryFn: () => getProviderStatus(payment.id),
    enabled: showProvider,
    retry: false,
  });

  const outcomeLabel =
    payment.status === "SUCCEEDED" ? "Paid" : payment.status === "FAILED" ? "Declined" : null;

  return (
    <div className="grid gap-6 p-5 lg:grid-cols-3">
      <div className="space-y-4">
        <div>
          <div className="mb-1 flex items-center justify-between">
            <h3 className="text-sm font-semibold">Payment</h3>
            <button
              onClick={() => copy(payment.id, "pid")}
              title="Copy payment id"
              className="font-mono text-xs text-slate-600 hover:text-slate-900"
            >
              {copiedKey === "pid" ? "copied ✓" : "copy id ⧉"}
            </button>
          </div>
          <KeyValue data={payment as unknown as Record<string, unknown>} />
          <div className="mt-3 flex gap-3 text-xs">
            <button
              onClick={() => setShowRefundForm(!showRefundForm)}
              disabled={payment.status !== "SUCCEEDED"}
              title={payment.status !== "SUCCEEDED" ? "only SUCCEEDED payments can be refunded" : undefined}
              className="rounded-md border border-slate-400 bg-white px-2.5 py-1 text-xs font-medium text-slate-800 shadow-sm hover:bg-slate-200 disabled:opacity-40"
            >
              {showRefundForm ? "hide refund form" : "request refund"}
            </button>
            <button onClick={() => setShowProvider(!showProvider)} className="rounded-md border border-slate-400 bg-white px-2.5 py-1 text-xs font-medium text-slate-800 shadow-sm hover:bg-slate-200">
              {showProvider ? "hide" : "check"} provider status
            </button>
          </div>
        </div>
        {showRefundForm && <InlineRefundForm paymentId={payment.id} onDone={() => setShowRefundForm(false)} />}
      </div>

      <div>
        <h4 className="mb-1 text-xs font-semibold text-slate-700">History</h4>
        {(() => {
          type Entry = { at: string | null; label: string; tone?: string; sub?: string };
          const TONE: Record<string, string> = {
            SUCCEEDED: "text-emerald-700",
            FAILED: "text-rose-700",
            PENDING: "text-amber-700",
            IPN_RECEIVED: "text-sky-700",
            VERIFIED: "text-sky-700",
            CREATED: "text-slate-700",
            EXPIRED: "text-violet-700",
          };
          const LABEL: Record<string, string> = {
            CREATED: "Created",
            PENDING: "Pending — sent to provider",
            IPN_RECEIVED: "IPN received",
            VERIFIED: "Status verified",
            SUCCEEDED: "Paid",
            FAILED: "Declined",
            EXPIRED: "Expired",
          };
          let entries: Entry[];
          if (statusTrail.data?.length) {
            entries = statusTrail.data.map((h) => ({
              at: h.occurredAt,
              label: LABEL[h.status] ?? h.status,
              tone: TONE[h.status],
              sub: `${h.source}${h.providerReference ? ` · provider ref ${h.providerReference.slice(0, 8)}…` : ""}${h.eventId ? ` · ${h.eventId.slice(0, 8)}…` : ""}`,
            }));
          } else {
            entries = [{ at: payment.createdAt, label: "Created" }];
            if (outcomeLabel) {
              entries.push({
                at: payment.statusUpdatedAt ?? null,
                label: outcomeLabel,
                tone: payment.status === "SUCCEEDED" ? "text-emerald-700" : "text-rose-700",
              });
            } else {
              entries.push({ at: null, label: "Awaiting outcome…", tone: "text-slate-500" });
            }
          }
          for (const r of refunds.data ?? []) {
            entries.push({ at: r.createdAt, label: `Refund initiated · ${r.amount} ${r.currency}` });
          }
          for (const d of deliveries.data ?? []) {
            entries.push({
              at: d.createdAt,
              label: `Webhook ${d.eventType}`,
              tone:
                d.status === "SUCCESS"
                  ? "text-emerald-700"
                  : d.status === "RETRYABLE_FAILURE"
                    ? "text-amber-700"
                    : "text-rose-700",
              sub: `${d.status.toLowerCase()} · ${d.attempts} attempt${d.attempts === 1 ? "" : "s"} → ${d.url}`,
            });
          }
          entries.sort((a, b) => (a.at ?? "9999").localeCompare(b.at ?? "9999"));
          return entries.map((e, i) => (
            <div key={i} className="border-l-2 border-slate-300 py-1 pl-3 text-xs">
              <div className="flex items-center justify-between">
                <span className={e.tone ?? "text-slate-700"}>{e.label}</span>
                <span className="font-mono text-slate-600">
                  {e.at ? new Date(e.at).toLocaleTimeString() : "—"}
                </span>
              </div>
              {e.sub && <div className="truncate text-[11px] text-slate-500">{e.sub}</div>}
            </div>
          ));
        })()}
      </div>

      <div className="space-y-4">
        {showProvider && (
          <div className="rounded border border-slate-200 bg-white p-3">
            <h4 className="mb-1 text-xs font-semibold text-slate-700">Provider status (request-reply over Kafka)</h4>
            {provider.isPending && <p className="text-xs text-slate-500">asking psp-connector…</p>}
            {provider.error && <p className="text-xs text-rose-600">{provider.error.message}</p>}
            {provider.data && <KeyValue data={provider.data} />}
          </div>
        )}
        <div>
          <h4 className="mb-1 text-xs font-semibold text-slate-700">Refunds (click to expand)</h4>
          {refunds.data?.length === 0 && <p className="text-xs text-slate-500">none</p>}
          <ul className="space-y-1">
            {(refunds.data ?? []).map((r) => (
              <RefundRow key={r.id} paymentId={payment.id} refund={r} />
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}
