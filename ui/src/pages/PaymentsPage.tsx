import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  getPaymentRefunds,
  getProviderStatus,
  getWebhookDeliveries,
  listPayments,
} from "../api/paymentsApi";
import { getRefundState, requestRefund } from "../api/refundApi";
import { useCopy } from "../lib/clipboard";
import type { RefundResponse } from "../api/types";
import type { PaymentResponse } from "../api/types";

const STATUSES = ["", "CREATED", "PENDING", "SUCCEEDED", "FAILED"] as const;

const STATUS_BADGE: Record<string, string> = {
  SUCCEEDED: "bg-emerald-100 text-emerald-700",
  FAILED: "bg-rose-100 text-rose-700",
  PENDING: "bg-amber-100 text-amber-700",
  CREATED: "bg-slate-100 text-slate-600",
};

/**
 * The single transactions panel (payments + their refunds + deliveries in one place): every payment the platform has ever taken, straight from payment-api's
 * Postgres (now kept in sync by its payment-status-changed listener), filterable by merchant and
 * status. Selecting a row opens the full detail: fields, refund history, on-demand provider
 * status (M12 request-reply over Kafka) and webhook delivery attempts (M8's Mongo log).
 */
export function PaymentsPage() {
  const [merchantId, setMerchantId] = useState("");
  const [status, setStatus] = useState("");
  const [page, setPage] = useState(0);
  const [selected, setSelected] = useState<PaymentResponse | null>(null);
  const size = 25;

  const payments = useQuery({
    queryKey: ["payments", merchantId.trim(), status, page],
    queryFn: () => listPayments({ merchantId: merchantId.trim() || undefined, status: status || undefined, page, size }),
    refetchInterval: 10000,
    placeholderData: (prev) => prev,
  });

  const totalPages = payments.data ? Math.max(1, Math.ceil(payments.data.total / size)) : 1;

  return (
    <main className="mx-auto max-w-6xl px-6 py-8">
      <div className="mb-5 flex flex-wrap items-end gap-4">
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Merchant</span>
          <input
            value={merchantId}
            onChange={(e) => { setMerchantId(e.target.value); setPage(0); }}
            placeholder="merchant-1 (exact)"
            className="w-56 rounded-md border border-slate-300 px-3 py-2 font-mono text-sm"
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Status</span>
          <select
            value={status}
            onChange={(e) => { setStatus(e.target.value); setPage(0); }}
            className="rounded-md border border-slate-300 px-3 py-2 text-sm"
          >
            {STATUSES.map((s) => (
              <option key={s} value={s}>{s || "any"}</option>
            ))}
          </select>
        </label>
        <span className="pb-2 text-xs text-slate-400">
          {payments.data ? `${payments.data.total} payment(s)` : payments.isPending ? "loading…" : ""}
        </span>
        {payments.error && (
          <span className="pb-2 text-xs text-rose-600">{payments.error.message}</span>
        )}
      </div>

      <div className="grid gap-6 lg:grid-cols-[1fr_380px]">
        <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="px-4 py-3">Payment</th>
                <th className="px-4 py-3">Merchant</th>
                <th className="px-4 py-3 text-right">Amount</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Created</th>
              </tr>
            </thead>
            <tbody>
              {(payments.data?.items ?? []).map((p) => (
                <tr
                  key={p.id}
                  onClick={() => setSelected(p)}
                  className={`cursor-pointer border-t border-slate-100 ${selected?.id === p.id ? "bg-slate-100" : "hover:bg-slate-50"}`}
                >
                  <td className="px-4 py-2 font-mono text-xs">{p.id.slice(0, 8)}…</td>
                  <td className="px-4 py-2">{p.merchantId}</td>
                  <td className="px-4 py-2 text-right">{p.amount} {p.currency}</td>
                  <td className="px-4 py-2">
                    <span className={`rounded px-2 py-0.5 text-xs font-medium ${STATUS_BADGE[p.status] ?? "bg-slate-100 text-slate-600"}`}>
                      {p.status}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-xs text-slate-500">{new Date(p.createdAt).toLocaleString()}</td>
                </tr>
              ))}
              {payments.data?.items.length === 0 && (
                <tr><td colSpan={5} className="px-4 py-10 text-center text-slate-400">No payments match.</td></tr>
              )}
            </tbody>
          </table>
          <div className="flex items-center justify-between border-t border-slate-100 px-4 py-2 text-xs text-slate-500">
            <button disabled={page === 0} onClick={() => setPage(page - 1)} className="disabled:opacity-30">← prev</button>
            <span>page {page + 1} / {totalPages}</span>
            <button disabled={page + 1 >= totalPages} onClick={() => setPage(page + 1)} className="disabled:opacity-30">next →</button>
          </div>
        </div>

        <PaymentDetail payment={selected} />
      </div>
    </main>
  );
}

function KeyValue({ data }: { data: Record<string, unknown> }) {
  return (
    <dl className="grid grid-cols-[140px_1fr] gap-y-1 text-xs">
      {Object.entries(data).map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="text-slate-500">{k}</dt>
          <dd className="break-all font-mono">{v == null ? "–" : String(v)}</dd>
        </div>
      ))}
    </dl>
  );
}

function LifecycleRow({ label, at, tone }: { label: string; at?: string | null; tone?: string }) {
  return (
    <div className="flex items-center justify-between border-l-2 border-slate-200 py-1 pl-3 text-xs">
      <span className={tone ?? "text-slate-700"}>{label}</span>
      <span className="font-mono text-slate-500">{at ? new Date(at).toLocaleString() : "—"}</span>
    </div>
  );
}

/** One refund with an expandable detail (its own fields + the ledger's saga state on demand). */
function RefundRow({ refund }: { refund: RefundResponse }) {
  const [open, setOpen] = useState(false);
  const ledger = useQuery({
    queryKey: ["refund-state", refund.id],
    queryFn: () => getRefundState(refund.id),
    enabled: open,
    retry: false,
  });

  return (
    <li className="rounded border border-slate-100 text-xs">
      <button onClick={() => setOpen(!open)} className="flex w-full items-center justify-between px-2 py-1.5">
        <span className="font-mono">{refund.id.slice(0, 8)}…</span>
        <span>
          {refund.amount} {refund.currency}
          <span className="ml-2 text-slate-400">{open ? "▲" : "▼"}</span>
        </span>
      </button>
      {open && (
        <div className="border-t border-slate-100 px-2 py-2">
          <LifecycleRow label="Refund initiated" at={refund.createdAt} />
          {ledger.data && (
            <LifecycleRow
              label={`Refund ${ledger.data.status.toLowerCase()} (ledger)`}
              at={ledger.data.updatedAt}
              tone={ledger.data.status === "COMPLETED" ? "text-emerald-700" : "text-rose-700"}
            />
          )}
          {ledger.error && <p className="pl-3 text-slate-400">ledger has no saga row yet</p>}
          {ledger.data && (
            <div className="mt-2 rounded border border-slate-100 bg-slate-50 p-2">
              <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-slate-400">
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

/** The refund form, IN the panel - one place for everything, per the user's redesign ask. */
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
    <div className="rounded border border-slate-200 bg-slate-50 p-3">
      <div className="mb-2 grid grid-cols-[1fr_90px] gap-2">
        <input
          value={amount}
          onChange={(e) => setAmount(e.target.value)}
          className="rounded-md border border-slate-300 px-2 py-1.5 text-xs"
          placeholder="amount"
        />
        <select
          value={currency}
          onChange={(e) => setCurrency(e.target.value)}
          className="rounded-md border border-slate-300 px-2 py-1.5 text-xs"
        >
          <option>EUR</option>
          <option>USD</option>
          <option>GBP</option>
        </select>
      </div>
      <input
        value={reason}
        onChange={(e) => setReason(e.target.value)}
        className="mb-2 w-full rounded-md border border-slate-300 px-2 py-1.5 text-xs"
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
      <p className="mt-2 text-[10px] text-slate-400">
        The saga runs async - watch the refund appear in the list below (initiated → completed,
        usually ~3 s) and its webhook delivery land underneath.
      </p>
    </div>
  );
}

function PaymentDetail({ payment }: { payment: PaymentResponse | null }) {
  const [showProvider, setShowProvider] = useState(false);
  const [showRefundForm, setShowRefundForm] = useState(false);
  const { copy, copiedKey } = useCopy();

  const refunds = useQuery({
    queryKey: ["payment-refunds", payment?.id],
    queryFn: () => getPaymentRefunds(payment!.id),
    enabled: !!payment,
    refetchInterval: 5000,
  });
  const deliveries = useQuery({
    queryKey: ["payment-deliveries", payment?.id],
    queryFn: () => getWebhookDeliveries({ paymentId: payment!.id }),
    enabled: !!payment,
    retry: false,
    refetchInterval: 7000,
  });
  const provider = useQuery({
    queryKey: ["provider-status", payment?.id],
    queryFn: () => getProviderStatus(payment!.id),
    enabled: !!payment && showProvider,
    retry: false,
  });

  if (!payment) {
    return (
      <aside className="rounded-lg border border-dashed border-slate-200 bg-slate-50 p-6 text-sm text-slate-400">
        Select a payment to see its full detail: lifecycle, fields, refund history, provider
        status and webhook deliveries.
      </aside>
    );
  }

  const outcomeLabel =
    payment.status === "SUCCEEDED" ? "Paid" : payment.status === "FAILED" ? "Declined" : null;

  return (
    <aside className="space-y-4 rounded-lg border border-slate-200 bg-white p-5">
      <div>
        <div className="mb-1 flex items-center justify-between">
          <h3 className="text-sm font-semibold">Payment</h3>
          <button
            onClick={() => copy(payment.id, "pid")}
            title="Copy payment id"
            className="font-mono text-xs text-slate-500 hover:text-slate-900"
          >
            {copiedKey === "pid" ? "copied ✓" : "copy id ⧉"}
          </button>
        </div>
        <KeyValue data={payment as unknown as Record<string, unknown>} />
        <div className="mt-3 flex gap-3 text-xs">
          <button
            onClick={() => setShowRefundForm((s) => !s)}
            disabled={payment.status !== "SUCCEEDED"}
            title={payment.status !== "SUCCEEDED" ? "only SUCCEEDED payments can be refunded" : undefined}
            className="font-medium text-slate-700 underline-offset-2 hover:underline disabled:opacity-40"
          >
            {showRefundForm ? "hide refund form" : "request refund"}
          </button>
          <button onClick={() => setShowProvider((s) => !s)} className="text-slate-500 underline-offset-2 hover:underline">
            {showProvider ? "hide" : "check"} provider status
          </button>
        </div>
      </div>

      {showRefundForm && <InlineRefundForm paymentId={payment.id} onDone={() => setShowRefundForm(false)} />}

      <div>
        <h4 className="mb-1 text-xs font-semibold text-slate-600">Lifecycle</h4>
        <LifecycleRow label="Created" at={payment.createdAt} />
        {outcomeLabel ? (
          <LifecycleRow
            label={outcomeLabel}
            at={payment.statusUpdatedAt}
            tone={payment.status === "SUCCEEDED" ? "text-emerald-700" : "text-rose-700"}
          />
        ) : (
          <LifecycleRow label="Awaiting outcome…" at={null} tone="text-slate-400" />
        )}
        {(refunds.data ?? []).map((r) => (
          <LifecycleRow key={r.id} label={`Refund initiated (${r.amount} ${r.currency})`} at={r.createdAt} />
        ))}
      </div>

      {showProvider && (
        <div className="rounded border border-slate-100 bg-slate-50 p-3">
          <h4 className="mb-1 text-xs font-semibold text-slate-600">Provider status (request-reply over Kafka)</h4>
          {provider.isPending && <p className="text-xs text-slate-400">asking psp-connector…</p>}
          {provider.error && <p className="text-xs text-rose-600">{provider.error.message}</p>}
          {provider.data && <KeyValue data={provider.data} />}
        </div>
      )}

      <div>
        <h4 className="mb-1 text-xs font-semibold text-slate-600">Refunds (click to expand)</h4>
        {refunds.data?.length === 0 && <p className="text-xs text-slate-400">none</p>}
        <ul className="space-y-1">
          {(refunds.data ?? []).map((r) => (
            <RefundRow key={r.id} refund={r} />
          ))}
        </ul>
      </div>

      <div>
        <h4 className="mb-1 text-xs font-semibold text-slate-600">Webhook deliveries</h4>
        {deliveries.error && (
          <p className="text-xs text-slate-400">
            no delivery log available{deliveries.error.message ? ` (${deliveries.error.message})` : ""}
          </p>
        )}
        {deliveries.data?.length === 0 && (
          <p className="text-xs text-slate-400">no deliveries planned for this payment yet</p>
        )}
        <ul className="space-y-1">
          {(deliveries.data ?? []).map((d) => (
            <li key={d.id} className="rounded border border-slate-100 p-2 text-xs">
              <div className="flex items-center justify-between gap-2">
                <span className="font-medium">{d.eventType}</span>
                <span
                  className={`rounded px-1.5 py-0.5 font-medium ${
                    d.status === "SUCCESS"
                      ? "bg-emerald-100 text-emerald-700"
                      : d.status === "RETRYABLE_FAILURE"
                        ? "bg-amber-100 text-amber-700"
                        : "bg-rose-100 text-rose-700"
                  }`}
                >
                  {d.status}
                </span>
              </div>
              <div className="mt-0.5 text-slate-500">
                {d.attempts} attempt{d.attempts === 1 ? "" : "s"} · last{" "}
                {new Date(d.lastAttemptAt).toLocaleTimeString()}
                {d.refundId && <span className="ml-2 font-mono">refund {d.refundId.slice(0, 8)}…</span>}
              </div>
              <div className="mt-0.5 truncate font-mono text-[10px] text-slate-400">{d.url}</div>
            </li>
          ))}
        </ul>
      </div>
    </aside>
  );
}
