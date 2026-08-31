import { useMemo, useState } from "react";
import { useSearch } from "@tanstack/react-router";
import { useMutation, useQuery } from "@tanstack/react-query";
import { usePaymentHistory } from "../hooks/usePaymentHistory";
import { getRefundState, requestRefund } from "../api/refundApi";
import { ApiError } from "../api/client";
import { useEventStream } from "../hooks/useEventStream";
import { ConnectionStatus } from "../components/ConnectionStatus";
import type { RealtimeEvent } from "../api/types";

/**
 * Page 6: refund tracker. The saga step timeline is reconstructed purely from the SSE stream
 * (all five refund event types carry paymentId, so the page-1 stream already contains the whole
 * choreography) - no dedicated saga-history API exists, and per ADR-0004 none should: push to
 * the browser beats querying services. Ledger's GET /api/refunds/{id} supplements it with the
 * saga's current state row as the ledger sees it.
 */
const SAGA_STEPS = [
  { type: "refunds.refund-requested.v1", label: "Refund requested", kind: "step" },
  { type: "refunds.funds-reserved.v1", label: "Funds reserved (ledger)", kind: "step" },
  { type: "refunds.refund-completed.v1", label: "Refund completed (provider)", kind: "success" },
  { type: "refunds.refund-failed.v1", label: "Refund failed (provider)", kind: "failure" },
  {
    type: "refunds.reservation-released.v1",
    label: "Reservation released - compensating transaction",
    kind: "compensation",
  },
] as const;

const KIND_STYLES: Record<string, string> = {
  step: "border-slate-300 bg-white",
  success: "border-emerald-300 bg-emerald-50",
  failure: "border-rose-300 bg-rose-50",
  compensation: "border-amber-300 bg-amber-50",
};

export function RefundTrackerPage() {
  const search = useSearch({ strict: false }) as { paymentId?: string };
  const { history } = usePaymentHistory();
  const [paymentId, setPaymentId] = useState(search.paymentId ?? "");
  const [amount, setAmount] = useState("10.00");
  const [currency, setCurrency] = useState("EUR");
  const [reason, setReason] = useState("");
  const [trackedPaymentId, setTrackedPaymentId] = useState<string | null>(null);
  const { events, state, reconnect } = useEventStream(trackedPaymentId);
  const [expandedStep, setExpandedStep] = useState<string | null>(null);

  const refund = useMutation({
    mutationFn: () =>
      requestRefund(paymentId.trim(), {
        amount: Number(amount),
        currency,
        reason: reason.trim() || undefined,
      }),
    onSuccess: () => setTrackedPaymentId(paymentId.trim()),
  });

  const refundId = refund.data?.id ?? events.find((e) => e.refundId)?.refundId ?? null;

  const ledgerState = useQuery({
    queryKey: ["refund-state", refundId],
    queryFn: () => getRefundState(refundId!),
    enabled: !!refundId,
    refetchInterval: 3000,
    retry: false,
  });

  const refundEvents = useMemo(
    () => events.filter((e) => e.eventType.startsWith("refunds.")),
    [events],
  );
  const byType = useMemo(() => {
    const m = new Map<string, RealtimeEvent>();
    for (const e of refundEvents) if (!m.has(e.eventType)) m.set(e.eventType, e);
    return m;
  }, [refundEvents]);

  return (
    <main className="mx-auto grid max-w-6xl gap-8 px-6 py-8 lg:grid-cols-[360px_1fr]">
      <section className="rounded-lg border border-slate-200 bg-white p-6">
        <h2 className="mb-4 text-base font-semibold">Request a refund</h2>
        <p className="mb-4 text-xs text-slate-500">
          Needs an APPROVED payment. Pick one below, use "refund →" on the Timeline page, or
          paste an id.
        </p>
        {history.length > 0 && (
          <label className="mb-3 block">
            <span className="mb-1 block text-sm font-medium text-slate-700">Recent payments</span>
            <select
              value={history.some((h) => h.id === paymentId) ? paymentId : ""}
              onChange={(e) => e.target.value && setPaymentId(e.target.value)}
              className="w-full rounded-md border border-slate-300 px-3 py-2 font-mono text-xs"
            >
              <option value="">— pick a recent payment —</option>
              {history.map((h) => (
                <option key={h.id} value={h.id}>
                  {h.id.slice(0, 8)}… · {h.merchantId} · {h.amount} {h.currency}
                </option>
              ))}
            </select>
          </label>
        )}
        <label className="mb-3 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Payment ID</span>
          <input
            value={paymentId}
            onChange={(e) => setPaymentId(e.target.value)}
            placeholder="uuid of an approved payment"
            className="w-full rounded-md border border-slate-300 px-3 py-2 font-mono text-xs"
          />
        </label>
        <div className="mb-3 grid grid-cols-2 gap-3">
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-slate-700">Amount</span>
            <input
              value={amount}
              onChange={(e) => setAmount(e.target.value)}
              className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-slate-700">Currency</span>
            <select
              value={currency}
              onChange={(e) => setCurrency(e.target.value)}
              className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
            >
              <option>EUR</option>
              <option>USD</option>
              <option>GBP</option>
            </select>
          </label>
        </div>
        <label className="mb-4 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Reason (optional)</span>
          <input
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
          />
        </label>
        <button
          onClick={() => refund.mutate()}
          disabled={!paymentId.trim() || refund.isPending}
          className="w-full rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:opacity-40"
        >
          {refund.isPending ? "Submitting…" : "POST refund (202)"}
        </button>
        {refund.error && (
          <p className="mt-3 text-sm text-rose-600">{refund.error.message}</p>
        )}
        {refund.data && (
          <p className="mt-3 text-xs text-slate-500">
            refundId <span className="font-mono">{refund.data.id}</span> accepted - the saga now
            runs on its own; steps appear on the right as their events hit the topics.
          </p>
        )}
      </section>

      <section>
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-base font-semibold">Saga choreography, live</h2>
          <ConnectionStatus state={state} onReconnect={reconnect} />
        </div>

        <ol className="space-y-3">
          {SAGA_STEPS.map((step) => {
            const evt = byType.get(step.type);
            const open = expandedStep === step.type;
            return (
              <li
                key={step.type}
                className={`rounded-lg border ${
                  evt ? KIND_STYLES[step.kind] : "border-dashed border-slate-200 bg-slate-50 opacity-60"
                }`}
              >
                <button
                  onClick={() => evt && setExpandedStep(open ? null : step.type)}
                  disabled={!evt}
                  className="block w-full px-4 py-3 text-left disabled:cursor-default"
                >
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">{step.label}</span>
                    {evt ? (
                      <span className="font-mono text-xs text-slate-500">
                        {new Date(evt.occurredAt).toLocaleTimeString()}
                        <span className="ml-2 text-slate-400">{open ? "▲" : "▼"}</span>
                      </span>
                    ) : (
                      <span className="text-xs text-slate-400">not seen</span>
                    )}
                  </div>
                  {evt && !open && (
                    <div className="mt-1 font-mono text-xs text-slate-500">
                      {evt.eventType} · click for full event
                      {evt.reason && <span className="ml-2 text-amber-700">reason: {evt.reason}</span>}
                    </div>
                  )}
                </button>
                {evt && open && (
                  <dl className="grid grid-cols-[130px_1fr] gap-y-1 border-t border-slate-200/60 px-4 py-3 text-xs">
                    {Object.entries(evt)
                      .filter(([, v]) => v != null && v !== "")
                      .map(([k, v]) => (
                        <div key={k} className="contents">
                          <dt className="text-slate-500">{k}</dt>
                          <dd className="break-all font-mono">{String(v)}</dd>
                        </div>
                      ))}
                  </dl>
                )}
              </li>
            );
          })}
        </ol>

        <div className="mt-6 rounded-lg border border-slate-200 bg-white p-4">
          <h3 className="mb-2 text-sm font-semibold">Ledger's view (GET /api/refunds/{"{id}"})</h3>
          {!refundId && <p className="text-xs text-slate-400">No refund yet.</p>}
          {ledgerState.error instanceof ApiError && ledgerState.error.status === 404 && (
            <p className="text-xs text-slate-500">
              404 - ledger hasn't consumed refund-requested yet (its local saga row doesn't exist
              until step 1 lands).
            </p>
          )}
          {ledgerState.data && (
            <p className="text-sm">
              status <span className="font-mono font-medium">{ledgerState.data.status}</span>
              <span className="ml-3 text-xs text-slate-500">
                updated {new Date(ledgerState.data.updatedAt).toLocaleTimeString()}
              </span>
            </p>
          )}
        </div>
      </section>
    </main>
  );
}
