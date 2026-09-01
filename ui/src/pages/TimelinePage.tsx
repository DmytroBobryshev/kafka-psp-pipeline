import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { EventTimeline } from "../components/EventTimeline";
import { ConnectionStatus } from "../components/ConnectionStatus";
import { useEventStream } from "../hooks/useEventStream";
import { recordPayment, usePaymentHistory } from "../hooks/usePaymentHistory";
import { useCopy } from "../lib/clipboard";
import { createPayment } from "../api/paymentApi";
import { requestRefund } from "../api/refundApi";
import { listMerchants } from "../api/merchantsApi";
import { ApiError } from "../api/client";
import type { PaymentResponse } from "../api/types";

type Mode = "payment" | "refund";

// Sandbox convention (docs.stripe.com/testing): the amount's ending selects the outcome.
const PAYMENT_OUTCOMES = [
  { key: "succeed", label: "succeed", cents: null, on: "bg-emerald-600 text-white border-emerald-600", off: "text-emerald-700 border-emerald-300 hover:bg-emerald-50" },
  { key: "decline", label: "decline", cents: 13, on: "bg-rose-600 text-white border-rose-600", off: "text-rose-700 border-rose-300 hover:bg-rose-50" },
  { key: "timeout", label: "timeout", cents: 66, on: "bg-amber-500 text-white border-amber-500", off: "text-amber-700 border-amber-300 hover:bg-amber-50" },
] as const;

const REFUND_OUTCOMES = [
  { key: "succeed", label: "succeed", cents: null, on: "bg-emerald-600 text-white border-emerald-600", off: "text-emerald-700 border-emerald-300 hover:bg-emerald-50" },
  { key: "fail", label: "fail", cents: 13, on: "bg-rose-600 text-white border-rose-600", off: "text-rose-700 border-rose-300 hover:bg-rose-50" },
] as const;

export function TimelinePage() {
  const [mode, setMode] = useState<Mode>("payment");
  const [payment, setPayment] = useState<PaymentResponse | null>(null);
  const { events, state, reconnect } = useEventStream(payment?.id ?? null);
  const { history } = usePaymentHistory();
  const { copy, copiedKey } = useCopy();

  // Only ACTIVE merchants may transact; the picker enforces it client-side, payment-api
  // enforces it for real.
  const merchants = useQuery({
    queryKey: ["merchants", "ACTIVE"],
    queryFn: () => listMerchants({ status: "ACTIVE" }),
    retry: false,
  });
  const activeMerchants = merchants.data?.items ?? [];

  const [merchantId, setMerchantId] = useState("");
  const [amount, setAmount] = useState("49.99");
  const [paymentOutcome, setPaymentOutcome] = useState<(typeof PAYMENT_OUTCOMES)[number]>(PAYMENT_OUTCOMES[0]);
  const [refundOutcome, setRefundOutcome] = useState<(typeof REFUND_OUTCOMES)[number]>(REFUND_OUTCOMES[0]);
  const [currency, setCurrency] = useState("EUR");
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const selectedMerchant = activeMerchants.find((m) => m.merchantId === merchantId);
  const merchantCurrencies = selectedMerchant
    ? selectedMerchant.allowedCurrencies?.length
      ? selectedMerchant.allowedCurrencies
      : [selectedMerchant.payoutCurrency]
    : ["EUR"];
  useEffect(() => {
    if (!merchantId && activeMerchants.length > 0) setMerchantId(activeMerchants[0].merchantId);
  }, [activeMerchants, merchantId]);
  useEffect(() => {
    if (!merchantCurrencies.includes(currency)) setCurrency(merchantCurrencies[0]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [merchantId, merchants.data]);

  const [refundPaymentId, setRefundPaymentId] = useState("");
  const [refundAmount, setRefundAmount] = useState("5.00");
  const [refundReason, setRefundReason] = useState("");
  const [refunding, setRefunding] = useState(false);

  const [autoRunning, setAutoRunning] = useState(false);
  const [idleSeconds, setIdleSeconds] = useState(5);
  const [autoCount, setAutoCount] = useState(0);
  const autoTimer = useRef<number | undefined>(undefined);

  const onCreated = (p: PaymentResponse) => {
    recordPayment(p);
    setPayment(p);
  };

  const withEnding = (base: string, cents: number | null) => {
    const n = Math.max(1, Math.floor(Number(base) || 10));
    return cents == null ? `${n}.00` : `${n}.${String(cents).padStart(2, "0")}`;
  };

  const simulatePayment = async (cents: number | null) => {
    setError(null);
    setCreating(true);
    try {
      const p = await createPayment({
        merchantId,
        amount: Number(withEnding(amount, cents)),
        currency,
      });
      onCreated(p);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  };

  const simulateRefund = async (cents: number | null) => {
    if (!refundPaymentId) return;
    setError(null);
    setRefunding(true);
    try {
      const h = history.find((x) => x.id === refundPaymentId);
      if (h) setPayment({ ...h, status: "", createdAt: h.createdAt } as PaymentResponse);
      const known = history.find((x) => x.id === refundPaymentId);
      await requestRefund(refundPaymentId, {
        amount: Number(cents == null ? refundAmount : withEnding(refundAmount, cents)),
        currency: known?.currency ?? "EUR",
        reason: refundReason.trim() || undefined,
      });
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    } finally {
      setRefunding(false);
    }
  };

  useEffect(() => {
    if (!autoRunning || !merchantId) {
      window.clearInterval(autoTimer.current);
      return;
    }
    const tick = async () => {
      try {
        const p = await createPayment({
          merchantId,
          amount: Math.round((5 + Math.random() * 95) * 100) / 100,
          currency: "EUR",
        });
        onCreated(p);
        setAutoCount((c) => c + 1);
      } catch (e) {
        console.error("auto-run payment failed", e);
      }
    };
    void tick();
    autoTimer.current = window.setInterval(tick, Math.max(2, idleSeconds) * 1000);
    return () => window.clearInterval(autoTimer.current);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRunning, idleSeconds, merchantId]);

  const fullAmountOf = (id: string) => history.find((h) => h.id === id)?.amount;

  return (
    <main className="mx-auto grid max-w-[1500px] gap-8 px-6 py-8 lg:grid-cols-[400px_1fr]">
      <div className="space-y-6">
        <section className="rounded-lg border border-slate-300 bg-white p-5">
          <div className="mb-4 flex rounded-lg border border-slate-300 p-0.5">
            {(["payment", "refund"] as Mode[]).map((m) => (
              <button
                key={m}
                onClick={() => setMode(m)}
                className={`flex-1 rounded-md px-3 py-1.5 text-sm font-medium capitalize ${
                  mode === m ? "bg-slate-900 text-white" : "text-slate-700 hover:bg-slate-200"
                }`}
              >
                simulate {m}
              </button>
            ))}
          </div>

          {mode === "payment" ? (
            <>
              <label className="mb-3 block">
                <span className="mb-1 block text-sm font-medium text-slate-700">
                  Merchant <span className="text-xs font-normal text-slate-500">(ACTIVE only)</span>
                </span>
                {activeMerchants.length > 0 ? (
                  <select
                    value={merchantId}
                    onChange={(e) => setMerchantId(e.target.value)}
                    className="w-full rounded-md border border-slate-400 px-3 py-2 text-sm"
                  >
                    {activeMerchants.map((m) => (
                      <option key={m.merchantId} value={m.merchantId}>
                        {m.displayName} ({m.merchantId})
                      </option>
                    ))}
                  </select>
                ) : (
                  <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                    No ACTIVE merchants{merchants.error ? " (list unavailable)" : ""} — create one on
                    the Merchants page first.
                  </div>
                )}
              </label>
              <div className="mb-3 grid grid-cols-2 gap-3">
                <label className="block">
                  <span className="mb-1 block text-sm font-medium text-slate-700">Amount (base)</span>
                  <input
                    value={amount}
                    onChange={(e) => setAmount(e.target.value)}
                    className="w-full rounded-md border border-slate-400 px-3 py-2 text-sm"
                  />
                </label>
                <label className="block">
                  <span className="mb-1 block text-sm font-medium text-slate-700">Currency</span>
                  <select
                    value={currency}
                    onChange={(e) => setCurrency(e.target.value)}
                    className="w-full rounded-md border border-slate-400 px-3 py-2 text-sm"
                  >
                    {merchantCurrencies.map((c) => (
                      <option key={c}>{c}</option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="mb-3">
                <span className="mb-1 block text-xs font-medium text-slate-700">Outcome</span>
                <div className="grid grid-cols-3 gap-2">
                  {PAYMENT_OUTCOMES.map((o) => (
                    <button
                      key={o.key}
                      onClick={() => setPaymentOutcome(o)}
                      className={`rounded-md border px-2 py-1.5 text-xs font-medium ${
                        paymentOutcome.key === o.key ? o.on : `bg-white ${o.off}`
                      }`}
                    >
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>
              <button
                onClick={() => simulatePayment(paymentOutcome.cents)}
                disabled={creating || !merchantId}
                className="w-full rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:opacity-40"
              >
                {creating
                  ? "Sending…"
                  : `Create payment · ${withEnding(amount, paymentOutcome.cents)} ${currency}`}
              </button>
              <p className="mt-2 text-[11px] leading-relaxed text-slate-500">
                The amount's ending selects the outcome (Stripe/Adyen sandbox convention).
                Timeout never publishes a status — it goes down the retry path.
              </p>
            </>
          ) : (
            <>
              <label className="mb-3 block">
                <span className="mb-1 block text-sm font-medium text-slate-700">
                  Payment ID <span className="text-xs font-normal text-slate-500">(paste, or pick from suggestions)</span>
                </span>
                <input
                  list="recent-payment-ids"
                  value={refundPaymentId}
                  onChange={(e) => {
                    setRefundPaymentId(e.target.value.trim());
                    const full = fullAmountOf(e.target.value.trim());
                    if (full != null) setRefundAmount(String(full));
                  }}
                  placeholder="uuid of a succeeded payment"
                  className="w-full rounded-md border border-slate-400 px-3 py-2 font-mono text-xs"
                />
                <datalist id="recent-payment-ids">
                  {history.map((h) => (
                    <option key={h.id} value={h.id}>
                      {h.merchantId} · {h.amount} {h.currency}
                    </option>
                  ))}
                </datalist>
              </label>
              <div className="mb-3 grid grid-cols-[1fr_auto] gap-2">
                <label className="block">
                  <span className="mb-1 block text-sm font-medium text-slate-700">Amount</span>
                  <input
                    value={refundAmount}
                    onChange={(e) => setRefundAmount(e.target.value)}
                    className="w-full rounded-md border border-slate-400 px-3 py-2 text-sm"
                  />
                </label>
                <button
                  onClick={() => {
                    const full = fullAmountOf(refundPaymentId);
                    if (full != null) setRefundAmount(String(full));
                  }}
                  className="mt-6 rounded-md border border-slate-400 px-3 py-2 text-xs text-slate-700 hover:bg-slate-100"
                >
                  full
                </button>
              </div>
              <label className="mb-3 block">
                <span className="mb-1 block text-sm font-medium text-slate-700">
                  Reason <span className="text-xs font-normal text-slate-500">(optional)</span>
                </span>
                <input
                  value={refundReason}
                  onChange={(e) => setRefundReason(e.target.value)}
                  className="w-full rounded-md border border-slate-400 px-3 py-2 text-sm"
                />
              </label>
              <div className="mb-3">
                <span className="mb-1 block text-xs font-medium text-slate-700">Outcome</span>
                <div className="grid grid-cols-2 gap-2">
                  {REFUND_OUTCOMES.map((o) => (
                    <button
                      key={o.key}
                      onClick={() => setRefundOutcome(o)}
                      className={`rounded-md border px-2 py-1.5 text-xs font-medium ${
                        refundOutcome.key === o.key ? o.on : `bg-white ${o.off}`
                      }`}
                    >
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>
              <button
                onClick={() => simulateRefund(refundOutcome.cents)}
                disabled={refunding || !refundPaymentId}
                className="w-full rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:opacity-40"
              >
                {refunding
                  ? "Sending…"
                  : `Send refund · ${refundOutcome.cents == null ? refundAmount : withEnding(refundAmount, refundOutcome.cents)} EUR`}
              </button>
              <p className="mt-2 text-[11px] leading-relaxed text-slate-500">
                A failed refund fires the compensating transaction — watch reservation-released
                appear in the timeline.
              </p>
            </>
          )}
          {error && <p className="mt-3 text-xs text-rose-600">{error}</p>}
        </section>

        <section className="rounded-lg border border-slate-300 bg-white p-4">
          <h3 className="mb-2 text-sm font-semibold text-slate-700">Auto-run (operation + idle)</h3>
          <div className="flex items-center gap-3">
            <label className="flex items-center gap-2 text-xs text-slate-700">
              every
              <input
                type="number"
                min={2}
                max={120}
                value={idleSeconds}
                onChange={(e) => setIdleSeconds(Number(e.target.value))}
                className="w-16 rounded-md border border-slate-400 px-2 py-1 text-xs"
              />
              s
            </label>
            <button
              onClick={() => setAutoRunning(!autoRunning)}
              disabled={!merchantId}
              className={`rounded-md px-3 py-1.5 text-xs font-medium disabled:opacity-40 ${
                autoRunning ? "bg-rose-600 text-white" : "bg-slate-900 text-white"
              }`}
            >
              {autoRunning ? "Stop" : "Start"}
            </button>
            <span className="text-xs text-slate-500">
              {autoRunning ? `running · ${autoCount} created` : autoCount ? `${autoCount} created` : "idle"}
            </span>
          </div>
          <p className="mt-2 text-[11px] text-slate-500">
            Random 5–100 EUR payments for the selected merchant, one per interval.
          </p>
        </section>

        <section className="rounded-lg border border-slate-300 bg-white p-4">
          <h3 className="mb-2 text-sm font-semibold text-slate-700">Recent payments (this browser)</h3>
          {history.length === 0 && (
            <p className="text-xs text-slate-500">Payments you create appear here with quick actions.</p>
          )}
          <ul className="max-h-64 space-y-1.5 overflow-auto">
            {history.map((h) => (
              <li key={h.id} className="rounded border border-slate-200 px-2.5 py-1.5 text-xs">
                <div className="flex items-center justify-between gap-2">
                  <button
                    className="truncate font-mono text-slate-700 hover:text-slate-900"
                    title="Copy payment id"
                    onClick={() => copy(h.id)}
                  >
                    {copiedKey === h.id ? "copied ✓" : `${h.id.slice(0, 8)}… ⧉`}
                  </button>
                  <span className="whitespace-nowrap text-slate-600">
                    {h.amount} {h.currency}
                  </span>
                </div>
                <div className="mt-0.5 flex items-center justify-between gap-2">
                  <span className="truncate text-slate-500">{h.merchantId}</span>
                  <span className="flex gap-2 whitespace-nowrap">
                    <button
                      className="rounded-md border border-slate-400 bg-white px-2.5 py-1 text-xs font-medium text-slate-800 shadow-sm hover:bg-slate-200"
                      onClick={() =>
                        setPayment({ ...h, status: "", createdAt: h.createdAt } as PaymentResponse)
                      }
                    >
                      track
                    </button>
                    <button
                      className="rounded-md border border-slate-400 bg-white px-2.5 py-1 text-xs font-medium text-slate-800 shadow-sm hover:bg-slate-200"
                      onClick={() => {
                        setMode("refund");
                        setRefundPaymentId(h.id);
                        setRefundAmount(String(h.amount));
                      }}
                    >
                      refund
                    </button>
                  </span>
                </div>
              </li>
            ))}
          </ul>
        </section>
      </div>

      <section>
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-base font-semibold text-slate-900">
            Event timeline
            {payment && (
              <span className="ml-2 font-mono text-sm font-normal text-slate-500">
                {payment.id}
              </span>
            )}
          </h2>
          <ConnectionStatus state={state} onReconnect={reconnect} />
        </div>

        <EventTimeline events={events} payment={payment} state={state} />
      </section>
    </main>
  );
}
