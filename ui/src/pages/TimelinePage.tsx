import { useEffect, useRef, useState } from "react";

import { PaymentForm } from "../components/PaymentForm";
import { EventTimeline } from "../components/EventTimeline";
import { ConnectionStatus } from "../components/ConnectionStatus";
import { useEventStream } from "../hooks/useEventStream";
import { recordPayment, usePaymentHistory } from "../hooks/usePaymentHistory";
import { useCopy } from "../lib/clipboard";
import { createPayment } from "../api/paymentApi";
import { requestRefund } from "../api/refundApi";
import type { PaymentResponse } from "../api/types";

/**
 * Page 1: create a payment, watch its events arrive live. Every created payment also lands in
 * the local history below the form, so its id survives navigation/refresh and feeds the refund
 * page - the SSE stream itself only carries NEW events (auto-offset-reset=latest by design),
 * so re-selecting an old payment arms the stream for its future events (e.g. a refund), it
 * does not replay the past.
 */
export function TimelinePage() {
  const [payment, setPayment] = useState<PaymentResponse | null>(null);
  const { events, state, reconnect } = useEventStream(payment?.id ?? null);
  const { history } = usePaymentHistory();
  const { copy, copiedKey } = useCopy();
  const [refunding, setRefunding] = useState<string | null>(null);

  // Auto-run: one simulated payment every N seconds ("operation with idle"). Interval-driven,
  // uses a fixed demo merchant, follows each new payment's live timeline as it lands.
  const [autoRunning, setAutoRunning] = useState(false);
  const [idleSeconds, setIdleSeconds] = useState(5);
  const [autoCount, setAutoCount] = useState(0);
  const autoTimer = useRef<number | undefined>(undefined);

  const onCreated = (p: PaymentResponse) => {
    recordPayment(p);
    setPayment(p);
  };

  useEffect(() => {
    if (!autoRunning) {
      window.clearInterval(autoTimer.current);
      return;
    }
    const tick = async () => {
      try {
        const p = await createPayment({
          merchantId: "merchant-simulator",
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
  }, [autoRunning, idleSeconds]);

  const refundNow = async (paymentId: string) => {
    setRefunding(paymentId);
    try {
      // Track this payment FIRST so the saga's events land in the live timeline on the right.
      const h = history.find((x) => x.id === paymentId);
      if (h) setPayment({ ...h, status: "", createdAt: h.createdAt } as PaymentResponse);
      await requestRefund(paymentId, { amount: 5.0, currency: "EUR", reason: "simulator refund" });
    } catch (e) {
      console.error("refund failed", e);
    } finally {
      setRefunding(null);
    }
  };

  return (
    <main className="mx-auto grid max-w-6xl gap-8 px-6 py-8 lg:grid-cols-[360px_1fr]">
      <div className="space-y-6">
        <PaymentForm onCreated={onCreated} />

        <section className="rounded-lg border border-slate-200 bg-white p-4">
          <h3 className="mb-2 text-sm font-semibold text-slate-700">Auto-run (operation + idle)</h3>
          <div className="flex items-center gap-3">
            <label className="flex items-center gap-2 text-xs text-slate-600">
              every
              <input
                type="number"
                min={2}
                max={120}
                value={idleSeconds}
                onChange={(e) => setIdleSeconds(Number(e.target.value))}
                className="w-16 rounded-md border border-slate-300 px-2 py-1 text-xs"
              />
              s
            </label>
            <button
              onClick={() => setAutoRunning(!autoRunning)}
              className={`rounded-md px-3 py-1.5 text-xs font-medium ${
                autoRunning ? "bg-rose-600 text-white" : "bg-slate-900 text-white"
              }`}
            >
              {autoRunning ? "Stop" : "Start"}
            </button>
            <span className="text-xs text-slate-400">
              {autoRunning ? `running · ${autoCount} created` : autoCount ? `${autoCount} created` : "idle"}
            </span>
          </div>
          <p className="mt-2 text-[10px] text-slate-400">
            Creates a payment (random 5–100 EUR, merchant-simulator) each interval and follows its
            live timeline. Stop before leaving the page.
          </p>
        </section>

        <section className="rounded-lg border border-slate-200 bg-white p-4">
          <h3 className="mb-2 text-sm font-semibold text-slate-700">Recent payments (this browser)</h3>
          {history.length === 0 && (
            <p className="text-xs text-slate-400">Payments you create appear here with quick actions.</p>
          )}
          <ul className="max-h-64 space-y-1.5 overflow-auto">
            {history.map((h) => (
              <li key={h.id} className="rounded border border-slate-100 px-2.5 py-1.5 text-xs">
                <div className="flex items-center justify-between gap-2">
                  <button
                    className="truncate font-mono text-slate-600 hover:text-slate-900"
                    title="Copy payment id"
                    onClick={() => copy(h.id)}
                  >
                    {copiedKey === h.id ? "copied ✓" : `${h.id.slice(0, 8)}… ⧉`}
                  </button>
                  <span className="whitespace-nowrap text-slate-500">
                    {h.amount} {h.currency}
                  </span>
                </div>
                <div className="mt-0.5 flex items-center justify-between gap-2">
                  <span className="truncate text-slate-400">{h.merchantId}</span>
                  <span className="flex gap-2 whitespace-nowrap">
                    <button
                      className="text-slate-500 underline-offset-2 hover:underline"
                      onClick={() =>
                        setPayment({ ...h, status: "", createdAt: h.createdAt } as PaymentResponse)
                      }
                    >
                      track
                    </button>
                    <button
                      onClick={() => refundNow(h.id)}
                      disabled={refunding === h.id}
                      className="font-medium text-slate-700 underline-offset-2 hover:underline disabled:opacity-40"
                    >
                      {refunding === h.id ? "refunding…" : "refund"}
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
              <span className="ml-2 font-mono text-sm font-normal text-slate-400">
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
