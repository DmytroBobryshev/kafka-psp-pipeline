import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { PaymentForm } from "../components/PaymentForm";
import { EventTimeline } from "../components/EventTimeline";
import { ConnectionStatus } from "../components/ConnectionStatus";
import { useEventStream } from "../hooks/useEventStream";
import { recordPayment, usePaymentHistory } from "../hooks/usePaymentHistory";
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

  const onCreated = (p: PaymentResponse) => {
    recordPayment(p);
    setPayment(p);
  };

  return (
    <main className="mx-auto grid max-w-6xl gap-8 px-6 py-8 lg:grid-cols-[360px_1fr]">
      <div className="space-y-6">
        <PaymentForm onCreated={onCreated} />

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
                    onClick={() => navigator.clipboard.writeText(h.id)}
                  >
                    {h.id.slice(0, 8)}… ⧉
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
                    <Link
                      to="/refunds"
                      search={{ paymentId: h.id }}
                      className="font-medium text-slate-700 underline-offset-2 hover:underline"
                    >
                      refund →
                    </Link>
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
