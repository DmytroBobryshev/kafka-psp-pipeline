import { useState } from "react";
import { PaymentForm } from "../components/PaymentForm";
import { EventTimeline } from "../components/EventTimeline";
import { ConnectionStatus } from "../components/ConnectionStatus";
import { useEventStream } from "../hooks/useEventStream";
import type { PaymentResponse } from "../api/types";

/** Page 1 (M17 slice 1, unchanged behaviour): create a payment, watch its events arrive live. */
export function TimelinePage() {
  const [payment, setPayment] = useState<PaymentResponse | null>(null);
  const { events, state, reconnect } = useEventStream(payment?.id ?? null);

  return (
    <main className="mx-auto grid max-w-6xl gap-8 px-6 py-8 lg:grid-cols-[360px_1fr]">
      <PaymentForm onCreated={setPayment} />

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
