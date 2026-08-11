import type { PaymentResponse, RealtimeEvent } from "../api/types";
import type { ConnectionState } from "../hooks/useEventStream";
import { EventCard } from "./EventCard";

interface Props {
  events: RealtimeEvent[];
  payment: PaymentResponse | null;
  state: ConnectionState;
}

export function EventTimeline({ events, payment, state }: Props) {
  if (!payment) {
    return (
      <div className="flex h-64 items-center justify-center rounded-xl border border-dashed border-slate-300 text-sm text-slate-400">
        Create a payment to start watching its timeline.
      </div>
    );
  }

  return (
    <div>
      {events.length === 0 ? (
        <div className="flex h-40 items-center justify-center rounded-xl border border-dashed border-slate-300 text-sm text-slate-400">
          {state === "live" || state === "connecting"
            ? "Connected — waiting for the first event…"
            : "No events received."}
        </div>
      ) : (
        <ol>
          {events.map((event, index) => (
            <EventCard
              key={event.eventId}
              event={event}
              payment={payment}
              priorEvents={events.slice(0, index)}
              index={index}
            />
          ))}
        </ol>
      )}
    </div>
  );
}
