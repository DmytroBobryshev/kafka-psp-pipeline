import type { PaymentResponse, RealtimeEvent } from "../api/types";

interface Props {
  event: RealtimeEvent;
  payment: PaymentResponse;
  /** Events seen so far on this timeline, in arrival order - used to resolve a causationId chain. */
  priorEvents: RealtimeEvent[];
  index: number;
}

const STATUS_COLOR: Record<string, string> = {
  CREATED: "bg-slate-100 text-slate-700 ring-slate-200",
  REQUESTED: "bg-indigo-100 text-indigo-700 ring-indigo-200",
  RESERVED: "bg-indigo-100 text-indigo-700 ring-indigo-200",
  SUCCEEDED: "bg-emerald-100 text-emerald-700 ring-emerald-200",
  COMPLETED: "bg-emerald-100 text-emerald-700 ring-emerald-200",
  DECLINED: "bg-red-100 text-red-700 ring-red-200",
  FAILED: "bg-red-100 text-red-700 ring-red-200",
  TIMEOUT: "bg-amber-100 text-amber-700 ring-amber-200",
  RELEASED: "bg-slate-100 text-slate-700 ring-slate-200",
};

function statusClass(status?: string | null): string {
  if (!status) return "bg-slate-100 text-slate-600 ring-slate-200";
  return STATUS_COLOR[status] ?? "bg-slate-100 text-slate-600 ring-slate-200";
}

function formatClock(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString(undefined, { hour12: false }) + `.${String(d.getMilliseconds()).padStart(3, "0")}`;
}

function formatElapsed(iso: string, sinceIso: string): string {
  const ms = new Date(iso).getTime() - new Date(sinceIso).getTime();
  if (ms < 0) return "";
  if (ms < 1000) return `+${ms}ms`;
  return `+${(ms / 1000).toFixed(2)}s`;
}

function shortId(id: string): string {
  return id.length <= 13 ? id : `${id.slice(0, 8)}…${id.slice(-4)}`;
}

/** Domain fields worth showing, skipping anything null/undefined so the card stays scannable. */
function domainFields(event: RealtimeEvent): Array<[string, string]> {
  const entries: Array<[string, string]> = [];
  if (event.refundId) entries.push(["refundId", event.refundId]);
  if (event.status) entries.push(["status", event.status]);
  if (event.reason) entries.push(["reason", event.reason]);
  if (event.providerReference) entries.push(["providerReference", event.providerReference]);
  return entries;
}

export function EventCard({ event, payment, priorEvents, index }: Props) {
  const causedBy = event.causationId
    ? priorEvents.find((e) => e.eventId === event.causationId)
    : undefined;

  return (
    // Visible ids are truncated for readability, so the full values are exposed here for the e2e
    // test to assert the causal chain on. Test hooks, not styling - keep them stable.
    <li
      className="relative pb-8 pl-10 last:pb-0"
      data-event-id={event.eventId}
      data-causation-id={event.causationId ?? ""}
    >
      {/* connecting line */}
      <span className="absolute left-[13px] top-3 -bottom-2 w-px bg-slate-200 last:hidden" aria-hidden />
      {/* node */}
      <span
        className="absolute left-2 top-1.5 flex h-4 w-4 items-center justify-center rounded-full border-2 border-white bg-indigo-500 text-[10px] font-bold text-white shadow"
        aria-hidden
      >
        {index + 1}
      </span>

      <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
          <span className="font-mono text-sm font-semibold text-slate-900">{event.eventType}</span>
          <span className="font-mono text-xs text-slate-400">
            {formatClock(event.occurredAt)}{" "}
            <span className="text-slate-300">({formatElapsed(event.occurredAt, payment.createdAt)})</span>
          </span>
        </div>

        <div className="mt-2 flex flex-wrap items-center gap-1.5">
          {event.status && (
            <span
              className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${statusClass(event.status)}`}
            >
              {event.status}
            </span>
          )}
          {event.source && (
            <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-600 ring-1 ring-inset ring-slate-200">
              source: {event.source}
            </span>
          )}
          {(event.topic || event.partition !== undefined || event.key) && (
            <span className="inline-flex items-center rounded-full bg-violet-50 px-2 py-0.5 font-mono text-xs text-violet-700 ring-1 ring-inset ring-violet-200">
              {event.topic ?? "?"}
              {event.partition !== undefined ? `[${event.partition}]` : ""} key={event.key ?? "?"}
            </span>
          )}
        </div>

        <dl className="mt-3 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs">
          <dt className="font-medium text-slate-400">eventId</dt>
          <dd className="font-mono text-slate-600" title={event.eventId}>
            {shortId(event.eventId)}
          </dd>

          <dt className="font-medium text-slate-400">causationId</dt>
          <dd className="font-mono text-slate-600">
            {event.causationId ? (
              <span title={event.causationId}>
                {shortId(event.causationId)}
                {causedBy && (
                  <span className="ml-1 text-slate-400">
                    (caused by #{priorEvents.indexOf(causedBy) + 1} {causedBy.eventType})
                  </span>
                )}
              </span>
            ) : (
              // The gateway forwards causationId, so an absent one means this event genuinely
              // had no cause - it is the root of the chain, not missing data.
              <span className="italic text-slate-400">none — root event</span>
            )}
          </dd>

          {domainFields(event).map(([key, value]) => (
            <>
              <dt key={`${key}-label`} className="font-medium text-slate-400">
                {key}
              </dt>
              <dd key={`${key}-value`} className="text-slate-600">
                {value}
              </dd>
            </>
          ))}
        </dl>
      </div>
    </li>
  );
}
