import { useCallback, useEffect, useRef, useState } from "react";
import { KNOWN_EVENT_TYPES, type RealtimeEvent } from "../api/types";

export type ConnectionState =
  | "idle" // no paymentId yet - nothing to connect to
  | "connecting" // EventSource created, waiting for the first byte
  | "live" // connection open, events may arrive at any moment
  | "reconnecting" // dropped after being live; the browser is auto-retrying
  | "closed"; // gave up - either the browser stopped retrying, or we closed it ourselves

/**
 * Owns one SSE connection to realtime-gateway's `GET /api/realtime/events?paymentId=...` and
 * the full connection-lifecycle state machine the module brief asks for: connect on demand,
 * a visible state for connecting/live/reconnecting/closed, and a guaranteed close on unmount.
 *
 * Every event realtime-gateway pushes carries an explicit SSE `event:` name equal to its Avro
 * event type (`PaymentTimelineController#stream`: `.name(event.eventType())`) - never the
 * default unnamed "message" event - so a plain `onmessage` handler would silently see nothing.
 * This hook instead attaches one listener per known event type (see api/types.ts
 * `KNOWN_EVENT_TYPES`) and merges everything into one ordered timeline.
 *
 * Reconnection: the native `EventSource` auto-retries on a transient drop (readyState goes back
 * to CONNECTING) - that case is surfaced as "reconnecting", not silently swallowed. If the
 * browser gives up entirely (readyState CLOSED - e.g. the initial request failed, or the server
 * completed the emitter after its idle timeout) this hook does NOT auto-retry, because that
 * would hide a genuinely dead connection behind an infinite silent retry loop; instead it
 * surfaces "closed" and exposes `reconnect()` for an explicit, visible retry.
 */
export function useEventStream(paymentId: string | null) {
  const [events, setEvents] = useState<RealtimeEvent[]>([]);
  const [state, setState] = useState<ConnectionState>("idle");
  const sourceRef = useRef<EventSource | null>(null);

  const connect = useCallback((id: string) => {
    sourceRef.current?.close();
    setEvents([]);
    setState("connecting");

    const source = new EventSource(`/api/realtime/events?paymentId=${encodeURIComponent(id)}`);
    sourceRef.current = source;

    source.onopen = () => setState("live");

    source.onerror = () => {
      // CLOSED means the browser has already given up and will not retry on its own -
      // a dropped connection must be visible, not silent, so this is a distinct state from a
      // transient retry rather than being folded into "connecting".
      setState(source.readyState === EventSource.CLOSED ? "closed" : "reconnecting");
    };

    for (const eventType of KNOWN_EVENT_TYPES) {
      source.addEventListener(eventType, (raw) => {
        const evt = raw as MessageEvent<string>;
        try {
          const parsed = JSON.parse(evt.data) as RealtimeEvent;
          setEvents((prev) => [...prev, parsed]);
        } catch (err) {
          console.error(`Failed to parse SSE payload for ${eventType}`, err);
        }
      });
    }
  }, []);

  useEffect(() => {
    if (paymentId) {
      connect(paymentId);
    } else {
      setState("idle");
      setEvents([]);
    }
    // Close on unmount AND whenever paymentId changes, before the next connection opens.
    return () => {
      sourceRef.current?.close();
      sourceRef.current = null;
    };
  }, [paymentId, connect]);

  const reconnect = useCallback(() => {
    if (paymentId) connect(paymentId);
  }, [paymentId, connect]);

  return { events, state, reconnect };
}
