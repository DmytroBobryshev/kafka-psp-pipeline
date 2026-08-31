import { useCallback, useSyncExternalStore } from "react";
import type { PaymentResponse } from "../api/types";

/**
 * Client-side payment history (localStorage): every payment created in THIS browser, newest
 * first, capped at 50. Exists because the timeline page used to forget the payment on the
 * first navigation - and a paymentId nobody can remember made the refund page unusable. This
 * is deliberately not a server query: the server-side transactions panel (/payments) is the
 * source of truth across browsers; this is the "what did I just create" working set that
 * survives a refresh and feeds quick actions (track / refund) without a round trip.
 */
export interface PaymentHistoryEntry {
  id: string;
  merchantId: string;
  amount: number;
  currency: string;
  createdAt: string;
}

const KEY = "psp.payment-history.v1";
const listeners = new Set<() => void>();
let cache: PaymentHistoryEntry[] | null = null;

function read(): PaymentHistoryEntry[] {
  if (cache) return cache;
  try {
    cache = JSON.parse(localStorage.getItem(KEY) ?? "[]") as PaymentHistoryEntry[];
  } catch {
    cache = [];
  }
  return cache;
}

function write(entries: PaymentHistoryEntry[]) {
  cache = entries;
  localStorage.setItem(KEY, JSON.stringify(entries));
  listeners.forEach((l) => l());
}

export function recordPayment(p: PaymentResponse) {
  write([
    { id: p.id, merchantId: p.merchantId, amount: p.amount, currency: p.currency, createdAt: p.createdAt },
    ...read().filter((e) => e.id !== p.id),
  ].slice(0, 50));
}

export function usePaymentHistory() {
  const history = useSyncExternalStore(
    useCallback((cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    }, []),
    read,
  );
  const clear = useCallback(() => write([]), []);
  return { history, clear };
}
