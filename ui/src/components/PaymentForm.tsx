import { useState, type FormEvent, type ReactNode } from "react";
import { useMutation } from "@tanstack/react-query";
import { createPayment, PaymentApiError } from "../api/paymentApi";
import type { PaymentResponse } from "../api/types";

const CURRENCIES = ["EUR", "USD", "GBP"];

interface Props {
  onCreated: (payment: PaymentResponse) => void;
}

function Field({
  label,
  error,
  children,
}: {
  label: string;
  error?: string;
  children: ReactNode;
}) {
  return (
    <label className="block">
      <span className="mb-1.5 block text-sm font-medium text-slate-700">{label}</span>
      {children}
      {error && <p className="mt-1.5 text-sm text-red-600">{error}</p>}
    </label>
  );
}

const inputClass =
  "w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm outline-none transition focus:border-indigo-500 focus:ring-2 focus:ring-indigo-100 aria-[invalid=true]:border-red-400 aria-[invalid=true]:ring-red-100";

export function PaymentForm({ onCreated }: Props) {
  const [merchantId, setMerchantId] = useState("merchant-acme");
  const [amount, setAmount] = useState("49.99");
  const [currency, setCurrency] = useState("EUR");

  const mutation = useMutation({
    mutationFn: createPayment,
    onSuccess: onCreated,
  });

  const problem = mutation.error instanceof PaymentApiError ? mutation.error.problem : null;
  const fieldErrors = problem?.errors ?? {};
  const hasFieldErrors = Object.keys(fieldErrors).length > 0;
  const topLevelError = problem && !hasFieldErrors ? mutation.error?.message : null;

  function handleSubmit(event: FormEvent) {
    event.preventDefault();
    mutation.mutate({ merchantId, amount: Number(amount), currency });
  }

  return (
    <section className="h-fit rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
      <h2 className="text-base font-semibold text-slate-900">Create payment</h2>
      <p className="mt-1 text-sm text-slate-500">
        Posts to <code className="rounded bg-slate-100 px-1 py-0.5 text-xs">/api/payments</code>.
      </p>

      <form onSubmit={handleSubmit} className="mt-5 space-y-4">
        <Field label="Merchant ID" error={fieldErrors.merchantId}>
          <input
            className={inputClass}
            value={merchantId}
            onChange={(e) => setMerchantId(e.target.value)}
            aria-invalid={Boolean(fieldErrors.merchantId)}
            placeholder="merchant-acme"
            required
          />
        </Field>

        <Field label="Amount" error={fieldErrors.amount}>
          <input
            className={inputClass}
            type="number"
            step="0.01"
            min="0"
            value={amount}
            onChange={(e) => setAmount(e.target.value)}
            aria-invalid={Boolean(fieldErrors.amount)}
            required
          />
        </Field>

        <Field label="Currency" error={fieldErrors.currency}>
          <select
            className={inputClass}
            value={currency}
            onChange={(e) => setCurrency(e.target.value)}
            aria-invalid={Boolean(fieldErrors.currency)}
          >
            {CURRENCIES.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </Field>

        {topLevelError && (
          <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
            {topLevelError}
          </div>
        )}

        <button
          type="submit"
          disabled={mutation.isPending}
          className="w-full rounded-lg bg-indigo-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {mutation.isPending ? "Creating…" : "Create payment"}
        </button>

        {mutation.isSuccess && (
          <p className="text-center text-xs text-slate-400">
            Created {mutation.data.id} — watch the timeline for its events.
          </p>
        )}
      </form>
    </section>
  );
}
