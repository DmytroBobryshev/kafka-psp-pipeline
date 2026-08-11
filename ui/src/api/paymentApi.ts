import type { CreatePaymentRequest, PaymentResponse, ProblemDetail } from "./types";

/** Thrown for any non-2xx response; carries the parsed RFC 7807 body so callers can render it. */
export class PaymentApiError extends Error {
  readonly problem: ProblemDetail;
  readonly status: number;

  constructor(problem: ProblemDetail, status: number) {
    super(problem.detail ?? problem.title ?? `Request failed with status ${status}`);
    this.name = "PaymentApiError";
    this.problem = problem;
    this.status = status;
  }
}

export async function createPayment(request: CreatePaymentRequest): Promise<PaymentResponse> {
  const response = await fetch("/api/payments", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const problem: ProblemDetail = await response.json().catch(() => ({}) as ProblemDetail);
    throw new PaymentApiError(problem, response.status);
  }

  return (await response.json()) as PaymentResponse;
}
