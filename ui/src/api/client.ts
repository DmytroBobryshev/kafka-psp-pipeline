import type { ProblemDetail } from "./types";

/**
 * Shared fetch helper for every M17 page API. Mirrors paymentApi.ts's error contract (RFC 7807
 * body surfaced on non-2xx) so all pages render backend errors the same way, but generalized:
 * page 1's paymentApi.ts predates this and is deliberately left untouched.
 */
export class ApiError extends Error {
  readonly problem: ProblemDetail;
  readonly status: number;

  constructor(problem: ProblemDetail, status: number) {
    super(problem.detail ?? problem.title ?? `Request failed with status ${status}`);
    this.name = "ApiError";
    this.problem = problem;
    this.status = status;
  }
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    headers: init?.body ? { "Content-Type": "application/json" } : undefined,
    ...init,
  });

  if (!response.ok) {
    const problem: ProblemDetail = await response.json().catch(() => ({}) as ProblemDetail);
    throw new ApiError(problem, response.status);
  }

  // 202/204-style bodies may be empty; callers that expect void pass T = void.
  const text = await response.text();
  return (text ? JSON.parse(text) : undefined) as T;
}
