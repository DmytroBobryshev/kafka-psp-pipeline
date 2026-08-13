// infra/k8s/load/payments.js - M18 phase 3.
//
// Drives POST /api/payments through api-gateway hard enough that psp-connector falls behind, so
// that KEDA's Kafka lag trigger has something real to react to. This is a LAG GENERATOR first and
// a latency benchmark second: the interesting output is `kubectl get hpa`, not the p95.
//
// Run it: infra/k8s/scripts/load-test.sh   (in-cluster Job, or --host for port-forward mode)
//
// ---------------------------------------------------------------------------------------------
// WHY THE DEFAULT RATE IS 5/s AND NOT "as fast as k6 can go"
// ---------------------------------------------------------------------------------------------
// api-gateway applies a RequestRateLimiter as a DEFAULT filter to every route (M16):
// replenishRate 5, burstCapacity 10, keyed by the caller's IP (config.RateLimiterConfig). A
// single-source flood therefore does not produce more payments, it produces 429s - the gateway
// doing exactly what it was built to do. Two consequences, both deliberate:
//
//   * The default arrival rate is 5/s, right at the replenish rate, so essentially every request
//     is a real payment. The limiter is not disabled or bypassed to make the demo look better.
//   * More load is bought with more SOURCE IPs, not a higher rate: the Job's `parallelism` gives
//     each k6 pod its own pod IP and therefore its own token bucket, so N pods = N x 5/s. See
//     k6-job.yaml.
//
// 5/s is already ~13x what one psp-connector replica can drain (~0.39 payments/s: one listener
// thread, simulated provider sleeping a mean 2.55s), so lag builds at ~4.6 messages/second with
// one replica. It does not need to be faster.
//
// ---------------------------------------------------------------------------------------------
// THE PROFILE
// ---------------------------------------------------------------------------------------------
// constant-arrival-rate, not constant-VUs. An open model: k6 starts a request every 200ms
// regardless of how long the previous one took. A closed model (VUs) would throttle itself the
// moment the gateway slowed down - which is precisely when a lag test needs it not to.
//
// Three stages, driven by env vars so the same script covers "smoke" and "the drill":
//   RATE      requests per second per k6 pod           default 5
//   DURATION  how long to hold that rate               default 3m
//   BASE_URL  gateway address                          default http://api-gateway:8000 (in-cluster)

// Only k6 built-ins are imported. The usual `import { randomIntBetween } from
// 'https://jslib.k6.io/k6-utils/...'` is a network fetch performed by the k6 pod at startup, and
// a load test that cannot start because a CDN is unreachable is a load test that will fail on the
// day it is most needed.
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const RATE = Number(__ENV.RATE || 5);
const DURATION = __ENV.DURATION || '3m';
const BASE_URL = __ENV.BASE_URL || 'http://api-gateway:8000';
const MERCHANT_ID = __ENV.MERCHANT_ID || 'merchant-keda-drill';

// Separate counters for the two non-201 outcomes, because they mean opposite things:
// a 429 is the gateway's rate limiter working (expected if RATE > 5), while a 5xx is the pipeline
// actually failing and would invalidate the drill.
const rateLimited = new Counter('gateway_rate_limited_429');
const serverErrors = new Counter('gateway_server_errors_5xx');
const accepted = new Counter('payments_accepted_201');
const acceptRate = new Rate('payments_accepted_ratio');

export const options = {
  scenarios: {
    build_lag: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      // preAllocatedVUs must exceed rate x expected latency, or k6 drops iterations and reports
      // "insufficient VUs" - which looks like the gateway rejecting load when it is k6 refusing
      // to create it. The POST returns as soon as payment-api has committed the row + outbox
      // event (~50ms), so 20 is generous; maxVUs gives headroom if the gateway slows down.
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
  thresholds: {
    // The drill is invalid if the pipeline was erroring rather than merely backlogged.
    gateway_server_errors_5xx: ['count==0'],
    // Deliberately NOT a threshold on http_req_duration: this test is supposed to make the
    // system slow. Failing on latency would fail on success.
  },
};

export default function () {
  // Amounts vary so the ledger's merchant_balances row is a sum that could only come from these
  // requests, and a fixed merchantId so the whole run is one queryable bucket. Both matter for
  // checking afterwards that the backlog was actually WORKED, not just measured - a scale-out
  // that processes nothing is the failure mode this project keeps finding.
  const payload = JSON.stringify({
    merchantId: MERCHANT_ID,
    amount: (Math.floor(Math.random() * 500) + 1) / 100,
    currency: 'EUR',
  });

  const res = http.post(`${BASE_URL}/api/payments`, payload, {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /api/payments' },
  });

  if (res.status === 201) {
    accepted.add(1);
  } else if (res.status === 429) {
    rateLimited.add(1);
  } else if (res.status >= 500) {
    serverErrors.add(1);
  }
  acceptRate.add(res.status === 201);

  check(res, {
    'created or rate-limited (never 5xx)': (r) => r.status === 201 || r.status === 429,
  });
}

export function handleSummary(data) {
  const count = (n) => (data.metrics[n] ? data.metrics[n].values.count : 0);
  const p95 = (n) => (data.metrics[n] ? data.metrics[n].values['p(95)'].toFixed(1) : '-');
  const created = count('payments_accepted_201');
  const limited = count('gateway_rate_limited_429');
  const errors = count('gateway_server_errors_5xx');
  const lines = [
    '',
    '  ===== M18 phase 3 load summary =====',
    `  201 Created (real payments):  ${created}`,
    `  429 rate-limited by gateway:  ${limited}`,
    `  5xx server errors:            ${errors}`,
    `  target rate:                  ${RATE}/s for ${DURATION}  ->  ${BASE_URL}`,
    `  gateway p95 (ms):             ${p95('http_req_duration')}`,
    '',
    `  Those ${created} payments become ${created} records on payments.payment-requested.v1,`,
    '  consumed by group psp-connector.v1 - which is exactly what KEDA is measuring.',
    '',
    '  Watch what that did:',
    '    kubectl get hpa keda-hpa-psp-connector -n kafka -w',
    '    kubectl get pods -n kafka -l app.kubernetes.io/name=psp-connector -w',
    '',
  ];
  return { stdout: lines.join('\n') };
}
