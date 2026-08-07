# ADR-0009: Eureka in the `compose` profile, native Kubernetes discovery in the `k8s` profile

- **Status:** Accepted
- **Date:** 2026-08-08
- **Affects:** M16 (discovery-server, api-gateway), M18

## Context

ADR-0004 removed service-to-service calls, which shrinks the discovery problem to a single
edge: `api-gateway` must resolve and load-balance across instances of `payment-api`,
`realtime-gateway`, `webhook-notifier`, and `analytics`. Nothing else in the system resolves
anything.

On Docker Compose there is no built-in load balancing across replicas of a service — Docker DNS
round-robins A records, with no health awareness and client-side DNS caching that outlives a
dead container. A registry genuinely adds something there.

On Kubernetes, a `Service` **is** the discovery mechanism: a stable DNS name, endpoints
maintained from readiness probes, kube-proxy load balancing. Running Eureka there means every
pod registers with a registry that duplicates, and lags behind, information the platform
already maintains authoritatively — with a slower failure detection path (30 s heartbeat +
90 s eviction, versus a readiness probe removing an endpoint in seconds).

## Decision

Discovery is **profile-scoped**, and the two profiles are the two deployment targets.

**Profile `compose`**
- `discovery-server` (Netflix Eureka) runs as a container.
- Services include `spring-cloud-starter-netflix-eureka-client`, activated only under this
  profile (`spring.cloud.discovery.enabled=true`, `eureka.client.enabled=true`).
- Gateway routes use `lb://payment-api`, resolved through Eureka.
- Eureka is tuned for a dev laptop, not for production defaults:
  `eureka.server.enable-self-preservation=false`, `eureka.instance.lease-renewal-interval-in-seconds=5`.

**Profile `k8s`**
- `discovery-server` is **not deployed**. No Eureka Deployment, no Service, no chart.
- `eureka.client.enabled=false`, `spring.cloud.discovery.enabled=false`.
- Gateway routes target Kubernetes Services directly by DNS:
  `http://payment-api.psp.svc.cluster.local:8080`. Load balancing is kube-proxy's job;
  liveness/readiness probes are the health mechanism.
- `spring-cloud-kubernetes-discovery` is **not** used. It queries the Kubernetes API for
  endpoints and does client-side load balancing — which is a real technique, but it needs RBAC,
  adds an API-server dependency, and buys nothing here because there is exactly one client and
  it has no per-request routing needs. Plain DNS is the smaller correct answer.

Both profiles are exercised: compose is the daily development loop, `k8s` is M18 onward.
Route definitions live in profile-specific config, and the same image runs in both.

**Kafka is unaffected.** Kafka clients discover brokers through `bootstrap.servers` and the
cluster's own metadata protocol; they never touch Eureka or a Kubernetes Service for broker
resolution. Discovery here is only about HTTP at the edge.

## Consequences

**Positive**
- Directly demonstrates why the industry abandoned client-side registries: the same
  application, unchanged, works with a registry and with none, and the k8s path is strictly
  simpler and faster to detect failures.
- No Eureka pod, no Eureka RBAC, no split-brain registry state on the cluster.
- The compose path still teaches the Spring Cloud registry model that a lot of existing Java
  systems run on.

**Negative / accepted costs**
- **Two route configurations to keep in sync**, and a class of bug that only appears in one
  profile. Mitigation: a smoke test per profile that walks every gateway route.
- Eureka client code and dependencies ship in images that will never use them on Kubernetes —
  a few MB and one more autoconfiguration to disable correctly. Verified by asserting at
  startup, under the `k8s` profile, that no `EurekaClient` bean exists.
- Developers must remember which profile they are running; a misconfigured profile fails with a
  confusing `UnknownHostException` rather than a clear message. Mitigation: fail fast on
  startup if the active profile is neither `compose` nor `k8s`.

## Alternatives considered

**Eureka in both environments.** One code path, one mental model, and it does work on
Kubernetes. Rejected: it duplicates platform state with worse failure detection, and it hides
the lesson that the platform replaced this component.

**Kubernetes discovery in both** (`spring-cloud-kubernetes` with a local kind cluster for dev).
Also one code path, and arguably the most "production-like" option. Rejected: it forces a
Kubernetes cluster into the day-one development loop, before M18 has built one, and eliminates
the Eureka learning goal the plan explicitly states.

**No discovery at all; static URLs in both profiles.** Simplest possible. Works, and would be
right for a system this size in the real world. Rejected only because the plan wants Eureka
exposure — and this ADR records that as the reason rather than pretending Eureka is needed.

**Consul or etcd as the registry.** Better than Eureka on most axes, and irrelevant to both the
compose convenience case and the Kubernetes case. Rejected as a third mechanism to learn for no
new insight.
