# Kafka PSP Pipeline

A miniature **payment service provider** built as an event-driven system on Apache Kafka:
Java 21 / Spring Boot microservices (hexagonal architecture, ArchUnit-enforced), Avro + Schema
Registry, a React + TypeScript UI, deployed to a local Kubernetes (kind) cluster with Strimzi,
KEDA, Debezium and full Prometheus/Grafana monitoring.

Merchants take payments and refunds; the platform authorizes them against a simulated provider,
keeps exactly-once balances, delivers merchant webhooks with retries/DLQ, expires stuck
operations per-merchant, and shows every lifecycle stage live in the UI.

**Documentation:**

- [`documentation/`](documentation/) — **how, what and why it works** + Mermaid diagrams
  (system overview, payment/refund sequences, config propagation, deployment).
- [`docs/PLAN.md`](docs/PLAN.md) — the module-by-module learning plan (M1..M19).
- [`docs/adr/`](docs/adr/) — architecture decision records.
- [`docs/M19-failure-drills.md`](docs/M19-failure-drills.md) /
  [`part 2`](docs/M19-failure-drills-part2.md) /
  [`docs/M20-lifecycle-trails-and-expiration.md`](docs/M20-lifecycle-trails-and-expiration.md)
  — failure drills and feature verification, measured against the live cluster.

---

## Running from zero

Target platform: **macOS** (Apple Silicon or Intel). Linux works the same minus Docker Desktop
specifics. Everything runs locally — no cloud accounts needed.

### 1. Install the tooling

Install [Homebrew](https://brew.sh) if you don't have it, then:

```bash
# Container runtime (the whole cluster lives inside it)
brew install --cask docker          # Docker Desktop; start it once after install

# Kubernetes tooling
brew install kind kubectl helm

# Java toolchain (service jars are built on the host)
brew install openjdk@21 maven       # or: sdkman with java 21-tem
sudo ln -sfn "$(brew --prefix)/opt/openjdk@21/libexec/openjdk.jdk" \
  /Library/Java/JavaVirtualMachines/openjdk-21.jdk

git --version                       # ships with Xcode CLT; install if missing
```

Optional (only for UI development / e2e scripts — the deployable UI builds inside Docker):

```bash
brew install node pnpm              # Node 22+
```

**Docker Desktop settings** (Settings → Resources): give it at least **8 GB RAM** (12 GB is
comfortable) and make sure you have **~25 GB free disk**. The stack runs a 3-node Kafka
cluster, 9 services, Postgres, Mongo, MinIO, Schema Registry, Kafka Connect and a monitoring
stack — it is a real workload.

Verify:

```bash
docker info          # daemon running
kind version && kubectl version --client && helm version
java --version       # 21.x
mvn --version
```

### 2. Bring the platform up

```bash
git clone <this-repo> kafka && cd kafka

# 1) Kafka platform: kind cluster (3 nodes), pinned Strimzi operator, Kafka 4.3 (KRaft),
#    all topics, all SASL/SCRAM users with deny-by-default ACLs. Idempotent. ~3 min.
./infra/k8s/scripts/up.sh

# 2) Applications: builds every service jar (maven) + docker image, loads them into kind,
#    deploys the umbrella Helm chart (services, Postgres, Mongo, MinIO, Schema Registry,
#    Kafka Connect + Debezium, KEDA, ingress-nginx, UI). ~5-8 min first run.
./infra/k8s/scripts/deploy-apps.sh

# 3) Monitoring: Prometheus + Grafana + kafka-exporter in the `monitoring` namespace.
./infra/k8s/scripts/install-monitoring.sh
```

Open **http://localhost** — the UI is served through ingress on port 80.

### 3. Known first-run quirks (both have one-line fixes)

| Symptom | Fix |
|---|---|
| `deploy-apps.sh` times out on `payment-outbox-connector`, task shows `FAILED` | The Debezium task can start before Flyway creates the Postgres publication. Once payment-api is Running: `kubectl -n kafka annotate kafkaconnector payment-outbox-connector strimzi.io/restart-task=0` |
| `http://localhost` refuses connections after a Docker/host restart | ingress-nginx loses its hostPort binding: `kubectl -n ingress-nginx delete pod -l app.kubernetes.io/component=controller` |

### 4. First steps in the UI

1. **Merchants** → `+ new merchant`: pick an id, 1–3 allowed currencies, payment/refund
   expiration windows, and a webhook URL — grab a free inbox at
   [webhook.site](https://webhook.site) to see real deliveries arrive.
2. **Simulator** → create payments (outcome chips use magic amount endings: `.13` = declined,
   `.66` = provider timeout) or switch to refund mode; auto-run generates steady traffic.
3. **Transactions** → expand any row: the full lifecycle trail (created → pending → IPN
   received → verified → paid, and the six-stage refund trail), provider references, webhook
   deliveries, refund form.
4. **Dashboard** → totals, latest operations, live 1-minute Kafka Streams windows per merchant.
5. **DLQ / Cluster** → poison-message console with replay; topics, consumer groups and lag.

### 5. Everyday commands

```bash
# All tests (unit + ArchUnit + Testcontainers ITs; Docker must be running)
mvn clean verify

# Rebuild & redeploy ONE service after a change (tag = current deployed tag)
TAG=$(kubectl -n kafka get deploy payment-api -o jsonpath='{.spec.template.spec.containers[0].image}' | cut -d: -f2)
mvn -q -pl services/payment-api -am package -DskipTests
docker build -q -t psp/payment-api:$TAG services/payment-api
kind load docker-image --name kafka-psp psp/payment-api:$TAG
kubectl -n kafka rollout restart deploy/payment-api

# UI dev loop (hot reload against the cluster's APIs)
cd ui && pnpm install && pnpm dev

# Regenerate typed API clients from the running services
kubectl -n kafka port-forward svc/payment-api 8085:8085 &
kubectl -n kafka port-forward svc/analytics 8089:8089 &
cd ui && pnpm gen:api

# UI stability e2e (needs a global playwright install)
NODE_PATH=$(npm root -g) node ui/e2e/layout-shift.cjs

# Kafka CLI access from the host (SASL listeners forwarded per broker)
./infra/k8s/scripts/port-forward.sh

# Grafana (namespace `monitoring`)
kubectl -n monitoring get secret grafana -o jsonpath='{.data.admin-password}' | base64 -d; echo
kubectl -n monitoring port-forward svc/grafana 3000:80

# Tear everything down / start fresh
kind delete cluster --name kafka-psp
```

There is also a docker-compose variant of the whole stack (pre-k8s modules) under
[`infra/compose/`](infra/compose/) with its own README.

---

## Stack & persistence decisions

| Service | DB | Why |
|---|---|---|
| payment-api | PostgreSQL | outbox needs ACID |
| ledger | PostgreSQL | balance integrity, transactions |
| psp-connector | PostgreSQL | dedup tables with unique constraints |
| webhook-notifier | MongoDB | delivery-attempt documents |
| analytics | MongoDB + RocksDB | projections + Streams state |
| audit-trail | MongoDB | Kafka Connect sink, zero code |

## Hexagonal template per service

```
domain/          # pure Java: aggregate, value objects, ports. No Spring, no Kafka
application/     # use cases orchestrating ports
adapters/
  in/web         # controllers, DTOs, MapStruct dto<->domain
  in/kafka       # listeners, MapStruct avro<->domain
  out/persistence# entities, repos, MapStruct entity<->domain
  out/kafka      # producers
config/
```

`domain/` compiles with zero framework dependencies — enforced per service by an ArchUnit test.
Annotation-processor order (root `pom.xml`, inherited): **Lombok → lombok-mapstruct-binding →
MapStruct** — wrong order silently breaks MapStruct over Lombok accessors.
