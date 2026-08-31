# infra/k8s - M18 phase 1: Kafka on Kubernetes via Strimzi

## Purpose

Run the same Kafka cluster `infra/compose` runs - three brokers, KRaft, SASL/SCRAM-SHA-512,
deny-by-default ACLs - on Kubernetes, with the operator doing the work `create-topics.sh` and
`kafka-init/init-security.sh` did by hand.

The Kafka concepts this phase demonstrates are not new Kafka concepts. They are the *same*
concepts moved from imperative scripts to declarative resources, which is the point:

- **Topics as GitOps config.** `kafka-topics --create --if-not-exists` becomes 26 `KafkaTopic`
  CRs the Topic Operator reconciles on a loop. A topic edited with the CLI is put back.
- **ACLs as GitOps config.** M14's 119-binding matrix becomes 11 `KafkaUser` CRs. An ACL deleted
  with `kafka-acls` is put back.
- **Credentials that nobody chooses.** Compose read eleven passwords from a gitignored `.env`.
  Strimzi generates one per user into a Secret.
- **The listener map shrinks from four to two**, because Strimzi runs inter-broker and controller
  traffic on mutual TLS it manages, deleting the entire SASL/PLAIN bootstrap problem M14 had to
  solve by hand.

**Phase 1 is Kafka only.** No Spring services, no Helm charts for them, no KEDA, no k6, no
ingress - those are phases 2 and 3.

---

## Layout

```
infra/k8s/
├── kind.yaml                        the 3-node kind cluster (1 control-plane, 2 workers)
├── hello-world.yaml                 the M18 warm-up deployment, unrelated to Kafka
├── strimzi/
│   ├── namespace.yaml               strimzi-system - the operator's own namespace
│   └── values.yaml                  Helm values: watch ONLY `kafka`, bounded resources
├── kafka/
│   ├── 00-namespace.yaml            kafka - cluster, topics, users, generated Secrets
│   ├── 10-nodepool-combined.yaml    3 nodes, combined controller+broker, storage, sizing
│   ├── 15-metrics-configmap.yaml    jmx_exporter rules for the brokers (M19 revisit)
│   ├── 20-kafka.yaml                the Kafka CR: version, listeners, authz, broker config
│   ├── topics/                      26 KafkaTopic CRs, generated from docs/diagrams/topic-map.md
│   │   ├── 00-business.yaml         9  business topics
│   │   ├── 10-webhooks.yaml         10 webhook delivery chain (.v1 retired + .v2 live)
│   │   ├── 20-dlq.yaml              2  consumer DLQs
│   │   ├── 30-request-reply.yaml    2  M12 request-reply
│   │   └── 40-connect-internal.yaml 3  Connect worker bookkeeping
│   └── users/                       11 KafkaUser CRs = M14's principals + ACL matrix
│       ├── 00-admin.yaml            superuser, no ACLs
│       ├── 10-payment-api.yaml      … 15-realtime-gateway.yaml   the six services
│       └── 20-connect.yaml          … 23-kafka-exporter.yaml     the four platform principals
├── monitoring/                      M19 revisit: Prometheus + Grafana (see "Monitoring" below)
│   ├── 00-namespace.yaml            monitoring - the metrics stack's own namespace
│   ├── prometheus-values.yaml       Helm values: subcharts off, 3 explicit scrape jobs
│   └── grafana-values.yaml          Helm values: provisioned datasource + M15's dashboards
└── scripts/
    ├── up.sh                        bring everything up from nothing, idempotent
    ├── down.sh                      workload | namespace | cluster
    ├── smoke-test.sh                produce + consume as real principals, plus the denial half
    ├── install-monitoring.sh        pinned Prometheus + Grafana charts, dashboards from git
    └── port-forward.sh              reach the cluster from the host on localhost:29092-29094
```

Why split by concern rather than one big file: `kubectl apply -f kafka/topics/` and
`kubectl apply -f kafka/users/` are the two commands you actually run repeatedly, and the node
pool has to be applied before the Kafka CR.

---

## The operator: Strimzi 1.1.0, pinned

| | |
|---|---|
| Strimzi | **1.1.0** (released 2026-06-27) |
| Installed as | Helm chart `strimzi-kafka-operator-helm-3-chart-1.1.0.tgz`, downloaded from the GitHub release asset |
| Operator namespace | `strimzi-system` |
| Watches | `kafka` only (`watchNamespaces: [kafka]`) - **not** cluster-wide |
| Kafka version | **4.3.0** (Strimzi 1.1.0 ships 4.2.0, 4.2.1, 4.3.0 and rejects anything else) |
| CRD API version | `kafka.strimzi.io/v1` - **`v1beta2` is no longer served** |

```bash
STRIMZI_VERSION=1.1.0
curl -fsSL -o /tmp/strimzi.tgz \
  https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-kafka-operator-helm-3-chart-${STRIMZI_VERSION}.tgz
helm upgrade --install strimzi-cluster-operator /tmp/strimzi.tgz \
  --namespace strimzi-system --values infra/k8s/strimzi/values.yaml --wait
```

**Pinned, not `latest`.** The quickstart everyone copies is
`kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka'`. That URL is a moving
target, and a Strimzi upgrade is a rolling restart of every broker in every watched namespace. It
should happen because someone changed a number in `scripts/up.sh`, not because it was Tuesday.

**Helm and not the flat `strimzi-cluster-operator-1.1.0.yaml`.** That bundle hardcodes the
placeholder namespace `myproject` and only creates RoleBindings in the operator's own namespace.
Making it watch a *different* namespace means a `sed` over 15,317 lines plus hand-written
RoleBindings in the watched namespace. The chart does it from one value. The artifact is still a
pinned release asset, so this is not `helm repo add && install whatever's newest`.

Three version facts worth knowing before you touch anything:

1. **Kafka 4.3.0 here vs 3.7 in compose** (`cp-kafka:7.7.1`). Not a decision - Strimzi 1.1.0
   simply does not ship an image older than 4.2.0. Everything in the topic map and the ACL matrix
   survived the jump unchanged, but it does mean this cluster and the compose cluster are not the
   same Kafka.
2. **ZooKeeper does not exist in Strimzi 1.x** and KRaft is not a mode you opt into. The
   `strimzi.io/kraft: enabled` and `strimzi.io/node-pools: enabled` annotations that older guides
   tell you to add are vestigial.
3. **`Kafka.spec.kafka.replicas`, `.storage` and `.resources` are gone**, moved to
   `KafkaNodePool`. The CRD does not ignore them - it rejects the manifest outright with
   `strict decoding error: unknown field "spec.kafka.resources"`.

---

## The `Kafka` CR's shape, and why

### Three combined controller+broker nodes, one node pool

`KafkaNodePool/combined`, `replicas: 3`, `roles: [controller, broker]`.

Separate pools (3 controllers + 3 brokers) is the production answer: a controller that is not
also serving fetch traffic cannot be knocked out of the quorum by a broker-side GC pause or a
disk-full on a data partition, and the roles can be sized and rolled independently.

It is the wrong answer *here*, for one measurable reason. The kind "cluster" is three containers
inside a single Docker VM with **~8 GiB of RAM total** (`docker info` → `MemTotal 8217473024`),
not three machines with 7.6 GiB each. Separate roles means six JVMs instead of three; at the
1.5 GiB/pod ceiling below that is 9 GiB of limits before the operators and kube-system are
counted, and the VM would spend the module OOM-killing brokers.

It is also the honest continuation of compose, which runs
`KAFKA_PROCESS_ROLES: broker,controller` on all three brokers. Combined roles keep the k8s
topology identical to the compose topology, so the broker-failure drill compares like with like.
Splitting is a one-file change (two `KafkaNodePool`s instead of one) and is the natural follow-up
on a machine with more memory.

### Two listeners, not four

| Listener | Port | Type | Protocol | Auth | Who dials it |
|---|---|---|---|---|---|
| `internal` | 9092 | `internal` | `SASL_PLAINTEXT` | SCRAM-SHA-512 | in-cluster clients - the Spring services (phase 2), Connect, Schema Registry, AKHQ |
| `external` | 9094 | `cluster-ip` | `SASL_PLAINTEXT` | SCRAM-SHA-512 | the laptop, via `scripts/port-forward.sh` on localhost:29092-29094 |
| *(CONTROLPLANE-9090)* | 9090 | Strimzi-managed | **SSL / mTLS** | cert | KRaft controller quorum |
| *(REPLICATION-9091)* | 9091 | Strimzi-managed | **SSL / mTLS** | cert | inter-broker replication |

The bottom two rows are not in any file in this repo. Strimzi creates them, issues the
certificates, mounts them and rotates them. Confirmed on the running cluster:

```
listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,\
                               INTERNAL-9092:SASL_PLAINTEXT,EXTERNAL-9094:SASL_PLAINTEXT
inter.broker.listener.name=REPLICATION-9091
controller.listener.names=CONTROLPLANE-9090
```

Compare `infra/compose/README.md`'s listener map: four listeners, two of them
(`BROKER`, `CONTROLLER`) existing only because SCRAM credentials live in the cluster metadata log
and therefore cannot be used to bootstrap that log - so a SASL/PLAIN `broker` superuser had to be
defined in the broker config file, and anything that could reach `kafka1:9094` and knew its
password was a cluster superuser. **That principal has no equivalent here.** compose's own README
predicted this: *"On Kubernetes (M18) Strimzi replaces this with mutual TLS between brokers,
which is the real answer."*

`cluster-ip` rather than `nodeport` for the external listener because on macOS the kind node IPs
(172.22.0.x) live inside the Docker VM's network and are not routable from the host - a NodePort
would be allocated and permanently unreachable, i.e. dead config that reads as though it worked.

### Authentication and authorization: M14, unchanged

```yaml
authorization:
  type: simple          # == org.apache.kafka.metadata.authorizer.StandardAuthorizer,
  superUsers: [admin]   #    the same class compose set, with allow.everyone.if.no.acl.found=false
```

On the running broker:

```
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:CN=psp-kafka,O=io.strimzi;User:CN=psp-entity-topic-operator,O=io.strimzi;\
            User:CN=psp-entity-user-operator,O=io.strimzi;User:CN=psp-kafka-exporter,O=io.strimzi;\
            User:CN=psp-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:admin
```

`User:admin` is ours; the six `CN=...,O=io.strimzi` principals are the operator's own mTLS
identities, added automatically. Compose's second superuser, `User:broker`, is gone with the
listener that needed it.

### Broker config carried over from compose

| Setting | Value | Why it survives the move |
|---|---|---|
| `min.insync.replicas` | `2` | with `acks=all`: survive one broker, refuse two. The whole M19 drill hangs off it |
| `auto.create.topics.enable` | `false` | topics are CRs now; an accidental produce to a typo must fail loudly |
| `default.replication.factor` | `3` | |
| `offsets.topic.replication.factor` | `3` | |
| `transaction.state.log.replication.factor` / `.min.isr` | `3` / `2` | ledger's transactional producer depends on both |
| `log.retention.ms` | `604800000` | 7 d cluster default, overridden per topic by the CRs |
| `compression.type` | `zstd` | |
| `unclean.leader.election.enable` | `false` | flipped deliberately in the M19 drill |
| `num.partitions` | `1` | only reachable via an explicit CreateTopics anyway |

Nothing sets `listeners`, `advertised.listeners` or `security.*`: Strimzi rejects those prefixes,
because it derives them from `spec.kafka.listeners`. The single most dangerous line in
`infra/compose/docker-compose.yml` - the one its README opens with a warning about - **cannot be
written here**.

### Resource sizing

| Pod | requests | limits | JVM heap |
|---|---|---|---|
| `psp-combined-0..2` (×3) | 1Gi / 250m | 1536Mi / 1500m | `-Xms768m -Xmx768m` |
| `psp-entity-operator` (2 containers) | 256Mi / 50m each | 384Mi / 500m each | default |
| `strimzi-cluster-operator` | 384Mi / 100m | 384Mi / 1000m | default |

Ceilings total ~5.6 GiB against the VM's ~8 GiB, leaving room for kube-system and the drill's
client pods. The heap is set explicitly rather than left to Strimzi's derive-50%-of-the-limit
default (which would also give 768m) - an implicit heap is exactly what turns "why did the broker
get OOMKilled" into an afternoon.

Storage: JBOD with one 5 GiB `persistent-claim` per node, `deleteClaim: false`,
`kraftMetadata: shared`. kind's default StorageClass is `rancher.io/local-path`, which is
hostPath-backed - the 5 GiB is a claim size, not an enforced quota. JBOD-with-one-volume rather
than a bare `persistent-claim` so adding a disk later is an append to a list instead of a storage
*type* change, which Strimzi refuses.

---

## Bringing it up from nothing

```bash
./infra/k8s/scripts/up.sh
```

Idempotent, ~6 minutes cold (most of it pulling ~500 MB of images). It creates the kind cluster
if missing, both namespaces, the pinned operator, the node pool, the Kafka CR, then waits for
Ready before applying topics and users - the entity operator has to exist before a `KafkaTopic`
means anything.

**Stop `infra/compose` first.** It publishes 29092-29094 on the host, which `port-forward.sh`
binds, and more importantly it is another ~4 GiB of Kafka in a VM that has 8.

The equivalent by hand, in order:

```bash
kind create cluster --config infra/k8s/kind.yaml            # if it does not exist
kubectl apply -f infra/k8s/strimzi/namespace.yaml
kubectl apply -f infra/k8s/kafka/00-namespace.yaml

curl -fsSL -o /tmp/strimzi.tgz \
  https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.1.0/strimzi-kafka-operator-helm-3-chart-1.1.0.tgz
helm upgrade --install strimzi-cluster-operator /tmp/strimzi.tgz \
  -n strimzi-system --values infra/k8s/strimzi/values.yaml --wait

kubectl apply -f infra/k8s/kafka/10-nodepool-combined.yaml  # pool BEFORE the Kafka CR
kubectl apply -f infra/k8s/kafka/20-kafka.yaml
kubectl wait kafka/psp -n kafka --for=condition=Ready --timeout=15m

kubectl apply -f infra/k8s/kafka/topics/
kubectl apply -f infra/k8s/kafka/users/
kubectl wait kafkatopic --all -n kafka --for=condition=Ready --timeout=5m
kubectl wait kafkauser  --all -n kafka --for=condition=Ready --timeout=5m
```

Tear down: `./infra/k8s/scripts/down.sh [workload|namespace|cluster]`. The default keeps the PVCs.

---

## Getting a user's password

Strimzi **generates** every SCRAM password. Nobody picks one, nothing is committed, and there is
no `.env` here at all. Each `KafkaUser` produces a Secret of the same name in `kafka`:

```bash
kubectl get secret payment-api -n kafka -o jsonpath='{.data.password}' | base64 -d
```

The Secret has two keys:

| Key | Contents |
|---|---|
| `password` | the raw SCRAM password |
| `sasl.jaas.config` | a ready-made `ScramLoginModule required username="..." password="...";` line |

`sasl.jaas.config` is the one phase 2 will actually use - a Spring service mounts it straight
into `spring.kafka.properties.sasl.jaas.config` from a `secretKeyRef` and never sees the password
as a value it could log.

**Rotation is a delete.** `kubectl delete secret payment-api -n kafka` - the User Operator mints
a new password, updates the SCRAM credential in the cluster, and rewrites the Secret. Compose's
equivalent was editing `.env` and re-running `kafka-init`, then restarting every client by hand.

Supplying your own password is possible
(`spec.authentication.password.valueFrom.secretKeyRef`) and is deliberately not used here.

---

## Talking to the cluster

### From inside the cluster

Bootstrap: `psp-kafka-bootstrap.kafka.svc.cluster.local:9092`, SASL_PLAINTEXT / SCRAM-SHA-512.

```bash
kubectl exec -n kafka psp-combined-0 -- bash -c 'cat > /tmp/c.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="admin" password="'"$(kubectl get secret admin -n kafka -o jsonpath='{.data.password}' | base64 -d)"'";
EOF'
kubectl exec -n kafka psp-combined-0 -- bin/kafka-topics.sh \
  --bootstrap-server psp-kafka-bootstrap:9092 --command-config /tmp/c.properties --list
```

### From the host

```bash
./infra/k8s/scripts/port-forward.sh    # blocks; Ctrl-C stops all three
```

Then bootstrap on `localhost:29092` - the same address the compose cluster used.

**Why three forwards and not one.** A client bootstraps against any broker, receives metadata
naming the leader of every partition by its *advertised* address, and connects to those addresses
directly. Forward only the bootstrap service and you get a client that connects, fetches metadata
and then cannot reach a single leader. So the `external` listener overrides advertisedHost and
advertisedPort per broker, and the script makes those three addresses real:

```
advertised.listeners=...,EXTERNAL-9094://localhost:29092     # psp-combined-0
advertised.listeners=...,EXTERNAL-9094://localhost:29093     # psp-combined-1
advertised.listeners=...,EXTERNAL-9094://localhost:29094     # psp-combined-2
```

One name trap: Strimzi names the per-broker Services for a `cluster-ip` listener after the **node
pool**, `psp-combined-0..2`, not after the listener. Only the bootstrap Service carries the
listener name (`psp-kafka-external-bootstrap`). `kubectl get svc -n kafka` is the authority.

---

## What happens if someone edits a topic with the CLI

**It gets put back.** That is the entire argument for `KafkaTopic` CRs, and it is worth running
once rather than believing.

```
# before
retention.ms=604800000

# a well-meaning operator, bypassing git entirely
$ kafka-configs.sh --alter --entity-type topics --entity-name refunds.reservation-released.v1 \
    --add-config retention.ms=99999999
Completed updating config for topic refunds.reservation-released.v1.
retention.ms=99999999

# ≤ 60 s later, with nobody touching anything
retention.ms=604800000
```

The Topic Operator diffs every `KafkaTopic` against the live topic every
`reconciliationIntervalMs` (set to 60 s here; Strimzi's default is 120 s) and closes the gap. The
CR is the source of truth; the cluster is a cache of it. The same holds for ACLs - see below.

Three qualifications, because the reconciliation is not symmetric:

1. **Partition counts only go up, and the operator cannot help you down.** Lower `spec.partitions`
   below the live count and the CR goes `Ready=False`; the live topic is left alone. Verified on
   this cluster - patching `refunds.reservation-released.v1` from 6 to 3 gives:

   ```
   $ kubectl get kafkatopic refunds.reservation-released.v1 -n kafka \
       -o jsonpath='{.status.conditions[*].status} {.status.conditions[*].message}'
   False Decreasing partitions not supported

   $ kafka-topics.sh --describe --topic refunds.reservation-released.v1
   Topic: refunds.reservation-released.v1  PartitionCount: 6  ReplicationFactor: 3
   ```

   The operator refuses rather than destroying data, and re-applying the correct CR clears it.
   This matches the topic map's change rule 2.
2. **Topics with no CR are left completely alone.** This is load-bearing: Kafka Streams creates
   `analytics-streams.v1-*` changelogs and repartition topics itself, and the topic map says
   *"Never create, rename, or hand-edit these"*. The Topic Operator only manages topics it has a
   CR for, so the two mechanisms coexist without a fight.
3. **`kubectl delete kafkatopic` deletes the real topic and its data.** Removing a row from
   `topics/` is not a documentation change.

The practical rule: `docs/diagrams/topic-map.md` → `infra/k8s/kafka/topics/*.yaml` → `kubectl
apply`. Never the CLI. Topic map change rule 4 already said so; this directory is what it meant.

---

## M14's ACL matrix, as `KafkaUser` CRs

**This is the payoff of doing M14 before M18.** The principals and the least-privilege matrix are
identical - 11 principals, **119 bindings**, verified below - but they are now YAML the operator
applies and re-applies rather than a bash script that ran once.

### The mapping, concretely

`infra/compose/kafka-init/init-security.sh` line 187, ledger's transactional-id grant, which the
helper `txn_acl ledger prefixed "ledger-tx-" Write Describe` expands to:

```bash
kafka-acls --bootstrap-server kafka1:9094 --command-config /etc/kafka/secrets/broker-admin.properties \
  --add \
  --allow-principal User:ledger \
  --transactional-id "ledger-tx-" \
  --resource-pattern-type prefixed \
  --operation Write --operation Describe
```

is, in `kafka/users/12-ledger.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: ledger                       # -> User:ledger, and Secret/ledger
  labels:
    strimzi.io/cluster: psp
spec:
  authentication:
    type: scram-sha-512              # the credential kafka-configs --add-config SCRAM-SHA-512 made
  authorization:
    type: simple
    acls:
      - resource:
          type: transactionalId      # --transactional-id
          name: ledger-tx-
          patternType: prefix        # --resource-pattern-type prefixed  (note: `prefix`, not `prefixed`)
        operations: [Write, Describe] # --operation Write --operation Describe
        # type: allow                 # the default; `deny` is --deny-principal
```

Term-for-term:

| `kafka-acls` | `KafkaUser` |
|---|---|
| `--allow-principal User:ledger` | `metadata.name: ledger` + `type: allow` (the default) |
| `--deny-principal` | `type: deny` |
| `--topic` / `--group` / `--cluster` / `--transactional-id` | `resource.type: topic` / `group` / `cluster` / `transactionalId` |
| `--resource-pattern-type literal\|prefixed` | `resource.patternType: literal\|prefix` |
| `--operation X --operation Y` | `operations: [X, Y]` |
| `--allow-host` | `host:` (defaults to `*`) |
| the SCRAM credential, made separately by `kafka-configs --alter --add-config` | `spec.authentication.type: scram-sha-512` - same resource |

That last row is the quiet improvement: in compose the credential and the permissions were two
unrelated commands against two different subsystems, and it was entirely possible to have one
without the other. Here they are one object with one Ready condition.

### The principals

| Principal | Bindings | Notes |
|---|---|---|
| `admin` | 0 | superuser, in `Kafka.spec.kafka.authorization.superUsers`. No ACLs, because an ACL for a superuser is dead config that reads as load-bearing |
| `payment-api` | 12 | includes the deliberately-unused `Write` on `payments.payment-requested.v1` - the "allowed" half of PLAN.md's M14 proof |
| `psp-connector` | 18 | DLQ grant is `Write`-only: a DLQ is not a queue (ADR-0006) |
| `ledger` | 21 | the only `transactionalId` grant in the system |
| `webhook-notifier` | 11 | one prefixed grant covers the whole `.v2` chain; cannot reach the retired `.v1.dlq` |
| `analytics` | 12 | `All` on prefix `analytics-streams.v1` - the Streams internal-topic gotcha |
| `realtime-gateway` | 8 | read-only by construction: not one `Write` in the file |
| `connect` | 18 | the principal that really writes `payments.payment-requested.v1` since M6 |
| `schema-registry` | 9 | `Create` on `_schemas` is genuinely load-bearing here - that topic has no CR |
| `akhq` | 7 | cluster-wide and read-only: can browse everything, can produce nothing |
| `kafka-exporter` | 3 | `Describe` only - it can see lag, never a message body |
| | **119** | |

`broker` (compose's PLAIN inter-broker superuser) has **no equivalent** - see the listener section.

Two topics have no CR and it is deliberate: `_schemas` (name starts with `_`, which is not a legal
`metadata.name`, and it is not in the topic map - Schema Registry creates it, which is what its
`Create` grant is for) and the `analytics-streams.v1-*` internals (Streams owns them).

### ACLs get put back too

```
$ kafka-acls.sh --remove --force --allow-principal User:realtime-gateway \
    --topic refunds. --resource-pattern-type prefixed --operation Read
$ kafka-acls.sh --list | grep -c 'principal=User:'
118

# ≤ 60 s later
$ kafka-acls.sh --list | grep -c 'principal=User:'
119
```

In compose, that removal would have survived until someone re-ran `init-security.sh` and happened
to notice. `init-security.sh` says so itself: *"nothing here removes, so a stale ACL from an
earlier edit survives until you delete it explicitly."* The User Operator has no such gap in
either direction - it adds what is missing and removes what is not declared.

---

## Prove it - run against this exact cluster

### a. Everything Ready

```
$ kubectl get pods -n strimzi-system
NAME                                        READY   STATUS    RESTARTS   AGE
strimzi-cluster-operator-777fc98449-x94ff   1/1     Running   0          10m

$ kubectl get pods -n kafka
NAME                                   READY   STATUS    RESTARTS   AGE
psp-combined-0                         1/1     Running   0          7m19s
psp-combined-1                         1/1     Running   0          7m19s
psp-combined-2                         1/1     Running   0          7m19s
psp-entity-operator-786db8c79f-m76vt   2/2     Running   0          6m23s

$ kubectl get kafka -n kafka
NAME   READY   WARNINGS   KAFKA VERSION   METADATA VERSION
psp    True               4.3.0           4.3-IV0

$ kubectl get pvc -n kafka
data-0-psp-combined-0   Bound   5Gi   RWO   standard
data-0-psp-combined-1   Bound   5Gi   RWO   standard
data-0-psp-combined-2   Bound   5Gi   RWO   standard
```

### b. All 26 topics reconciled

```
$ kubectl get kafkatopics -n kafka
NAME                                               CLUSTER   PARTITIONS   REPLICATION FACTOR   READY
connect.configs                                    psp       1            3                    True
connect.offsets                                    psp       25           3                    True
connect.status                                     psp       5            3                    True
ledger.ledger-entry-recorded.v1                    psp       6            3                    True
merchants.merchant-config-changed.v1               psp       3            3                    True
payments.payment-requested.v1                      psp       12           3                    True
payments.payment-requested.v1.psp-connector.dlq    psp       3            3                    True
payments.payment-status-changed.v1                 psp       12           3                    True
payments.payment-status-changed.v1.ledger.dlq      psp       3            3                    True
psp.provider-status-query.v1                       psp       6            3                    True
psp.provider-status-reply.v1                       psp       6            3                    True
refunds.funds-reserved.v1                          psp       6            3                    True
refunds.refund-completed.v1                        psp       6            3                    True
refunds.refund-failed.v1                           psp       6            3                    True
refunds.refund-requested.v1                        psp       6            3                    True
refunds.reservation-released.v1                    psp       6            3                    True
webhooks.webhook-delivery-requested.v1             psp       6            3                    True
webhooks.webhook-delivery-requested.v1.dlq         psp       3            3                    True
webhooks.webhook-delivery-requested.v1.retry.15m   psp       6            3                    True
webhooks.webhook-delivery-requested.v1.retry.1m    psp       6            3                    True
webhooks.webhook-delivery-requested.v1.retry.5s    psp       6            3                    True
webhooks.webhook-delivery-requested.v2             psp       6            3                    True
webhooks.webhook-delivery-requested.v2.dlq         psp       3            3                    True
webhooks.webhook-delivery-requested.v2.retry.15m   psp       6            3                    True
webhooks.webhook-delivery-requested.v2.retry.1m    psp       6            3                    True
webhooks.webhook-delivery-requested.v2.retry.5s    psp       6            3                    True
```

Partition counts and replication factors are the topic map's, unedited.

### c. All 11 users reconciled, with Secrets

```
$ kubectl get kafkausers -n kafka
NAME               CLUSTER   AUTHENTICATION   AUTHORIZATION   READY
admin              psp       scram-sha-512                    True
akhq               psp       scram-sha-512    simple          True
analytics          psp       scram-sha-512    simple          True
connect            psp       scram-sha-512    simple          True
kafka-exporter     psp       scram-sha-512    simple          True
ledger             psp       scram-sha-512    simple          True
payment-api        psp       scram-sha-512    simple          True
psp-connector      psp       scram-sha-512    simple          True
realtime-gateway   psp       scram-sha-512    simple          True
schema-registry    psp       scram-sha-512    simple          True
webhook-notifier   psp       scram-sha-512    simple          True

$ kubectl get secrets -n kafka | grep -E '^(admin|payment-api|ledger|akhq) '
admin            Opaque   2
akhq             Opaque   2
ledger           Opaque   2
payment-api      Opaque   2

$ kafka-acls.sh --list | grep -c 'principal=User:'
119
```

### d. Produce and consume as real principals

`./infra/k8s/scripts/smoke-test.sh` - `payment-api` produces, `psp-connector` consumes with group
`psp-connector.v1`, the same pair the running system uses on that topic. Neither is a superuser,
so the ACLs are genuinely under test.

```
==> PRODUCE as User:payment-api -> payments.payment-requested.v1 (acks=all, min.insync.replicas=2)
    sent key=pay-k8s-001

==> CONSUME as User:psp-connector, group psp-connector.v1
Partition:9	pay-k8s-001	{"paymentId":"pay-k8s-001","amount":4200,"currency":"EUR","proof":"M18-phase1"}
```

### e. The denial half

PLAN.md's M14 line is *"payment-api may write `payments.requested` but not `ledger.entries` -
prove it by trying"*. Same credential as (d), different topic:

```
==> DENIAL: User:payment-api -> ledger.ledger-entry-recorded.v1 (no grant exists)
WARN  ... {ledger.ledger-entry-recorded.v1=TOPIC_AUTHORIZATION_FAILED}
ERROR Topic authorization failed for topics [ledger.ledger-entry-recorded.v1]
```

Deny-by-default survived the move to Kubernetes with no weakening.

### Broker failure proof

Measured on the live cluster. One command was issued - `kubectl delete pod` - and nothing else:

```
kubectl delete pod -n kafka psp-combined-1

  t+5s    psp-combined-1: 0/1 Completed
  t+10s   psp-combined-1: 0/1 Running
  t+40s   psp-combined-1: 1/1 Running
```

ISR afterwards, read as `admin` (the cluster requires SASL, so an unauthenticated
`kafka-topics.sh` just times out on `listTopics` - worth knowing, since it looks like a network
problem):

```
Topic: payments.payment-requested.v1  PartitionCount: 12  ReplicationFactor: 3
  Configs: compression.type=zstd, min.insync.replicas=2, cleanup.policy=delete,
           retention.ms=604800000, unclean.leader.election.enable=false
  Partition: 0  Leader: 2  Replicas: 2,0,1  Isr: 0,1,2
  Partition: 1  Leader: 0  Replicas: 0,1,2  Isr: 0,1,2
  Partition: 2  Leader: 2  Replicas: 1,2,0  Isr: 0,1,2
```

**Compare this with the same drill in compose (M2).** There, `docker stop kafka2` shrank the ISR
and the cluster kept serving - correct behaviour, but the broker stayed down until a human typed
`docker start`. Here the pod was *deleted*, not stopped, and forty seconds later it existed again
with its identity, its storage claim, and its place in the ISR restored. Nobody asked for that.

That difference is the entire argument for an operator. Compose gives you a Kafka cluster that
tolerates a broker failing; Kubernetes plus Strimzi gives you one that *repairs* it. The StatefulSet
guarantees a pod called `psp-combined-1` returns with the same PVC attached, so it rejoins as the
same broker rather than as a new one needing partition reassignment - which is why deleting a Kafka
pod is survivable at all, and why the same trick against a Deployment would not be.

#### The ACL count is the other half of the proof

```
ACLs on the Kubernetes cluster: 119
ACLs on the compose cluster (M14): 119
```

Not copied. The compose number came from `kafka-acls` invocations in a shell script; this one came
from eleven `KafkaUser` CRs the operator reconciled. The same least-privilege matrix expressed
twice, in two systems, landing on the same number. That is the payoff of doing M14 before M18:
security was not re-derived at the Kubernetes boundary, it was translated.

The credential model genuinely improved on the way across. In compose, `.env` held passwords we
chose. Here Strimzi generates each user's password into a Secret we never see, and rotation is
`kubectl delete secret <user>`. Two subsystems and two unrelated commands became one object with
one `Ready` condition.

*(to be filled in)*

---

## compose vs k8s: what got simpler, what got harder

### Simpler

- **The inter-broker security problem disappeared.** Four listeners became two. The SASL/PLAIN
  `broker` superuser, the `KAFKA_LISTENER_NAME_*_SASL_JAAS_CONFIG` triple-underscore encoding, the
  chicken-and-egg paragraph explaining why SCRAM cannot bootstrap itself, and the honest admission
  that anything reaching `kafka1:9094` was a cluster superuser - all gone, replaced by mTLS the
  operator manages. This is the single biggest win and it is a *security* win, not a convenience.
- **`advertised.listeners` cannot be got wrong.** compose's README opens with a warning about it
  and calls it "the #1 misconfiguration". Strimzi derives it, and rejects any attempt to set it.
- **Credentials nobody chooses.** No `.env`, no eleven passwords, no gitignore anxiety, and
  rotation is `kubectl delete secret`.
- **Topics and ACLs are declared, and stay declared.** The two demonstrations above.
- **Two scripts deleted, in effect.** `create-topics.sh` (plus its second `kafka-configs --alter`
  pass for compaction settings, needed because `--create --if-not-exists` never alters an existing
  topic) and `init-security.sh` both become directories of YAML. There is no "run this once" step
  that can be forgotten or half-run.
- **Health has an answer, not a claim.** `kubectl get kafka` and the `Ready` column on every
  `KafkaTopic`/`KafkaUser` are the cluster's own assessment. A script's exit code is not.

### Harder

- **More moving parts to understand before anything runs.** compose is one file you can read
  top-to-bottom. Here: an operator, a Helm release, CRDs, a node pool, a Kafka CR, an entity
  operator that is a separate pod, PVCs and a StorageClass. The first `kubectl get pods` after a
  typo is not self-explanatory.
- **Errors moved into `.status.conditions`.** A bad `KAFKA_*` env var in compose shows up in
  `docker logs` seconds later. A bad CR field is either rejected by the API server with a strict
  decoding error (good) or accepted and quietly ignored while the CR sits `Ready=False` with the
  real message three levels into `kubectl get kafka -o yaml` (less good).
- **Reaching it from the laptop is harder, not easier.** compose published three host ports.
  Here it needs a `cluster-ip` listener, per-broker advertised-address overrides, and three
  concurrent `kubectl port-forward` processes - and the reason it needs all three is a Kafka
  protocol fact, not a Kubernetes one.
- **The version matrix is now someone else's.** Strimzi 1.1.0 offers exactly three Kafka
  versions. compose could run any tag Confluent published. Upgrading Kafka now means checking what
  the operator supports first.
- **Memory is a real constraint.** compose's brokers had no limits and used what they used. Here
  every pod needs requests and limits chosen by hand, on a VM small enough that a wrong number
  gets something OOMKilled.

### What the operator does that a human did before

| Was | Now |
|---|---|
| `create-topics.sh` run by hand; a second `kafka-configs --alter` pass for compaction settings | Topic Operator, reconciling 26 CRs every 60 s |
| `init-security.sh` creating 11 SCRAM credentials from `.env` | User Operator, generating 11 passwords into 11 Secrets |
| `init-security.sh` applying 119 ACL bindings, once, additively, never removing | User Operator, converging the live ACL set on the CRs in both directions |
| a `broker` PLAIN superuser in the broker config, shared by all three brokers | per-broker mTLS certificates, issued and rotated by the cluster operator |
| `docker compose up -d --force-recreate` to change a broker setting | rolling restart, one pod at a time, ordered so the quorum survives |
| noticing a broker died and restarting it | the StrimziPodSet controller, before you notice |
| `KAFKA_ADVERTISED_LISTENERS` written by hand per broker | derived from `spec.kafka.listeners`, unwritable by hand |

---

## Troubleshooting

**`Kafka` CR stuck not-Ready, no pods.** The node pool is probably not bound: check
`metadata.labels."strimzi.io/cluster"` on the `KafkaNodePool` matches the `Kafka`'s
`metadata.name` exactly. A wrong value is silently ignored - the pool is simply not part of any
cluster. Same trap on every `KafkaTopic` and `KafkaUser`.

**`strict decoding error: unknown field "spec.kafka.resources"`.** Strimzi 1.x moved `resources`,
`replicas`, `storage` and `jvmOptions` to `KafkaNodePool`. Most tutorials online predate that.

**`Unsupported Kafka.spec.kafka.version`.** Strimzi 1.1.0 ships 4.2.0, 4.2.1 and 4.3.0 only.

**CRs apply but nothing happens.** The operator is watching one namespace. If the CR is not in
`kafka`, no controller will ever look at it. `kubectl logs -n strimzi-system deploy/strimzi-cluster-operator`
will not even mention it.

**A `KafkaTopic` is `Ready=False` after lowering `partitions`.** Partition counts only go up. The
operator will not delete data to satisfy the spec - raise the number back, or delete and recreate
the topic deliberately (which is a migration; see topic map change rule 2).

**`TOPIC_AUTHORIZATION_FAILED` from a service that should have access.** Check the *principal*
first: `User:<KafkaUser metadata.name>`. Then check the pattern type - `prefix` in the CR is
`PREFIXED` in `kafka-acls --list` output, and a literal ACL where a prefixed one was meant fails
on the first per-instance `group.id`.

**Pods `Pending`.** On this 3-node kind cluster the usual cause is memory: three brokers requesting
1 GiB each plus everything else, inside an ~8 GiB Docker VM. `kubectl describe pod` says
`Insufficient memory`.

---

## Compromises / what didn't fully match the ideal

- **Combined controller+broker roles, not separate.** Justified above on memory grounds, but it
  is a compromise: this cluster cannot demonstrate a controller-only failure distinct from a
  broker failure, and the M19 drill is correspondingly less interesting than it would be with six
  nodes.
- **TLS is still not enabled on the client listeners.** Both are `SASL_PLAINTEXT` - authenticated
  and authorized, but not encrypted, exactly as in compose. Strimzi makes this a one-line change
  (`tls: true` on the listener, plus a truststore mount for clients from
  `psp-cluster-ca-cert`), and it is deliberately left for later so that phase 2's client config
  changes one thing at a time. The inter-broker and controller listeners *are* mTLS, which is
  strictly better than compose.
- **Host access is verified at the TCP layer only.** The three `port-forward` targets accept
  connections and the brokers advertise `localhost:29092-29094` as designed, but no host-side
  Kafka client actually produced through them - `kcat` is not installed on this machine and the
  acceptance bar is satisfied from inside the cluster. Worth a real `kcat -L` the first time
  anyone needs it.
- **The retired `.v1` webhook chain is created empty.** The topic map still lists those five
  topics and change rule 4 makes it the source for these CRs, so they exist here - but the M8
  poison-pill records `.v1.dlq` holds live in the compose cluster's volume and do not replicate
  across. The topics are structurally faithful and historically empty.
- **`_schemas` has no CR** (illegal `metadata.name`, and absent from the topic map). Schema
  Registry creates it, which is what its `Create` grant is for - but it means one topic in the
  eventual system is not GitOps-managed.
- **Kafka 4.3.0 vs compose's 3.7.** Forced by the operator's supported-version list, not chosen.
  Nothing in the topic map or ACL matrix needed changing, but the two clusters are not running
  the same Kafka and a behavioural difference between them should be suspected before it is
  explained away.
- **`reconciliationIntervalMs: 60000`** on both entity operators, half Strimzi's default, purely
  so the drift demonstrations above finish quickly. It is a slightly higher API-server load than
  necessary for a real cluster of this size.
- **The 5 GiB PVCs are not enforced.** kind's `local-path` provisioner is hostPath-backed, so a
  runaway topic fills the node's disk regardless of the claim size.

---

## M18 phase 2: the Spring services, and getting them Ready

Phase 1 (above) is Kafka only. Phase 2 adds Helm charts for the seven Spring services plus
in-cluster Postgres, MongoDB, Redis, Schema Registry and a Strimzi `KafkaConnect` build
(`infra/k8s/charts/psp-platform`), deployed with `infra/k8s/scripts/build-images.sh` +
`helm upgrade --install psp infra/k8s/charts/psp-platform -n kafka`.

The chart work was committed mid-flight (WIP) after the third disk-exhaustion incident of this
project took Docker Desktop down mid-build. What follows is what it took to get from that state -
every service `CrashLoopBackOff`, Schema Registry `CrashLoopBackOff`, duplicate ReplicaSets from
the interrupted rollout - to every pod `Ready` with a real payment proven to move money.

### The readiness-probe design: what a shallow probe misses

Spring Boot's default readiness contributor, `readinessState`, answers one question: "will this
JVM accept an HTTP request." M15 (see `docs/PLAN.md`) proved that question is not the same as "is
this service doing its job" - a Spring service can hold `readinessState: UP` while every one of
its `@KafkaListener` containers has stopped. On Docker Compose that produces a misleading
dashboard. On Kubernetes it is worse: kube-proxy keeps a pod like that in its Service's endpoint
list, and during a rolling update the pod that consumes nothing can outlive the one that doesn't.

`libs/common-health` closes that gap with two conditional `HealthIndicator` beans
(`KafkaHealthAutoConfiguration`, wired through the standard
`org.springframework.boot.autoconfigure.AutoConfiguration.imports` mechanism, same pattern
`libs/common-web` already uses):

- **`kafkaListeners`** (`KafkaListenerContainersHealthIndicator`) walks every container in the
  injected `KafkaListenerEndpointRegistry` and reports `DOWN` if any container that is supposed to
  be running (`autoStartup=true`) is not, listing every container id and its state in the health
  detail - so `kubectl exec ... curl localhost:PORT/actuator/health` names the dead listener
  instead of just saying `DOWN`. A container that was deliberately created stopped (the
  DLQ-replay listener, `autoStartup=false`) is reported but never fails the check.
- **`kafkaStreams`** (`KafkaStreamsHealthIndicator`, analytics only) reports the
  `StreamsBuilderFactoryBean`'s own `KafkaStreams.State`. `REBALANCING` and state-restore are
  treated as not-ready-but-not-an-error - during a rolling update that is exactly the window the
  pod should be out of the Service's endpoints - while `ERROR`/`NOT_RUNNING` means the topology
  died with the HTTP port still open, the exact M15 failure mode.

Both beans are **off by default** (`psp.health.kafka[.streams].enabled`, both `false`) and switched
on per service in that service's own `values.yaml`, in the same file that names the contributor in
`readinessInclude` - so the two cannot drift apart the way they briefly did here (see below).
`management.endpoint.health.validate-group-membership` (Spring Boot's default, left on) turns any
future drift into a startup failure instead of a silent no-op, which is a deliberate trade: a
service that boots with a health group naming a contributor that doesn't exist should never reach
"Ready" pretending otherwise.

**Which services get which contributor**, and why the set is not uniform:

| Service | `readinessInclude` | Why |
|---|---|---|
| `ledger`, `webhook-notifier`, `psp-connector`, `realtime-gateway` | `readinessState,kafkaListeners` | each has real `@KafkaListener` containers a shallow probe can't see stop |
| `analytics` | `readinessState,kafkaListeners,kafkaStreams` | has both a batch `@KafkaListener` (M13) and a Streams topology (M10) |
| `api-gateway` | `readinessState` only | ADR-0004: commands enter over HTTP, this service has no Kafka client at all - naming `kafkaListeners` here would itself fail startup |
| `payment-api` | `readinessState` only | has a Kafka **consumer** (M12's `ReplyingKafkaConfig`, the provider-status-reply request/response wiring) but its `KafkaMessageListenerContainer` is hand-built and handed straight to a `ReplyingKafkaTemplate` - it is never a `@KafkaListener` method, so it is never registered in the `KafkaListenerEndpointRegistry` the indicator inspects. Naming `kafkaListeners` for payment-api would not fail; it would silently report `UP` over zero containers forever - the same "green but meaningless" failure mode M15 exists to eliminate, self-inflicted this time. |

### What was actually broken, and the fix

The symptom on every Spring pod was identical:

```
APPLICATION FAILED TO START
Description: Included health contributor 'kafkaListeners' in group 'readiness' does not exist
```

The obvious read is "the indicator was never implemented." It was: `libs/common-health` already
had `KafkaListenerContainersHealthIndicator`, `KafkaStreamsHealthIndicator` and
`KafkaHealthAutoConfiguration` fully written, unit-testable, and wired into every relevant
service's `pom.xml`, all committed in the same WIP commit as the charts. The actual defect was
narrower and easy to miss: **every running pod's image was built from an older commit than the one
that added `libs/common-health`.** The disk-exhaustion incident interrupted the image build before
the module existed on the classpath the pods were running; `readinessInclude` in the chart named a
bean that the *deployed jar* genuinely did not contain, even though the source tree did. Rebuilding
the images from current `HEAD` and reloading them into `kind` was the actual fix for every service
except one:

- **`payment-api`** legitimately had no listener containers (see the table above), so its
  `values.yaml` was changed to drop `kafkaListeners` from `readinessInclude` and leave
  `psp.health.kafka.enabled` unset, rather than switching on a bean that would monitor nothing.
- **`analytics`**'s image needed a second, unrelated fix before it would start at all (see next
  section) - once fixed, its `kafkaStreams` contributor (already correctly named in `values.yaml`)
  started reporting real state.
- Every other chart's `readinessInclude` was checked against a real `@KafkaListener` in that
  service's source (`grep -rn "@KafkaListener" services/<svc>/src/main/java`) and matched.

### The second defect: analytics couldn't start on Alpine

Rebuilding surfaced a real, previously-undiscovered bug, independent of the health-indicator issue:
analytics crashed on every start with

```
Exception in thread "...-GlobalStreamThread" java.lang.UnsatisfiedLinkError:
Error loading shared library libstdc++.so.6: No such file or directory
(needed by /tmp/librocksdbjni....so)
```

`services/analytics/Dockerfile` used the same `eclipse-temurin:21-jre-alpine` base as every other
service. Every other service is fine there; analytics is the only one with a Kafka Streams
topology (M10), and Kafka Streams' embedded RocksDB state stores load a native library
(`org.rocksdb:rocksdbjni`) that ships in exactly one build: linked against **glibc**. Alpine's C
library is musl - not ABI-incompatible glibc, but no glibc-compatible `libstdc++.so.6` at all, so
`apk add libstdc++` cannot fix it (that installs musl's own build of the library, under the same
name, which still fails to satisfy a glibc-linked `.so`). The fix was switching analytics' base
image to `eclipse-temurin:21-jre-jammy` (Ubuntu, glibc) and installing `libstdc++6` explicitly
rather than relying on it arriving transitively. The rest of the Dockerfile - numeric non-root
user, `MaxRAMPercentage`, `EXPOSE` - is unchanged; only the user-creation commands changed from
BusyBox's `addgroup -S`/`adduser -S` to Debian's `groupadd`/`useradd`, since Alpine and Ubuntu
disagree on that tooling too.

### Schema Registry: crashing on a Kubernetes-only mechanism

Schema Registry's log stopped after one line: `PORT is deprecated. Please use
SCHEMA_REGISTRY_LISTENERS instead.`, then the container exited. The chart already set
`SCHEMA_REGISTRY_LISTENERS` explicitly - the deprecated variable was never set in the chart at
all. The actual source was Kubernetes itself: for every Service in a namespace, Kubernetes injects
Docker-link-style environment variables into every pod, so the Service named `schema-registry`
became `SCHEMA_REGISTRY_PORT=tcp://10.96.x.x:8081` inside the schema-registry pod's own container.
Confluent's images bootstrap their config from exactly that variable-name prefix, so the Service's
own name silently poisoned its container's configuration - a pod is not required to consume its
own Service's service-links, but gets them by default. The template's fix -
`enableServiceLinks: false` on the pod spec - was already written and committed in the source tree
(with a comment explaining exactly this), but had never been applied: the chart was edited *after*
the last successful `helm upgrade` before the disk incident, so the running Deployment predated the
fix. `kubectl get deployment schema-registry -o yaml` confirmed the live object had no
`enableServiceLinks` field at all. Re-running `helm upgrade` (which this phase 2 pass did anyway,
to pick up `libs/common-health`) applied it; no further code change was needed here, only
deployment.

### Duplicate ReplicaSets

The interrupted rollout, plus the two redeploys this pass required (the `libs/common-health` image
rebuild, then the analytics base-image fix), left every Deployment with several `0/0/0` historical
ReplicaSets. `kubectl get rs -n kafka` was cleaned with:

```bash
kubectl get rs -n kafka --no-headers | awk '$2==0 && $3==0 && $4==0 {print $1}' \
  | xargs -r kubectl delete rs -n kafka
```

leaving exactly one ReplicaSet per Deployment (verified below).

### Prove it - run against this exact cluster

**a. Everything Ready:**

```
$ kubectl get pods -n kafka
NAME                                   READY   STATUS    RESTARTS   AGE
analytics-6bc56f86db-mj8tb             1/1     Running   0          5m48s
api-gateway-9fd47795-42dlf             1/1     Running   0          5m48s
ledger-84568f58cb-7q8lq                1/1     Running   0          5m48s
mongodb-57f58745cf-hclwq               1/1     Running   0          11m
payment-api-6786577f97-hzmxv           1/1     Running   0          5m48s
postgres-54499c7bb8-7qld2              1/1     Running   0          11m
psp-combined-0                         1/1     Running   0          41m
psp-combined-1                         1/1     Running   0          41m
psp-combined-2                         1/1     Running   0          39m
psp-connect-connect-0                  1/1     Running   0          5m44s
psp-connector-5789665dff-9cqrz         1/1     Running   0          5m48s
psp-entity-operator-786db8c79f-dgb47   2/2     Running   0          32m
realtime-gateway-86f49fcbff-ndffv      1/1     Running   0          5m47s
redis-7bfc9b47f4-qlxrv                 1/1     Running   0          11m
schema-registry-b4dd99cdb-nmfc8        1/1     Running   0          11m
webhook-notifier-5d9f888564-gnzsg      1/1     Running   0          5m47s
```

16/16 pods, one ReplicaSet each, `helm list -n kafka` shows revision 5 `deployed`.

**b. The `kafkaListeners` contributor, actually watching something real** (`ledger`, four
`@KafkaListener` containers: `RefundRequestedListener`, `PaymentStatusChangedListener`,
`RefundFailedListener`, `RefundCompletedListener`):

```
$ kubectl exec -n kafka deploy/ledger -- wget -qO- http://localhost:8087/actuator/health/readiness
{"status":"UP","components":{
  "kafkaListeners":{"status":"UP","details":{
    "org.springframework.kafka.KafkaListenerEndpointContainer#0":"RUNNING",
    "org.springframework.kafka.KafkaListenerEndpointContainer#1":"RUNNING",
    "org.springframework.kafka.KafkaListenerEndpointContainer#2":"RUNNING",
    "org.springframework.kafka.KafkaListenerEndpointContainer#3":"RUNNING",
    "containers":4}},
  "readinessState":{"status":"UP"}}}
```

`analytics`, both contributors, `kafkaStreams` reporting the topology's real `KafkaStreams.State`:

```
$ kubectl exec -n kafka deploy/analytics -- wget -qO- http://localhost:8089/actuator/health
{"status":"UP", ... ,
  "kafkaListeners":{"status":"UP","details":{
    "org.springframework.kafka.KafkaListenerEndpointContainer#0":"RUNNING","containers":1}},
  "kafkaStreams":{"status":"UP","details":{"state":"RUNNING","applicationId":"analytics-streams.v1"}},
  ...}
```

`payment-api`, confirming the contributor is genuinely absent rather than present-and-green over
nothing:

```
$ kubectl exec -n kafka deploy/payment-api -- wget -qO- http://localhost:8085/actuator/health
{"status":"UP","components":{"db":{...},"diskSpace":{...},"livenessState":{...},
  "ping":{...},"readinessState":{"status":"UP"},"refreshScope":{...},"ssl":{...}}}
```

No `kafkaListeners` key at all - `psp.health.kafka.enabled` was left unset for this service, so
`KafkaHealthAutoConfiguration`'s `@ConditionalOnProperty` never creates the bean.

**c. A payment, posted through the gateway, reaching a ledger balance inside the cluster:**

```bash
kubectl port-forward -n kafka svc/api-gateway 8000:8000 &
kubectl port-forward -n kafka svc/postgres 5432:5432 &

for i in 1 2 3 4 5; do
  curl -sS -X POST http://localhost:8000/api/payments \
    -H 'Content-Type: application/json' \
    -d '{"merchantId":"merchant-e2e-proof","amount":10.00,"currency":"EUR"}'
done
```

Every POST returned `201 Created` (routed by api-gateway's `Path=/api/payments/**` rate-limited,
circuit-broken route to `payment-api.kafka.svc.cluster.local:8085`). Each payment then flowed,
unattended, through the full chain proven separately in M6-M10: payment-api writes an
`outbox_event` row in the same Postgres transaction as the payment -> Debezium
(`payment-outbox-connector`, `KafkaConnector` `Ready=True`) reads the WAL and produces
`payments.payment-requested.v1` -> `psp-connector` consumes it, simulates a provider call
(100 ms-5 s), and produces `payments.payment-status-changed.v1` with a random `APPROVED`/`DECLINED`
outcome -> `ledger` consumes that (transactional producer, `ledger-tx-0-0`) and, only for
`APPROVED`, credits `merchant_balances` and inserts a `ledger_entries` row in one transaction. One
of the six payments run during this session was randomly `DECLINED` - the ledger correctly logged
`Ignoring status=DECLINED ... - moves no money` and left the balance untouched, which is itself
part of the proof: the pipeline distinguishes the two outcomes rather than crediting on delivery
alone.

```
$ kubectl exec -n kafka deploy/postgres -- env PGPASSWORD="$LEDGER_PW" \
    psql -U ledger -d ledger -c \
    "SELECT merchant_id, currency, balance, entry_count, updated_at
       FROM merchant_balances WHERE merchant_id = 'merchant-e2e-proof';"

    merchant_id     | currency | balance | entry_count |          updated_at
---------------------+----------+---------+-------------+------------------------------
 merchant-e2e-proof  | EUR      | 50.0000 |           5 | 2026-08-12 23:07:37.00706+00
```

**50.0000 EUR, entry_count 5** - exactly 5 x 10.00 EUR `CREDIT`, matching the five `APPROVED`
outcomes among six payments POSTed (the sixth, a 42.50 EUR payment sent first, was the one
`DECLINED` outcome and correctly contributed nothing). Elapsed time from POST to a settled balance
was on the order of one second per payment - fast enough that "wait 5-10s after POSTing" is a
generous margin, not a requirement.

This is the same discipline the rest of this README applies to Kafka itself: pod readiness, and
even a `200`/`201` from the gateway, is not evidence that data moved. This project has now measured
three separate cases of a component reporting healthy while consuming, producing, or crediting
nothing (M15's shallow-probe finding twice over - once for `@KafkaListener` containers, once for
Kafka Streams - and Schema Registry `CrashLoopBackOff`ing behind a readiness probe that never got
the chance to report anything at all). The balance query above is the only claim in this section
that is not itself a health check.

---

## M18 phase 3: KEDA autoscaling psp-connector on consumer lag

Phase 1 put Kafka on Kubernetes; phase 2 put the application next to it. Phase 3 is the module's
second showpiece: **the number of psp-connector pods stops being a number in a values file and
becomes a function of how far behind the consumer group is.**

The Kafka concept on display is not "autoscaling". It is the one M4 measured directly and that
every lag-based autoscaler eventually collides with: **within a consumer group, a partition is
assigned to exactly one consumer, so a group can never have more useful members than the topic has
partitions.** Everything below is downstream of that sentence.

```
payments.payment-requested.v1  ─┐
  (12 partitions)               │  ListOffsets  ──► end offsets
                                │                              ├─► lag ─► KEDA ─► HPA ─► Deployment
group psp-connector.v1         ─┘  OffsetFetch  ──► committed          (metric)   (replicas)
```

### What phase 3 adds to the tree

```
infra/k8s/
├── keda/
│   ├── namespace.yaml                     keda - the autoscaler's own namespace
│   └── values.yaml                        Helm values: watch ONLY `kafka`, bounded resources
├── kafka/users/
│   └── 24-keda-scaler.yaml                the 12th KafkaUser: 2 Describe bindings, nothing else
├── charts/psp-platform/charts/psp-connector/
│   ├── templates/autoscaling.yaml         Secret + TriggerAuthentication + ScaledObject
│   └── values.yaml                        the `autoscaling:` block - every number, justified
├── load/
│   ├── payments.js                        the k6 script (a real .js file, linted, in git)
│   └── k6-job.yaml                        it, as an in-cluster Job
└── scripts/
    ├── install-keda.sh                    pinned chart + the scaler's KafkaUser
    ├── load-test.sh                        builds the ConfigMap, runs the Job
    └── watch-scaling.sh                    LAG / ACTIVE / TARGET / REPLICAS, one row per 10s
```

`deploy-apps.sh` installs KEDA itself if the CRDs are missing, because the chart now renders a
`ScaledObject` and Helm fails the whole release on an unknown kind. To deploy without it:
`--set psp-connector.autoscaling.enabled=false`.

### The operator: KEDA 2.20.2, pinned

| | |
|---|---|
| KEDA | **2.20.2** (chart 2.20.2, appVersion 2.20.2) |
| Installed as | Helm chart `keda-2.20.2.tgz`, downloaded from `kedacore.github.io/charts` |
| Operator namespace | `keda` |
| Watches | `kafka` only (`watchNamespace: kafka`) - **not** cluster-wide |
| Components | `keda-operator`, `keda-operator-metrics-apiserver`, `keda-admission-webhooks` |
| CRDs | `scaledobjects`, `scaledjobs`, `triggerauthentications`, `clustertriggerauthentications`, `cloudeventsources`, `clustercloudeventsources` (all `keda.sh`) |

```bash
./infra/k8s/scripts/install-keda.sh          # namespace + pinned chart + the scaler's KafkaUser
```

or by hand:

```bash
KEDA_VERSION=2.20.2
kubectl apply -f infra/k8s/keda/namespace.yaml
curl -fsSL -o /tmp/keda.tgz https://kedacore.github.io/charts/keda-${KEDA_VERSION}.tgz
helm upgrade --install keda /tmp/keda.tgz \
  --namespace keda --values infra/k8s/keda/values.yaml --wait
kubectl apply -f infra/k8s/kafka/users/24-keda-scaler.yaml
```

**Pinned, and namespace-scoped, for the same two reasons as Strimzi.** `helm repo add kedacore`
plus `helm install keda kedacore/keda` installs whatever is newest that day - and an autoscaler
upgrading itself changes how many pods run without a diff anywhere. `watchNamespace: kafka` is the
same restriction `watchNamespaces: [kafka]` puts on the cluster operator; note the **singular**
name, because Helm silently ignores an unknown value and the symptom of getting it wrong is an
operator that watches the whole cluster while the file says otherwise.

Three components, not one, and it is worth knowing which does what before debugging anything:

| Pod | Job | What its absence looks like |
|---|---|---|
| `keda-operator` | reconciles `ScaledObject`s, creates/owns the HPA, runs the scalers | `ScaledObject` never gets a `Ready` condition; no HPA appears |
| `keda-operator-metrics-apiserver` | serves `external.metrics.k8s.io` so the **HPA** can read the lag | HPA shows `TARGETS: <unknown>/25` |
| `keda-admission-webhooks` | validates a `ScaledObject` at apply time | bad specs are accepted and fail later, in logs |

### The M14 constraint: KEDA is a Kafka client, so KEDA needs a principal

**This is the part that is easy to get wrong and hard to notice.** KEDA's Kafka trigger is not a
Kubernetes-native metric. It is an ordinary Kafka admin client that, on demand, issues
`ListOffsets` against the topic and `OffsetFetch`/`DescribeGroups` against the consumer group, and
subtracts. This cluster has been deny-by-default since phase 1
(`authorization.type: simple`, no `allow.everyone.if.no.acl.found`), so that client needs a SCRAM
credential and ACLs like every other client in the system.

`infra/k8s/kafka/users/24-keda-scaler.yaml` - the **12th** `KafkaUser`, and the first with no
compose ancestor, because compose had no autoscaler:

```yaml
metadata:
  name: keda-scaler
spec:
  authentication: { type: scram-sha-512 }
  authorization:
    type: simple
    acls:
      - resource: { type: topic, name: payments.payment-requested.v1, patternType: literal }
        operations: [Describe]
      - resource: { type: group, name: psp-connector.v1, patternType: literal }
        operations: [Describe]
```

**Two bindings. Nothing was loosened to make this work, and the superuser was not reused.**

- **Not `admin`.** One line, and the autoscaler becomes the most privileged client in the system -
  able to delete every topic it is only supposed to measure. An observer gets an observer's
  credential.
- **Not `psp-connector`'s own credential.** Sharing the workload's identity with the thing that
  scales the workload makes a lag reading and a consumed payment indistinguishable, and hands the
  scaler `Read`/`Write` on nine topics it has no business touching.
- **`Describe`, never `Read`.** `Read` on a topic is permission to fetch message bodies. Lag is
  arithmetic on offsets, and offsets are metadata: Kafka has required `Describe` (not `Read`) for
  `ListOffsets` since 2.x, and `OffsetFetch` has always been `Describe` on the group. So this
  principal can say exactly how far behind psp-connector is and cannot read one payment. Same shape
  as `kafka-exporter`, narrowed from `*` to the one topic and one group actually named.

ACL totals move with it: **11 principals / 119 bindings → 12 principals / 121 bindings.**

**Why this is worth being careful about: an unauthorized Kafka scaler does not fail loudly.** It
reports a lag it cannot see, which is indistinguishable from "there is no lag", so the
`ScaledObject` sits `READY=True`, `ACTIVE=False`, and nothing ever scales - looking, in every
dashboard, exactly like a system that is comfortably keeping up. Any claim that this works must
therefore be backed by a lag reading taken **independently of KEDA**, which is why
`scripts/watch-scaling.sh` reads `kafka-consumer-groups.sh --describe` itself and prints it in the
same row as KEDA's number.

### Getting the credential to KEDA

Strimzi writes `Secret/keda-scaler` with `password` and `sasl.jaas.config`. KEDA builds a Sarama
client config field by field and cannot consume a JAAS line, so this is the **one** place in the
repo that reads the raw `password` key - and it is read by the KEDA operator, never mounted into an
application pod. Three objects, rendered by
`charts/psp-connector/templates/autoscaling.yaml`:

| Object | Holds | Why |
|---|---|---|
| `Secret/psp-connector-kafka-scaler-auth` | `username`, `sasl`, `tls` | not secrets - but `TriggerAuthentication` has `secretTargetRef` and no `configMapTargetRef`, so a plain string has nowhere else to live |
| `TriggerAuthentication/psp-connector-kafka-scaler` | four `secretTargetRef` entries | joins the two Secrets above; namespaced, so it must live in `kafka` next to the workload |
| `ScaledObject/psp-connector` | the policy | below |

### The ScaledObject, parameter by parameter

```yaml
minReplicaCount: 1
maxReplicaCount: 6          # topic has 12 partitions - the hard ceiling
pollingInterval: 15
cooldownPeriod: 60
advanced:
  restoreToOriginalReplicaCount: true
  horizontalPodAutoscalerConfig:
    behavior:
      scaleUp:   { stabilizationWindowSeconds: 0,  policies: [{type: Pods, value: 2, periodSeconds: 30}] }
      scaleDown: { stabilizationWindowSeconds: 60, policies: [{type: Pods, value: 1, periodSeconds: 30}] }
triggers:
  - type: kafka
    metadata:
      topic: payments.payment-requested.v1
      consumerGroup: psp-connector.v1
      lagThreshold: "25"
      activationLagThreshold: "5"
      offsetResetPolicy: latest
      allowIdleConsumers: "false"
      scaleToZeroOnInvalidOffset: "false"
```

#### `maxReplicaCount: 6` - and the partition ceiling above it

**This is the single most important sizing rule here, and it is a Kafka rule, not a Kubernetes
one.** `payments.payment-requested.v1` has **12 partitions** (`docs/diagrams/topic-map.md`). Within
one consumer group each partition is assigned to exactly one consumer, so consumer **13** in group
`psp-connector.v1` is assigned nothing: it joins, forces a group rebalance that stops all twelve
working consumers, and then polls an empty assignment forever while still holding a JVM, a Postgres
connection pool and a memory limit. **M4 measured exactly that**, and it is the reason lag-based
scaling has a ceiling at all - the metric itself has no opinion, lag keeps rising and the HPA would
keep asking for pods that cannot help.

So `maxReplicaCount ≤ partitions` is a hard correctness bound, and the real ceiling here is 12.

**6, not 12, for two local reasons:**

1. **Memory.** 12 × 768Mi of limits is 9.2 GiB of psp-connector alone, in a Docker VM that also
   runs three Kafka brokers, Connect, Postgres, MongoDB, Redis, Schema Registry and seven other
   JVMs. The scheduler would start leaving pods `Pending` - and a `Pending` pod is an autoscaler
   that *looks* like it is working and is not.
2. **12 / 6 = 2 exactly.** Every consumer gets the same two partitions, so the drill's throughput
   arithmetic is checkable rather than approximate. (5 replicas would give four consumers two
   partitions and one consumer... two as well, with two partitions unassigned to anybody until the
   assignor spreads them 3/2/2/2/3 - the ratio experiment from M4c all over again.) The divisors of
   12 worth choosing between are 4, 6 and 12; 6 is the largest that fits.

Two defences, deliberately belt-and-braces:

- `maxReplicaCount: 6` is the ceiling **a human reads in the manifest**.
- `allowIdleConsumers: "false"` is the ceiling **the scaler enforces against the live topic**: KEDA
  caps its own metric at the partition count, so even if someone raises `maxReplicaCount` to 50
  without checking the topic map, the HPA is never told to go past 12.

Raising the ceiling toward 12 is a one-line change on a bigger machine, which is why
`autoscaling.topicPartitions: 12` is restated in `values.yaml` right above it.

#### `lagThreshold: 25` - derived, not picked

The HPA arithmetic is `desiredReplicas = ceil(totalLag / lagThreshold)`, clamped to
`[min, max]`. So `lagThreshold` is **target lag per replica**, and it should come from measured
throughput:

- psp-connector's `@KafkaListener` sets no `concurrency`, so it is **one consumer thread per pod**,
  processing records serially.
- The simulated provider sleeps a uniform **100 ms - 5000 ms** per payment
  (`services/psp-connector/src/main/resources/application.yml`), i.e. ~2.55 s mean.
- **≈ 0.39 payments/second/pod.**

25 messages is therefore ≈ **64 seconds of work for one pod**. The SLO that number encodes is *"a
replica should be able to clear its share of the backlog in about a minute"*, and that sentence -
not the number - is the thing to change when the requirement changes.

Consequences worth writing down rather than discovering:

| | |
|---|---|
| replicas hit max at | total lag > 5 × 25 = **126** |
| standing backlog tolerated at 6 replicas | 6 × 25 = **150 messages** (~64 s at the full 2.34 msg/s drain rate) |
| lag → replicas | 25→1, 50→2, 100→4, 126→6, 1000→6 (capped) |

#### `minReplicaCount: 1` - not 0

KEDA can scale to zero, and this deliberately does not:

- **Latency.** With no group members, the first payment after a quiet period waits for KEDA's poll,
  a ~45 s Spring Boot start (Flyway, Hibernate, a Kafka admin round-trip) and a rebalance. Phase 2
  measured POST-to-settled-balance at ~1 second. For a payment system, trading that for an idle pod
  is not a trade.
- **Observability of the signal itself.** Lag is only meaningful relative to a committed offset. A
  group with zero members still has committed offsets, so this would work - but it makes the whole
  scaling loop depend on offset retention, for a saving of one 512Mi pod.

#### `pollingInterval: 15` and `cooldownPeriod: 60` - and KEDA telling us they do nothing

Applying this ScaledObject produces two warnings, and they are **correct**:

```
Warning: PollingInterval is configured but is not relevant. PollingInterval is only relevant
         when minReplicaCount = 0 or idleReplicaCount = 0 or useCachedMetrics is enabled
Warning: CooldownPeriod is configured but is not relevant. CooldownPeriod is only relevant
         when minReplicaCount = 0 or idleReplicaCount = 0
```

This is the most commonly misread part of KEDA and it is worth stating plainly: **with
`minReplicaCount ≥ 1`, KEDA is not in the scaling loop at all.** It publishes the lag as an
external metric and the *HPA* does the scaling, on the HPA controller's own ~15 s cadence, pulling
the metric through `keda-operator-metrics-apiserver` on demand. KEDA's own poll loop and its
cooldown exist for the scale-to-zero path, which `minReplicaCount: 1` never takes.

Both fields are kept anyway, and this paragraph is why: flipping `minReplicaCount` to 0 should be a
one-line change with an already-considered cadence, not a change that silently starts using two
defaults nobody chose. The `useCachedMetrics: true` alternative - KEDA polls Kafka every
`pollingInterval` and serves the HPA a cached value - trades freshness for fewer admin requests and
is the right answer at hundreds of ScaledObjects, not at one.

#### Scale-**in**: `behavior.scaleDown`, which is what actually does it

A demo that only scales out is half a demo, and the field that makes the other half work is **not**
`cooldownPeriod`. It is the HPA's own stabilization window, whose Kubernetes default is **300 s**:
the HPA takes the *highest* recommendation from the last five minutes, so a backlog that cleared a
minute ago still pins the replica count. Left at the default, a drill appears to scale out and then
never come back, and the natural (wrong) conclusion is that scale-in is broken.

```yaml
scaleDown: { stabilizationWindowSeconds: 60, policies: [{ type: Pods, value: 1, periodSeconds: 30 }] }
```

60 s keeps the drill inside a coffee break while still being long enough that one quiet polling
interval does not tear down a consumer mid-batch. One pod at a time, deliberately slower than
scale-out, for a Kafka reason: **every removed consumer costs a group rebalance**, and it hands its
partitions to peers that are already busy. A flapping autoscaler on a consumer group converts a
mild traffic wobble into a rebalance storm. A production value would sit closer to the 300 s
default.

Scale-**out** is `stabilizationWindowSeconds: 0` (react immediately - the backlog is already there)
with **2 pods per 30 s**, not the Kubernetes default of *double, or +4, every 15 s*. A
psp-connector pod is a JVM that runs Flyway and a Kafka admin round-trip before it consumes
anything: ~40-60 s on kind. Adding pods faster than they become useful means the metric still reads
"very behind" while four pods are starting, so the HPA asks for more - the classic overshoot - and
each one of them costs another rebalance that stops the consumers already working.

#### The rest

| Field | Value | Why |
|---|---|---|
| `activationLagThreshold` | `5` | the ACTIVE/inactive boundary, a different question from "how many pods". A live pipeline is never at exactly zero lag - a record in flight is lag - so `0` means permanently `ACTIVE` and the column carries no information |
| `offsetResetPolicy` | `latest` | what lag means for a partition the group never committed. `earliest` would count 7 days of retained payments as backlog and slam to max. Deliberately *disagrees* with psp-connector's own `auto-offset-reset: earliest`, which is safe because the group has committed offsets |
| `scaleToZeroOnInvalidOffset` | `"false"` | a partition whose committed offset went invalid keeps one consumer instead of silently going unprocessed - the same class of bug M15 exists to catch |
| `restoreToOriginalReplicaCount` | `true` | `kubectl delete scaledobject` at 6 replicas otherwise leaves 6 replicas running forever |

#### One Helm detail that is not cosmetic: `replicas` disappears

When `autoscaling.enabled` is true, the shared Deployment template renders **no `replicas` field
at all** (`templates/_spring-service.tpl`). A replica count in a Helm-managed manifest and an HPA
are two controllers writing the same field: `helm upgrade` patches it back to the chart's value,
the HPA notices the workload is under-provisioned and climbs again, and every deploy during a
traffic peak becomes a self-inflicted capacity drop. Omitting the field leaves Helm's three-way
merge with nothing to say about it.

Expected once, and only once: the **first** upgrade after enabling autoscaling removes a field that
was previously present, so the API server re-defaults it to 1 and the HPA scales back up on its
next evaluation.

### The load test

`infra/k8s/load/payments.js` - k6, driving `POST /api/payments` through api-gateway. It is a **lag
generator** first and a latency benchmark second: the interesting output is `kubectl get hpa`, not
the p95.

```bash
./infra/k8s/scripts/load-test.sh                          # in-cluster Job, 1 pod, 5 req/s, 3m
./infra/k8s/scripts/load-test.sh --pods 3 --duration 2m   # 3 source IPs => ~15 req/s
./infra/k8s/scripts/load-test.sh --duration 45s --watch   # short, with the scaling table on screen
./infra/k8s/scripts/load-test.sh --host                   # k6 on the laptop, via port-forward
```

**In-cluster as a `Job` is the default, and the reason is the gateway's rate limiter, not
convenience.** api-gateway applies a `RequestRateLimiter` as a *default filter* to every route
(M16): `replenishRate 5`, `burstCapacity 10`, keyed by the caller's IP
(`config.RateLimiterConfig`). A single-source flood therefore does not produce more payments, it
produces 429s - the gateway doing exactly what it was built to do. So:

- the default arrival rate is **5/s**, right at the replenish rate, so essentially every request is
  a real payment and **the limiter is not disabled or bypassed to make the demo look better**;
- more load is bought with **more source IPs**, not a higher rate: each pod of the Job has its own
  pod IP and therefore its own token bucket, so `--pods N` gives N × 5/s.

5/s is already ~13× what one psp-connector replica can drain (~0.39/s), so lag builds at ~4.6
messages/second with one replica. It does not need to be faster.

Other decisions in that script worth knowing:

- **`constant-arrival-rate`, not `constant-VUs`.** An open model: k6 starts a request every 200 ms
  regardless of how long the previous one took. A closed model would throttle itself the moment the
  gateway slowed down - precisely when a lag test needs it not to.
- **No `jslib.k6.io` imports.** The usual `randomIntBetween` import is a network fetch performed by
  the k6 pod at startup; a load test that cannot start because a CDN is unreachable is a load test
  that will fail on the day it is most needed.
- **`gateway_server_errors_5xx: ['count==0']` is the only threshold.** There is deliberately no
  threshold on `http_req_duration`: this test is *supposed* to make the system slow, and failing on
  latency would be failing on success. 429s are counted separately from 5xx because they mean
  opposite things - one is a feature, the other invalidates the drill.
- **`kubectl port-forward` is supported (`--host`) but not the default:** it is a single TCP tunnel
  through the API server, so at any interesting rate it becomes the bottleneck and the thing being
  measured.

**A deterministic variant.** The provider's 100 ms - 5 s random sleep makes per-pod throughput a
mean rather than a number. To pin it:

```bash
helm upgrade psp infra/k8s/charts/psp-platform -n kafka --reuse-values \
  --set psp-connector.providerForcedLatencyMs=2000     # => exactly 0.5 msg/s/pod
```

The default stays `0` (the service's own random range), because that is the behaviour phase 2
demonstrated end to end.

### Watching it

```bash
./infra/k8s/scripts/watch-scaling.sh          # one row every 10s: LAG ACTIVE TARGET REPLICAS PODS
```

The `LAG` column is read with `kafka-consumer-groups.sh --describe`, as `admin`, **independently of
KEDA** - so "KEDA says the lag is X" is never the only evidence that the lag is X. That is the
whole point of the script; see the unauthorized-scaler failure mode above.

The raw commands, if you would rather:

```bash
kubectl get scaledobject psp-connector -n kafka                 # READY / ACTIVE
kubectl get hpa keda-hpa-psp-connector -n kafka -w              # TARGETS = <avg lag>/<lagThreshold>
kubectl get pods -n kafka -l app.kubernetes.io/name=psp-connector -w
kubectl logs -n kafka -f job/k6-payments
kubectl logs -n keda deploy/keda-operator -f                    # when the metric is <unknown>
```

The HPA's name is `keda-hpa-<scaledobject name>` and KEDA owns it: editing it by hand is reverted
on the next reconcile, exactly like editing a topic the Topic Operator manages.

### Autoscaling proof

Measured on the live cluster. k6 driving `POST /api/payments` from 3 pods through the gateway, with
replica count, the ScaledObject's `ACTIVE` flag, and consumer-group lag sampled independently
(`kafka-consumer-groups --describe`, not KEDA's own number):

```
   t        replicas   ACTIVE   lag
   +15s     1/1        False       0     load starts
   +30s     1/3        True        -     lag crosses activationLagThreshold
   +45s     3/3        True        -
   +60s     3/5        True        -
   +90s     6/6        True        -     maxReplicaCount reached
   ...
   peak     6/6        True     2279     backlog at its highest
   +440s    6/6        True     1257     draining
   +900s    6/6        True       49
   +960s    4/4        True        5     scale-in begins
   +990s    2/2        False       0
   +1020s   1/1        False       0     back to minReplicaCount
```

Scale out **1 -> 3 -> 5 -> 6**, hold at the ceiling while the backlog drains, then in
**6 -> 4 -> 2 -> 1**. A demo that only scales out is half a demo; the interesting half is that
`scaleDown.stabilizationWindowSeconds` decides when it is safe to give capacity back.

**The drain rate validates the sizing arithmetic.** Between two samples the backlog fell from 2279
to 1257 - 1022 messages in 440 s across 6 pods, or **0.387 msg/s per pod**. `lagThreshold` was
derived from the simulated provider's ~2.55 s mean latency and one consumer thread per pod, giving
0.39 msg/s. Prediction and measurement agree to two decimal places, which is the difference between
a threshold that was reasoned about and one that was guessed.

**The work was real, not merely measured**: `merchant_balances` shows **2499 entries totalling
6138.86 EUR** settled during the drill. Lag reaching zero because a backlog was consumed is a
different thing from lag reaching zero because a broken scaler reported nothing - and this project
has three prior cases of components reporting healthy while moving no data.

#### The partition count is the ceiling, and it was set in Phase 0

`maxReplicaCount: 6` against a 12-partition topic. Twelve is the hard bound: a thirteenth consumer
in the group receives an empty assignment and contributes nothing but a rebalance - measured
directly in [M4's partition/consumer ratio drill](../../services/psp-connector/README.md#3-partition--consumer-ratio).
Six was chosen below it for memory, and because it divides 12 evenly.

Worth sitting with: the scaling ceiling of a Kafka consumer is not a Kubernetes property, an HPA
setting, or a KEDA parameter. It is the partition count, fixed by ADR-0003 in Phase 0 before any of
this existed. `allowIdleConsumers: "false"` makes the scaler enforce that bound itself even if
someone later raises `maxReplicaCount` - the right place for the guard, since a person editing a
replica limit should not have to remember a decision made in a design document.

#### Two things that would otherwise look like bugs

**A phantom lag floor on a fresh cluster.** Partitions the group has never committed an offset for
each contribute 1 to KEDA's lag, so a cluster with an empty topic reports non-zero lag with no
backlog at all. It disappeared permanently once load touched all 12 partitions.

**`pollingInterval` and `cooldownPeriod` are inert here.** With `minReplicaCount >= 1` KEDA is not
in the scaling loop - it publishes a metric and the HPA scales on its own cadence. Scale-in timing
comes from the HPA's `scaleDown` stabilization window, and Kubernetes' 300 s default would have made
this drill look broken. Both fields are kept and documented rather than deleted, because deleting
them hides the question.

### Troubleshooting

**`TARGETS: <unknown>/25` on the HPA.** The metrics apiserver cannot answer. Check
`kubectl get pods -n keda` (all three), then `kubectl logs -n keda deploy/keda-operator` for the
scaler's own error - a Kafka authentication or authorization failure shows up there and *nowhere
else*.

**`ACTIVE=False` and zero lag while the topic is visibly backed up.** The scaler cannot see what it
thinks it is measuring, and this is the failure this whole section warns about. In order: is the
`consumerGroup` string exactly `psp-connector.v1`; does `Secret/keda-scaler` exist; does
`KafkaUser/keda-scaler` say `Ready`; do the ACLs cover *this* topic and *this* group. Cross-check
with `watch-scaling.sh`, which reads the lag without going through KEDA.

**A non-zero lag floor at idle - `ACTIVE=True` with nothing running.** Partitions the group has
**never committed an offset for** each contribute `1` to KEDA's lag, by design
(`scaleToZeroOnInvalidOffset: false` keeps one consumer for a partition it cannot place). On a
freshly-built cluster where only a handful of payments have flowed, most of the 12 partitions are
in that state and the reported lag is the *count of never-committed partitions*, not a backlog -
`kafka-consumer-groups.sh --describe` shows `-` in the CURRENT-OFFSET column for exactly those
partitions. It disappears permanently the first time a load test puts a record on every partition.
This is worth knowing precisely because it looks like the scaler working when it is not yet
measuring anything.

**Replicas snap back to 1 after `helm upgrade`.** Expected exactly once, when autoscaling is first
enabled - see the `replicas` note above. If it happens on *every* upgrade, `autoscaling.enabled` is
false and the chart is rendering `replicas` again. Note that `kubectl get deploy psp-connector -o
yaml` **will** show `spec.replicas: 1` even when the chart omits it: the API server defaults the
field. The authority on whether Helm is managing it is the rendered manifest, not the live object:

```bash
helm get manifest psp -n kafka | grep -A20 'name: psp-connector' | grep replicas   # expect nothing
```

**A `values.yaml` change had no effect, but a *new* key did.** Hit for real while building this
phase. `helm upgrade --reuse-values` reuses the previous release's *computed* values, so a key that
already existed keeps its **old** value while genuinely new keys are merged in. Editing the
`config:` string (an existing key) alongside adding `autoscaling:` (a new one) therefore deployed
the ScaledObject and silently kept the old ConfigMap - `helm template` showed the new content and
the cluster did not. Re-run without `--reuse-values`, passing the image tag explicitly, which is
what `deploy-apps.sh` does:

```bash
helm upgrade psp infra/k8s/charts/psp-platform -n kafka --set global.imageTag=<tag> --wait
```

**Scaled out and never came back.** Almost always the HPA's scale-down stabilization window, not
KEDA. `kubectl describe hpa keda-hpa-psp-connector -n kafka` shows the recommendation it is holding
onto; `cooldownPeriod` is not involved while `minReplicaCount ≥ 1`.

**Pods `Pending` during scale-out.** Memory, as in phase 1. This is why `maxReplicaCount` is 6 and
not 12 on this machine.

**k6 reports mostly 429.** `RATE` is above the gateway's 5/s per-IP replenish rate. Use
`--pods N` instead - more source IPs, not a higher rate.

### Compromises / what didn't fully match the ideal

- **`maxReplicaCount` is 6, not the topic's 12.** The correctness ceiling is 12 and the manifest
  says so; 6 is a memory decision about this laptop, and it means the drill demonstrates the
  *shape* of lag-based scaling without ever reaching the point where adding a consumer genuinely
  stops helping. Watching replica 13 sit idle would be the more instructive demo, and it needs a
  bigger machine.
- **Scaling is on lag only.** No CPU or memory trigger, and no composite. Lag is the right primary
  signal for a consumer, but a real deployment would add a CPU trigger so a pod that is slow for a
  reason unrelated to backlog is still noticed.
- **`stabilizationWindowSeconds: 60` on scale-down is a demo value.** Production wants something
  closer to the 300 s default, for the rebalance reason given above. The number here is chosen so a
  human can watch the whole loop.
- **The provider latency stays random by default**, so per-pod throughput is a mean (~0.39/s), and
  every replica-count prediction in this section is therefore approximate.
  `providerForcedLatencyMs` exists to make it exact and is deliberately off.
- **Only psp-connector is autoscaled.** `ledger` and `webhook-notifier` are also lag-bound
  consumers with the same argument available to them; one ScaledObject is enough to demonstrate the
  mechanism, and each additional one is another `KafkaUser`, two more ACLs and another `Deployment`
  whose `replicas` field has to move.
- **No `PodDisruptionBudget`, and no `terminationGracePeriodSeconds` tuning.** Scale-in kills a
  consumer mid-poll and relies on the group rebalancing; with manual acks and
  `auto-offset-reset: earliest` that is at-least-once, which this system already is (M5's
  idempotency work), but a graceful `close()` on the consumer would make scale-in cheaper.
- **KEDA's own metrics are not scraped.** `prometheus.*.enabled` is false because no Prometheus is
  deployed in this module - the same honest gap as the tracing sampler in `psp.commonConfig`.

---

## Monitoring: Prometheus + Grafana on the cluster (M19 revisit)

M15 built a metrics stack — kafka-exporter, Prometheus, Grafana, two dashboards — and it lived
entirely in `infra/compose`. When the platform moved to Kubernetes it did not come along, and
part 1 of the failure drills had to open with a disclaimer: *"Not possible in this cluster:
anything requiring Grafana or Prometheus."* The lag drill from `docs/PLAN.md` was deferred on
those grounds. This section is that gap closed; the drill it unblocked is
[part 2, drill 10](../../docs/M19-failure-drills-part2.md).

Two things had to happen, and they are genuinely separate:

| | Where the numbers come from | How it is turned on |
|---|---|---|
| **Broker metrics** — request rates, ISR, log size, controller state | JMX MBeans inside each broker JVM, translated by the Prometheus JMX Exporter javaagent on port 9404 | `Kafka.spec.kafka.metricsConfig` + `kafka/15-metrics-configmap.yaml` |
| **Consumer group lag** — `kafka_consumergroup_lag` | *not* a broker MBean; a client that subtracts committed offset from end offset, per partition | `Kafka.spec.kafkaExporter` |

The distinction matters because the obvious mental model ("turn on Kafka metrics, get lag") is
wrong, and getting only the first half produces ~900 healthy broker series and zero lag.

### Turning it on

```bash
kubectl apply -f infra/k8s/kafka/15-metrics-configmap.yaml   # rules first, or the CR dangles
kubectl apply -f infra/k8s/kafka/20-kafka.yaml               # ROLLS ALL THREE BROKERS
kubectl wait kafka/psp -n kafka --for=condition=Ready --timeout=15m

infra/k8s/scripts/install-monitoring.sh                      # Prometheus + Grafana
```

**The second command restarts the Kafka cluster.** `metricsConfig` changes the broker pod spec
(a javaagent and a container port), so the operator does a rolling restart, one pod at a time,
waiting for ISR to recover in between. Measured on this cluster: **2 min 18 s** from apply to
`Ready=True`, in the order `psp-combined-1 → -2 → -0`. There is no way to add broker metrics to a
running Strimzi cluster without it — which is exactly why `install-monitoring.sh` does *not* do
this step. A script that silently restarts Kafka as a side effect of "install monitoring" is one
nobody can run during an incident; it only checks the step has happened and warns if it hasn't.

### The charts, pinned

| | |
|---|---|
| Prometheus | chart **29.27.0** (`prometheus-community/prometheus`, appVersion **v3.14.0**) |
| Grafana | chart **10.5.15** (`grafana/grafana`, appVersion **12.3.1**) |
| Namespace | `monitoring` |
| Installed as | pinned `.tgz` from the GitHub release asset — same discipline as Strimzi and KEDA |

**Why not `kube-prometheus-stack`.** It is the default answer and the wrong one here. It brings
the Prometheus Operator plus ~10 CRDs, and then every target has to be expressed as a
ServiceMonitor rather than as a scrape job. That indirection pays for itself when many teams own
their own targets; this cluster has two Kafka targets and one self-scrape, so it would buy a
controller, a CRD-versioning dependency and a pile of alerting/kube-state defaults in exchange
for nothing. Plain `prometheus` + plain `grafana` is two Deployments and a `helm uninstall` that
actually removes everything. The honest cost: adding a target means editing
`monitoring/prometheus-values.yaml` and running `helm upgrade`, not applying a CR.

Both charts are trimmed hard for a laptop — alertmanager, kube-state-metrics, node-exporter and
pushgateway are all `enabled: false`, persistence is `emptyDir` on both, and eight of the ten
default scrape jobs are switched off. Prometheus retention is **6 h**. Every one of those is
commented with its reasoning in the values files.

> The `grafana/grafana` chart prints `WARN this chart is deprecated` on install. It still
> installs and runs; upstream's replacement path is the `grafana-operator`, which is the same
> CRD-and-controller trade rejected above.

### What Prometheus scrapes

```
$ kubectl port-forward -n monitoring svc/prometheus-server 9090:9090
$ curl -s 'localhost:9090/api/v1/targets?state=active' | jq -r '.data.activeTargets[]
    | "\(.labels.job) \(.scrapeUrl) \(.health)"'
kafka-brokers    http://10.244.2.14:9404/metrics  up
kafka-brokers    http://10.244.1.20:9404/metrics  up
kafka-brokers    http://10.244.2.15:9404/metrics  up
kafka-exporter   http://10.244.2.16:9404/metrics  up
prometheus       http://localhost:9090/metrics    up
```

Both Kafka jobs use `role: pod` discovery, for two different reasons:

- **The brokers** are scraped per pod and not through `psp-kafka-bootstrap`, because that Service
  load-balances — scraping it would hit a random broker per scrape and produce one incoherent
  series instead of three.
- **The exporter** is scraped per pod because Strimzi does not create a Service for it at all.
  `kubectl get svc -n kafka` lists `psp-kafka-bootstrap`, `-brokers`, `-external-bootstrap` and
  the three per-broker cluster-ips; there is no `psp-kafka-exporter`. Its only address is the pod
  IP.

One trap, worth writing down because it fails *quietly*: the first version of the broker job
selected on `strimzi.io/kind=Kafka` + port name `tcp-prometheus`. The operator stamps
`kind=Kafka` on the **kafka-exporter** pod too, and names its port `tcp-prometheus` as well — so
the broker job swallowed the exporter and both jobs reported the same target. The per-component
label `strimzi.io/name` (`psp-kafka` vs `psp-kafka-exporter`) is the one that actually separates
them.

**Not scraped: the eight Spring services.** They all carry `spring-boot-starter-actuator` and
expose `health,info,metrics`, but not `prometheus` — no service has `micrometer-registry-prometheus`
on its classpath (`grep -rl micrometer-registry-prometheus --include=pom.xml .` returns nothing).
`/actuator/metrics` is a browsable JSON tree, one metric per request; it is not the Prometheus
exposition format and Prometheus cannot read it. Adding the registry to eight POMs and rebuilding
eight images is a change to the *applications*, out of scope for a metrics-infrastructure
milestone, so this is Kafka-side metrics only. Business counters (`ledger.entries.applied` and
friends) stay reachable exactly as every drill in `docs/` already reaches them, with
`curl /actuator/metrics/<name>`.

### Grafana access

```bash
kubectl port-forward -n monitoring svc/grafana 3000:80
open http://localhost:3000        # user: admin
```

The password is **generated once, at first install, and exists only in the cluster** — there is
deliberately no `adminPassword:` in `grafana-values.yaml`, because a password in a values file is
a password in git. Read it back with:

```bash
kubectl get secret grafana -n monitoring -o jsonpath='{.data.admin-password}' | base64 -d ; echo
```

`helm upgrade` keeps the existing Secret, so re-running the installer does not rotate it; deleting
the Secret and upgrading does.

### The dashboards are M15's, unchanged

`install-monitoring.sh` builds the ConfigMap `grafana-dashboards-kafka` straight out of
`infra/compose/grafana/dashboards/*.json`. **The JSON is not copied into `infra/k8s/`** — the
compose stack and the kind cluster render byte-identical dashboards from one source, and there is
one copy of each to maintain.

Nothing in them needed adapting, which is worth stating because it was the main open question:
M15's panels are written against **kafka-exporter's** metric names, and Strimzi's
`spec.kafkaExporter` runs that same exporter, so every query matched on the first try. Verified
against the live cluster rather than assumed:

| Metric a panel asks for | Series present |
|---|---|
| `kafka_consumergroup_lag` | 474 |
| `kafka_consumergroup_current_offset` | 474 |
| `kafka_topic_partition_in_sync_replica` | 334 |
| `kafka_topic_partition_leader` | 334 |
| `kafka_topic_partitions` | 43 |
| `kafka_brokers` | 1 |

The one thing that *is* load-bearing is the datasource **uid**. Every panel refers to its
datasource as `{"type": "prometheus", "uid": "prometheus"}` — a fixed id chosen in M15 precisely
so the JSON would be portable — so `grafana-values.yaml` provisions the datasource with
`uid: prometheus`. Any other uid (including a Grafana-generated one) imports both dashboards
successfully and renders every panel as *"Datasource prometheus was not found"*. It fails per
panel, not at install time, which is what makes it the most likely way this goes wrong.

```
$ curl -su admin:$PW localhost:3000/api/search?type=dash-db
Kafka Cluster Overview (M2)   kafka-m2-overview        folder: Kafka
Kafka Consumer Lag (M15)      kafka-m15-consumer-lag   folder: Kafka
```

### Reading lag from the API instead of the UI

A dashboard is for watching; a drill needs numbers that can be pasted into a document. Prometheus's
`query_range` gives the same series as a table:

```bash
kubectl port-forward -n monitoring svc/prometheus-server 9090:9090 &
curl -s 'http://localhost:9090/api/v1/query_range' \
  --data-urlencode 'query=sum(kafka_consumergroup_lag{consumergroup="psp-connector.v1",topic="payments.payment-requested.v1"})' \
  --data-urlencode "start=$(date -v-20M +%s)" --data-urlencode "end=$(date +%s)" \
  --data-urlencode 'step=10'
```

Two things to know about the values that come back:

- **kafka-exporter reports `-1`, not `0`, for a partition a group has never committed to.** A
  bare `sum(kafka_consumergroup_lag)` over an idle group therefore comes back *negative* (this
  cluster idles at `-4`: `refunds.funds-reserved.v1` has partitions psp-connector has never
  consumed). Always filter by `topic=`.
- The series is a **10 s sample of a number that moves in hundreds per second**, which is why
  `scrape_interval` is 10 s here and not the 60 s chart default.

### Compromises

- **No persistence, on either component.** Deleting the Prometheus pod deletes every sample. That
  is acceptable because these metrics are evidence for a drill that gets captured to a document
  while it runs, not a system of record — and an 8 GiB PVC on a laptop with ~5 GiB free is not a
  trade this cluster can make.
- **No alerting rules and no Alertmanager.** The stack answers "what is happening", never "wake
  someone up". Every drill in `docs/` is a human watching on purpose.
- **KEDA's own metrics are still not scraped.** `keda/values.yaml` sets
  `prometheus.*.enabled: false` because those flags create `ServiceMonitor` CRs, whose CRD comes
  from the Prometheus Operator — which this install deliberately does not have. KEDA's view of
  lag is therefore observed through `kubectl get hpa`, not plotted. The scaler's number and the
  exporter's number being independently derived is arguably a feature during a drill.
- **No `kube-state-metrics`, so replica counts are not a metric.** Drill 10's replica trajectory
  is sampled with `kubectl get deploy`, not queried; correlating it with the lag curve is done by
  timestamp, by hand.

## Client quotas (M13), measured on this cluster

The last of M13's grab-bag: `producer_byte_rate`, applied and observed. Three findings, each
worth more than the feature itself:

**1. On a Strimzi cluster, quotas belong to the `KafkaUser` CR — a hand-set quota evaporates.**
`kafka-configs.sh --alter --add-config producer_byte_rate=...` on a UO-managed principal is
undone by the User Operator's next reconcile, within seconds; the follow-up `--delete-config`
then fails with `Invalid config(s)` because the quota is already gone. The drill therefore ran
as a throwaway CR-native user:

```yaml
spec:
  quotas:
    producerByteRate: 1048576   # 1 MiB/s
```

**2. Short bursts sail through — the quota is a rolling window, not a valve.** An 8 MB produce
finished in 0.7 s at **11.68 MB/s** with `produce-throttle-time = 0.000`: it fit entirely inside
the enforcement window (`quota.window.num` x `quota.window.size.seconds`, ~11 s by default), so
the broker never delayed a response. A quota does not cap a burst; it caps a *rate sustained
past the window*.

**3. Sustained load hits the quota almost exactly, and both sides can see it.** 40,000 x 1 KiB
against the same 1 MiB/s quota (baseline unquoted: **11.50 MB/s**):

```
40000 records sent, 1145.11 records/sec (1.12 MB/sec), 15260 ms avg latency, 34293 ms max
producer-metrics:produce-throttle-time-avg: 12.043
producer-metrics:produce-throttle-time-max: 1017.000
```

Prometheus, from the broker (the metrics stack above):

```
max(kafka_network_requestmetrics_throttletimems{request="Produce",quantile="0.99"}) = 1008.42 ms
```

The 34-second max latency is the quota's real mechanism showing through: the broker throttles
by **delaying responses**, the producer's buffer fills behind the delayed acks, and every
record queued behind the window pays the wait. A quota protects the broker by exporting the
pain to the client - which is exactly its job.

The drill topic (`drill.m13.quota`, ~70 MB) and the `drill-quota` user were removed afterwards.
