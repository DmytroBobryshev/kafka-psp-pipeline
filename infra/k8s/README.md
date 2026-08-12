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
└── scripts/
    ├── up.sh                        bring everything up from nothing, idempotent
    ├── down.sh                      workload | namespace | cluster
    ├── smoke-test.sh                produce + consume as real principals, plus the denial half
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
