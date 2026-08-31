#!/usr/bin/env bash
# infra/k8s/scripts/up.sh
#
# M18 phase 1 - bring the whole Kafka-on-Kubernetes stack up from nothing, reproducibly.
#
# From an empty machine (kind + kubectl + helm installed) this creates the cluster, installs a
# PINNED Strimzi operator, deploys the Kafka cluster, and applies every KafkaTopic and KafkaUser.
# Idempotent: safe to re-run. It never deletes anything - see down.sh for that.
#
#   ./infra/k8s/scripts/up.sh
#
# Phase 1 is Kafka only. No Spring services, no KEDA, no ingress.
set -euo pipefail

# ---------------------------------------------------------------------------------------------
# Pinned versions. NOT `latest`, on purpose: `strimzi.io/install/latest` is a moving target, and
# an operator upgrade is a rolling restart of every broker. Bumping Strimzi is a deliberate act.
#
# Strimzi 1.1.0 ships Kafka 4.2.0 / 4.2.1 / 4.3.0 ONLY. The Kafka version is set in
# kafka/20-kafka.yaml (spec.kafka.version) and must be one of those three - the operator rejects
# anything else. Changing STRIMZI_VERSION here without checking that list is how you get a Kafka
# CR stuck in NotReady with "Unsupported Kafka.spec.kafka.version".
# ---------------------------------------------------------------------------------------------
STRIMZI_VERSION="${STRIMZI_VERSION:-1.1.0}"
CHART_URL="https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-kafka-operator-helm-3-chart-${STRIMZI_VERSION}.tgz"

KIND_CLUSTER="kafka-psp"
KUBE_CONTEXT="kind-${KIND_CLUSTER}"
OPERATOR_NS="strimzi-system"
KAFKA_NS="kafka"
KAFKA_CLUSTER="psp"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$(dirname "$HERE")"

say() { printf '\n==> %s\n' "$*"; }

# ---------------------------------------------------------------------------------------------
# 0. The kind cluster.
# ---------------------------------------------------------------------------------------------
say "kind cluster '${KIND_CLUSTER}'"
if kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER}"; then
  echo "    already exists - leaving it alone"
else
  kind create cluster --config "${K8S_DIR}/kind.yaml"
fi
kubectl config use-context "${KUBE_CONTEXT}" >/dev/null
kubectl wait --for=condition=Ready nodes --all --timeout=180s

# STOP THE COMPOSE STACK FIRST. infra/compose publishes 29092-29094 on the host, and
# scripts/port-forward.sh binds the same three ports so the k8s cluster is reachable at exactly
# the addresses the compose cluster used to be. Both running at once is a port clash on the host
# and, more importantly, ~8 GiB of Kafka in a VM that has ~8 GiB.
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^kafka1$'; then
  echo "    WARNING: the compose cluster looks like it is running (container 'kafka1')."
  echo "             Run 'docker compose -f infra/compose/docker-compose.yml stop' first."
fi

# ---------------------------------------------------------------------------------------------
# 1. Namespaces. The operator gets its own; the cluster gets its own.
# ---------------------------------------------------------------------------------------------
say "namespaces"
kubectl apply -f "${K8S_DIR}/strimzi/namespace.yaml"
kubectl apply -f "${K8S_DIR}/kafka/00-namespace.yaml"

# ---------------------------------------------------------------------------------------------
# 2. Strimzi cluster operator, pinned, watching only the kafka namespace.
#
# The chart .tgz comes from the GitHub release asset for ${STRIMZI_VERSION} - a fixed artifact,
# not a Helm repo that resolves to whatever is newest. `helm upgrade --install` makes the re-run
# path a no-op instead of an error.
# ---------------------------------------------------------------------------------------------
say "Strimzi cluster operator ${STRIMZI_VERSION} -> namespace ${OPERATOR_NS}, watching ${KAFKA_NS}"
CHART_TGZ="$(mktemp -d)/strimzi-${STRIMZI_VERSION}.tgz"
curl -fsSL --retry 3 -o "${CHART_TGZ}" "${CHART_URL}"
helm upgrade --install strimzi-cluster-operator "${CHART_TGZ}" \
  --namespace "${OPERATOR_NS}" \
  --values "${K8S_DIR}/strimzi/values.yaml" \
  --wait --timeout 5m

# ---------------------------------------------------------------------------------------------
# 3. The Kafka cluster. Node pool BEFORE the Kafka CR - the Kafka CR has no replica count of its
#    own to fall back on, so applying it first just leaves it waiting.
# ---------------------------------------------------------------------------------------------
say "Kafka cluster '${KAFKA_CLUSTER}' (3 combined broker+controller nodes, KRaft)"
kubectl apply -f "${K8S_DIR}/kafka/10-nodepool-combined.yaml"
# The jmx_exporter rules the Kafka CR's `metricsConfig` points at. BEFORE the CR: a metricsConfig
# naming a ConfigMap that does not exist leaves the operator reconciling forever. On an existing
# cluster this is free, but changing it (or the CR's metricsConfig block) rolls all three brokers
# - see scripts/install-monitoring.sh.
kubectl apply -f "${K8S_DIR}/kafka/15-metrics-configmap.yaml"
kubectl apply -f "${K8S_DIR}/kafka/20-kafka.yaml"

say "waiting for the Kafka CR to reach Ready (first run pulls ~500 MB of images)"
kubectl wait kafka/"${KAFKA_CLUSTER}" -n "${KAFKA_NS}" --for=condition=Ready --timeout=15m

# ---------------------------------------------------------------------------------------------
# 4. Topics and users. Only meaningful once the entity operator is up, which the wait above
#    guarantees.
# ---------------------------------------------------------------------------------------------
say "KafkaTopic CRs (docs/diagrams/topic-map.md)"
kubectl apply -f "${K8S_DIR}/kafka/topics/"

say "KafkaUser CRs (the M14 ACL matrix)"
kubectl apply -f "${K8S_DIR}/kafka/users/"

say "waiting for every topic and user to reconcile"
kubectl wait kafkatopic --all -n "${KAFKA_NS}" --for=condition=Ready --timeout=5m
kubectl wait kafkauser  --all -n "${KAFKA_NS}" --for=condition=Ready --timeout=5m

# ---------------------------------------------------------------------------------------------
# 5. Report.
# ---------------------------------------------------------------------------------------------
say "done"
kubectl get pods -n "${OPERATOR_NS}"
kubectl get pods -n "${KAFKA_NS}"
echo
kubectl get kafka,kafkanodepool -n "${KAFKA_NS}"
echo
echo "    topics: $(kubectl get kafkatopics -n ${KAFKA_NS} --no-headers | wc -l | tr -d ' ')"
echo "    users:  $(kubectl get kafkausers  -n ${KAFKA_NS} --no-headers | wc -l | tr -d ' ')"
echo
echo "    smoke test:   ${HERE}/smoke-test.sh"
echo "    host access:  ${HERE}/port-forward.sh"
echo "    a password:   kubectl get secret payment-api -n ${KAFKA_NS} -o jsonpath='{.data.password}' | base64 -d"
