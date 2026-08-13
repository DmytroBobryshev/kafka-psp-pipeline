#!/usr/bin/env bash
#
# infra/k8s/scripts/install-keda.sh - M18 phase 3, step 1 of 3.
#
#   1. install-keda.sh     the autoscaler itself (this file)
#   2. deploy-apps.sh      re-runs helm, which now also renders psp-connector's ScaledObject
#   3. load-test.sh        k6, to give the ScaledObject something to react to
#
# Idempotent: `helm upgrade --install` converges, and the KafkaUser apply is a no-op if unchanged.
#
# WHY THE OPERATOR IS INSTALLED FROM A PINNED .tgz AND NOT `helm repo add kedacore`
# Identical reasoning to Strimzi in up.sh: a chart repo is a moving target, and an autoscaler
# upgrading itself on a Tuesday is an autoscaler that changes how many pods run without a diff.
# The URL below is a content-addressable release artifact; changing KEDA_VERSION is the only way
# the installed version moves.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$(cd "${HERE}/.." && pwd)"

KEDA_VERSION="${KEDA_VERSION:-2.20.2}"
KEDA_NS="${KEDA_NS:-keda}"
KAFKA_NS="${KAFKA_NS:-kafka}"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------------------------------------
# 1. Namespace + the pinned chart
# ---------------------------------------------------------------------------------------------
say "namespace ${KEDA_NS}"
kubectl apply -f "${K8S_DIR}/keda/namespace.yaml"

CHART_TGZ="/tmp/keda-${KEDA_VERSION}.tgz"
if [[ ! -f "${CHART_TGZ}" ]]; then
  say "downloading KEDA chart ${KEDA_VERSION}"
  curl -fsSL -o "${CHART_TGZ}" "https://kedacore.github.io/charts/keda-${KEDA_VERSION}.tgz"
fi

say "helm upgrade --install keda ${KEDA_VERSION}"
helm upgrade --install keda "${CHART_TGZ}" \
  --namespace "${KEDA_NS}" \
  --values "${K8S_DIR}/keda/values.yaml" \
  --wait --timeout 5m

# ---------------------------------------------------------------------------------------------
# 2. The scaler's own Kafka identity
# ---------------------------------------------------------------------------------------------
# THE POINT OF THIS STEP. KEDA's Kafka trigger is a Kafka client - it reads the consumer group's
# committed offsets and the topic's end offsets. On a cluster with deny-by-default authorization
# (M14, carried into phase 1) that means it needs a principal and ACLs like anything else. It is
# NOT given the admin superuser, and no ACL is loosened to accommodate it.
#
# The failure mode this guards against is the quiet one: an unauthenticated or unauthorized
# scaler does not error out of `kubectl get scaledobject` - it reports a lag it cannot see, which
# looks exactly like "there is no lag", and nothing ever scales.
say "KafkaUser keda-scaler (Describe on the group and the topic, nothing else)"
kubectl apply -f "${K8S_DIR}/kafka/users/24-keda-scaler.yaml"
kubectl wait kafkauser/keda-scaler -n "${KAFKA_NS}" --for=condition=Ready --timeout=2m

# ---------------------------------------------------------------------------------------------
# 3. Report
# ---------------------------------------------------------------------------------------------
say "done"
kubectl get pods -n "${KEDA_NS}"
echo
kubectl get crd -o name | grep keda.sh || true
echo
echo "    KEDA version:  $(helm list -n "${KEDA_NS}" -o json | grep -o '"app_version":"[^"]*"' | head -1)"
echo "    next:          ${HERE}/deploy-apps.sh --skip-build   # renders the ScaledObject"
echo "    then:          ${HERE}/load-test.sh                  # k6, to build lag"
