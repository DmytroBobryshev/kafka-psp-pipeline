#!/usr/bin/env bash
#
# infra/k8s/scripts/deploy-apps.sh - M18 phase 2, from a running phase-1 cluster to a working
# pipeline.
#
# Assumes phase 1 is up: `kubectl get kafka psp -n kafka` says Ready, the 26 KafkaTopics and 11
# KafkaUsers are reconciled. Run infra/k8s/scripts/up.sh first if not.
#
# Idempotent: re-running rebuilds the images (cheap if nothing changed - Docker layer cache), and
# `helm upgrade --install` converges the release.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
NAMESPACE="${NAMESPACE:-kafka}"
RELEASE="${RELEASE:-psp}"
CHART="$REPO_ROOT/infra/k8s/charts/psp-platform"
INGRESS_NGINX_VERSION="${INGRESS_NGINX_VERSION:-controller-v1.13.0}"

SKIP_BUILD=0
[[ "${1:-}" == "--skip-build" ]] && SKIP_BUILD=1

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------------------------------------
# 1. ingress-nginx
# ---------------------------------------------------------------------------------------------
# kind's own manifest, not the Helm chart: it pins the controller to the node labelled
# ingress-ready=true (which infra/k8s/kind.yaml puts on the control plane) and uses hostPort, so
# it lands on the node whose ports 80/443 kind published to the host. The generic chart installs a
# LoadBalancer Service, which on kind stays <pending> forever.
if ! kubectl get ns ingress-nginx >/dev/null 2>&1; then
  log "installing ingress-nginx (${INGRESS_NGINX_VERSION})"
  kubectl apply -f "https://raw.githubusercontent.com/kubernetes/ingress-nginx/${INGRESS_NGINX_VERSION}/deploy/static/provider/kind/deploy.yaml"
else
  log "ingress-nginx already present"
fi

# ---------------------------------------------------------------------------------------------
# 2. images
# ---------------------------------------------------------------------------------------------
if [[ "$SKIP_BUILD" -eq 0 ]]; then
  log "building and loading images"
  "$REPO_ROOT/infra/k8s/scripts/build-images.sh"
fi
IMAGE_TAG="${IMAGE_TAG:-$("$REPO_ROOT/infra/k8s/scripts/build-images.sh" --print-tag)}"
log "deploying image tag ${IMAGE_TAG}"

# ---------------------------------------------------------------------------------------------
# 3. the chart
# ---------------------------------------------------------------------------------------------
log "helm upgrade --install ${RELEASE}"
helm upgrade --install "$RELEASE" "$CHART" \
  --namespace "$NAMESPACE" \
  --set "global.imageTag=${IMAGE_TAG}" \
  --wait --timeout 15m

# ---------------------------------------------------------------------------------------------
# 4. wait for the parts Helm cannot wait on
# ---------------------------------------------------------------------------------------------
# `helm --wait` understands Deployments. It does not understand a Strimzi CR, so KafkaConnect and
# the two KafkaConnectors have to be waited on explicitly - and they are the two things most
# likely to be the reason a payment goes nowhere.
log "waiting for ingress-nginx"
kubectl wait -n ingress-nginx --for=condition=Available deploy/ingress-nginx-controller --timeout=5m

log "waiting for KafkaConnect"
kubectl wait -n "$NAMESPACE" --for=condition=Ready kafkaconnect/psp-connect --timeout=10m

log "waiting for the connectors"
kubectl wait -n "$NAMESPACE" --for=condition=Ready kafkaconnector --all --timeout=5m

log "state"
kubectl get pods -n "$NAMESPACE"
echo
kubectl get kafkaconnector -n "$NAMESPACE" \
  -o custom-columns='NAME:.metadata.name,READY:.status.conditions[0].status,STATE:.status.connectorStatus.connector.state,TASKS:.status.connectorStatus.tasks[*].state'
echo
helm list -n "$NAMESPACE"

cat <<'EOF'

Reach the gateway:
  curl -s http://localhost/actuator/health | jq .           # through the nginx ingress
  kubectl port-forward -n kafka svc/api-gateway 8000:8000   # or straight at the Service

End-to-end smoke test:
  ./infra/k8s/scripts/e2e-payment.sh
EOF
