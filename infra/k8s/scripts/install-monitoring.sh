#!/usr/bin/env bash
#
# infra/k8s/scripts/install-monitoring.sh - M19 revisit: the metrics stack the kind cluster
# never had.
#
#   1. kafka/15-metrics-configmap.yaml + kafka/20-kafka.yaml   broker JMX exporter + kafkaExporter
#   2. install-monitoring.sh                                   Prometheus + Grafana (this file)
#   3. load-test.sh                                            something to plot
#
# Step 1 is NOT done here, on purpose: adding `metricsConfig` to the Kafka CR rolls all three
# brokers (~2 min on this cluster), and a script that silently restarts a Kafka cluster as a
# side effect of "install monitoring" is a script nobody can run during an incident. It is a
# `kubectl apply` the operator owns; this script only checks it has happened and says so.
#
# Idempotent: `helm upgrade --install` converges, and the dashboard ConfigMap is rebuilt with
# `--dry-run=client | apply` so a re-run picks up edited JSON instead of failing on AlreadyExists.
#
# WHY PINNED .tgz URLS AND NOT `helm repo add`: identical reasoning to Strimzi in up.sh and KEDA
# in install-keda.sh. A chart repo is a moving target; `helm repo update && helm upgrade` can
# change what is installed with no diff anywhere in git. Changing the two _CHART_VERSION values
# below is the only way the installed version moves.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$(cd "${HERE}/.." && pwd)"
REPO_ROOT="$(cd "${K8S_DIR}/../.." && pwd)"

PROMETHEUS_CHART_VERSION="${PROMETHEUS_CHART_VERSION:-29.27.0}"   # app v3.14.0
GRAFANA_CHART_VERSION="${GRAFANA_CHART_VERSION:-10.5.15}"         # app 12.3.1
MON_NS="${MON_NS:-monitoring}"
KAFKA_NS="${KAFKA_NS:-kafka}"

# The dashboards are the ones M15 wrote for the compose stack, used verbatim - see
# monitoring/grafana-values.yaml's "Dashboards" section for why they are not copied.
DASHBOARD_DIR="${REPO_ROOT}/infra/compose/grafana/dashboards"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[33m    !! %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------------------------------------
# 0. Preflight: is there anything to scrape?
# ---------------------------------------------------------------------------------------------
# Both of these are step 1's job. Checking them here turns the most likely failure - a perfectly
# healthy Prometheus with two permanently DOWN targets - into a message at install time instead
# of an empty dashboard twenty minutes later.
say "preflight"
if ! kubectl get configmap kafka-metrics -n "${KAFKA_NS}" >/dev/null 2>&1; then
  warn "ConfigMap kafka-metrics missing in ${KAFKA_NS}."
  warn "run: kubectl apply -f ${K8S_DIR}/kafka/15-metrics-configmap.yaml"
  warn "     kubectl apply -f ${K8S_DIR}/kafka/20-kafka.yaml    # rolls the brokers, ~2 min"
fi
if ! kubectl get pods -n "${KAFKA_NS}" -l strimzi.io/name=psp-kafka-exporter \
     --no-headers 2>/dev/null | grep -q Running; then
  warn "no kafka-exporter pod in ${KAFKA_NS} - Kafka.spec.kafkaExporter is not applied yet."
  warn "without it there is no kafka_consumergroup_lag and the lag dashboard stays empty."
fi
echo "    brokers exporting metrics: $(kubectl get pods -n "${KAFKA_NS}" -l strimzi.io/name=psp-kafka \
  -o jsonpath='{range .items[*]}{.spec.containers[0].ports[*].name}{"\n"}{end}' 2>/dev/null \
  | grep -c tcp-prometheus || true)/3"

# ---------------------------------------------------------------------------------------------
# 1. Namespace
# ---------------------------------------------------------------------------------------------
say "namespace ${MON_NS}"
kubectl apply -f "${K8S_DIR}/monitoring/00-namespace.yaml"

# ---------------------------------------------------------------------------------------------
# 2. Prometheus
# ---------------------------------------------------------------------------------------------
PROM_TGZ="/tmp/prometheus-${PROMETHEUS_CHART_VERSION}.tgz"
if [[ ! -f "${PROM_TGZ}" ]]; then
  say "downloading prometheus chart ${PROMETHEUS_CHART_VERSION}"
  curl -fsSL -o "${PROM_TGZ}" \
    "https://github.com/prometheus-community/helm-charts/releases/download/prometheus-${PROMETHEUS_CHART_VERSION}/prometheus-${PROMETHEUS_CHART_VERSION}.tgz"
fi

say "helm upgrade --install prometheus ${PROMETHEUS_CHART_VERSION}"
# Release name `prometheus` is load-bearing twice over: it makes the Service
# `prometheus-server`, which is the URL hard-coded in grafana-values.yaml's datasource, and it is
# what every port-forward in infra/k8s/README.md assumes.
helm upgrade --install prometheus "${PROM_TGZ}" \
  --namespace "${MON_NS}" \
  --values "${K8S_DIR}/monitoring/prometheus-values.yaml" \
  --wait --timeout 5m

# ---------------------------------------------------------------------------------------------
# 3. Dashboards -> ConfigMap  (must exist BEFORE Grafana starts)
# ---------------------------------------------------------------------------------------------
# Grafana mounts this ConfigMap as a volume; a missing one leaves the pod stuck in
# ContainerCreating rather than starting with no dashboards, so it is created first.
say "ConfigMap grafana-dashboards-kafka (from ${DASHBOARD_DIR#"${REPO_ROOT}/"})"
ls "${DASHBOARD_DIR}"/*.json >/dev/null   # fail loudly if M15's dashboards moved
kubectl create configmap grafana-dashboards-kafka -n "${MON_NS}" \
  --from-file="${DASHBOARD_DIR}" \
  --dry-run=client -o yaml | kubectl apply -f -

# ---------------------------------------------------------------------------------------------
# 4. Grafana
# ---------------------------------------------------------------------------------------------
GRAF_TGZ="/tmp/grafana-${GRAFANA_CHART_VERSION}.tgz"
if [[ ! -f "${GRAF_TGZ}" ]]; then
  say "downloading grafana chart ${GRAFANA_CHART_VERSION}"
  curl -fsSL -o "${GRAF_TGZ}" \
    "https://github.com/grafana/helm-charts/releases/download/grafana-${GRAFANA_CHART_VERSION}/grafana-${GRAFANA_CHART_VERSION}.tgz"
fi

say "helm upgrade --install grafana ${GRAFANA_CHART_VERSION}"
helm upgrade --install grafana "${GRAF_TGZ}" \
  --namespace "${MON_NS}" \
  --values "${K8S_DIR}/monitoring/grafana-values.yaml" \
  --wait --timeout 5m

# ---------------------------------------------------------------------------------------------
# 5. Report
# ---------------------------------------------------------------------------------------------
say "done"
kubectl get pods -n "${MON_NS}"
echo
cat <<EOF
    Prometheus  ${PROMETHEUS_CHART_VERSION}   kubectl port-forward -n ${MON_NS} svc/prometheus-server 9090:9090
                             then http://localhost:9090/targets  (expect 3 kafka-brokers + 1 kafka-exporter, all UP)

    Grafana     ${GRAFANA_CHART_VERSION}     kubectl port-forward -n ${MON_NS} svc/grafana 3000:80
                             then http://localhost:3000 -> folder "Kafka"

    admin password (generated once, at first install, and stored only in the cluster):
      kubectl get secret grafana -n ${MON_NS} -o jsonpath='{.data.admin-password}' | base64 -d ; echo

    next:       ${HERE}/load-test.sh --pods 3 --duration 3m    # drill 10, docs/M19-failure-drills-part2.md
EOF
