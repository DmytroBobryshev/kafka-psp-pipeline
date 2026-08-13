#!/usr/bin/env bash
#
# infra/k8s/scripts/watch-scaling.sh - M18 phase 3, the one screen that shows the whole loop.
#
# Prints, every INTERVAL seconds, the four numbers that make an autoscaling claim checkable:
#
#   LAG        real consumer lag, read from Kafka as User:admin - the INPUT, measured independently
#              of KEDA so that "KEDA says the lag is X" is never the only evidence that it is X
#   ACTIVE     the ScaledObject's own view (activationLagThreshold crossed or not)
#   TARGET     the HPA's metric line: <current avg lag per replica>/<lagThreshold>
#   REPLICAS   desired vs ready - the OUTPUT
#
# Usage:  ./watch-scaling.sh [seconds]        (default 10, Ctrl-C to stop)
set -euo pipefail

NS="${NS:-kafka}"
INTERVAL="${1:-10}"
GROUP="${GROUP:-psp-connector.v1}"
TOPIC="${TOPIC:-payments.payment-requested.v1}"
BROKER_POD="${BROKER_POD:-psp-combined-0}"

# The independent lag reading needs a Kafka client config; `admin` is used HERE (and only here)
# because this is an operator's diagnostic shell, not a component of the system. KEDA itself uses
# User:keda-scaler with two Describe grants - see infra/k8s/kafka/users/24-keda-scaler.yaml.
ADMIN_PW="$(kubectl get secret admin -n "$NS" -o jsonpath='{.data.password}' | base64 -d)"
kubectl exec -n "$NS" "$BROKER_POD" -- bash -c "cat > /tmp/watch.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"${ADMIN_PW}\";
EOF" >/dev/null

lag() {
  kubectl exec -n "$NS" "$BROKER_POD" -- bin/kafka-consumer-groups.sh \
    --bootstrap-server psp-kafka-bootstrap:9092 --command-config /tmp/watch.properties \
    --describe --group "$GROUP" 2>/dev/null \
    | awk -v t="$TOPIC" '$2==t && $6 ~ /^[0-9]+$/ {s+=$6} END {print (s==""?0:s)}'
}

printf '%-9s  %-7s  %-7s  %-14s  %-9s  %s\n' TIME LAG ACTIVE TARGET REPLICAS PODS
while true; do
  L="$(lag || echo '?')"
  ACTIVE="$(kubectl get scaledobject psp-connector -n "$NS" \
    -o jsonpath='{.status.conditions[?(@.type=="Active")].status}' 2>/dev/null || echo '?')"
  # Read the HPA through jsonpath, NOT by parsing `kubectl get hpa` columns: the TARGETS column
  # renders as `13167m/25 (avg)`, i.e. it contains a space, so every awk field index after it is
  # off by one - and the resulting numbers look plausible, which is worse than an error.
  HPA_JSON="$(kubectl get hpa keda-hpa-psp-connector -n "$NS" \
    -o jsonpath='{.status.currentMetrics[0].external.current.averageValue}|{.spec.metrics[0].external.target.averageValue}|{.spec.minReplicas}|{.spec.maxReplicas}|{.status.desiredReplicas}' 2>/dev/null || true)"
  IFS='|' read -r CUR TGT MINR MAXR DESIRED <<<"$HPA_JSON"
  TARGET="${CUR:-?}/${TGT:-?}"
  REPL="${MINR:-?}-${MAXR:-?}:${DESIRED:-?}" # min-max:desired
  READY="$(kubectl get pods -n "$NS" -l app.kubernetes.io/name=psp-connector \
    --no-headers 2>/dev/null | awk '{c++; if ($2=="1/1" && $3=="Running") r++} END {print (r+0)"/"c+0}')"
  printf '%-9s  %-7s  %-7s  %-14s  %-9s  %s\n' "$(date +%H:%M:%S)" "$L" "${ACTIVE:-?}" "${TARGET:-?}" "${REPL:-?}" "$READY"
  sleep "$INTERVAL"
done
