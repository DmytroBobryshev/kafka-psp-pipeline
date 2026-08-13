#!/usr/bin/env bash
#
# infra/k8s/scripts/load-test.sh - M18 phase 3, step 3 of 3.
#
# Runs infra/k8s/load/payments.js against POST /api/payments to build consumer lag on
# payments.payment-requested.v1, so KEDA's ScaledObject has something real to react to.
#
#   ./load-test.sh                          in-cluster Job, 1 pod, 5 req/s, 3m   (the default)
#   ./load-test.sh --pods 3 --duration 2m   3 source IPs => ~15 req/s (see the rate-limiter note)
#   ./load-test.sh --host                   from the laptop, through kubectl port-forward
#   ./load-test.sh --watch                  print HPA/replica/lag every 10s while it runs
#
# The rate is per pod and defaults to 5/s because api-gateway rate-limits at 5/s per client IP
# (M16). More load comes from more pods, not a higher rate - see infra/k8s/load/k6-job.yaml.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_DIR="$(cd "${HERE}/../load" && pwd)"
NS="${NS:-kafka}"

PODS=1
RATE=5
DURATION=3m
MODE=job
WATCH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pods) PODS="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --host) MODE=host; shift ;;
    --watch) WATCH=1; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------------------------------------
# Host mode: k6 on the laptop, through a port-forward.
# ---------------------------------------------------------------------------------------------
# Documented and supported, but NOT the default: a port-forward is one TCP tunnel through the API
# server, so at any interesting rate it becomes the bottleneck and the thing being measured.
# Useful when you want k6's live terminal UI, or when you do not want to pull an image into kind.
if [[ "$MODE" == host ]]; then
  command -v k6 >/dev/null || { echo "k6 not on PATH - brew install k6, or drop --host"; exit 1; }
  say "port-forwarding svc/api-gateway 8000:8000"
  kubectl port-forward -n "$NS" svc/api-gateway 8000:8000 >/dev/null 2>&1 &
  PF_PID=$!
  trap 'kill "$PF_PID" 2>/dev/null || true' EXIT
  sleep 3
  say "k6 (host) ${RATE}/s for ${DURATION}"
  RATE="$RATE" DURATION="$DURATION" BASE_URL=http://localhost:8000 k6 run "${LOAD_DIR}/payments.js"
  exit 0
fi

# ---------------------------------------------------------------------------------------------
# Job mode (default).
# ---------------------------------------------------------------------------------------------
say "ConfigMap k6-payments (from ${LOAD_DIR}/payments.js)"
# --dry-run=client | apply, so re-running picks up an edited script instead of failing on
# AlreadyExists. The script is a real file in git; only its delivery is a ConfigMap.
kubectl create configmap k6-payments -n "$NS" \
  --from-file=payments.js="${LOAD_DIR}/payments.js" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl delete job k6-payments -n "$NS" --ignore-not-found --wait=true >/dev/null

say "Job k6-payments: ${PODS} pod(s) x ${RATE} req/s for ${DURATION}"
# The Job manifest carries the defaults; the overrides here are applied with a JSON patch on the
# fly so infra/k8s/load/k6-job.yaml stays a readable, complete, apply-able manifest.
python3 - "${LOAD_DIR}/k6-job.yaml" "$PODS" "$RATE" "$DURATION" <<'PY' > /tmp/k6-job-rendered.yaml
import re, sys
path, pods, rate, duration = sys.argv[1:5]
src = open(path).read()
src = re.sub(r'(\n  parallelism: )\d+', r'\g<1>' + pods, src, count=1)
src = re.sub(r'(\n  completions: )\d+', r'\g<1>' + pods, src, count=1)
src = re.sub(r'(- name: RATE\n              value: ")[^"]*', r'\g<1>' + rate, src, count=1)
src = re.sub(r'(- name: DURATION\n              value: ")[^"]*', r'\g<1>' + duration, src, count=1)
sys.stdout.write(src)
PY
kubectl apply -f /tmp/k6-job-rendered.yaml

say "baseline, before the load lands"
kubectl get scaledobject psp-connector -n "$NS" 2>/dev/null || true
kubectl get hpa keda-hpa-psp-connector -n "$NS" 2>/dev/null || true

if [[ "$WATCH" -eq 1 ]]; then
  say "watching (Ctrl-C to stop watching; the Job keeps running)"
  "${HERE}/watch-scaling.sh"
else
  cat <<EOF

  Job started. Follow it with:
    kubectl logs -n ${NS} -f job/k6-payments
    ${HERE}/watch-scaling.sh

  Then, when the backlog has drained, watch it come back down - scale-in is governed by the HPA's
  scaleDown stabilization window (60s), not by KEDA's cooldownPeriod. See infra/k8s/README.md.
EOF
fi
