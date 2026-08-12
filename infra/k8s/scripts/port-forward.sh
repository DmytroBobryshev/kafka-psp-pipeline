#!/usr/bin/env bash
# infra/k8s/scripts/port-forward.sh
#
# Reach the k8s Kafka cluster from the HOST, at the same three addresses the compose cluster used:
# localhost:29092, :29093, :29094.
#
# WHY THIS SCRIPT HAS TO EXIST, and why it forwards THREE ports and not one:
#
# A Kafka client bootstraps against any broker, gets back cluster metadata listing the leader of
# every partition by its ADVERTISED address, and then connects to those addresses directly. So
# forwarding just the bootstrap service gets you a client that connects, fetches metadata,
# and then immediately fails to reach any leader. Every broker has to be individually reachable
# at the address it advertises.
#
# The Kafka CR's `external` (cluster-ip) listener therefore overrides advertisedHost/advertisedPort
# per broker to localhost:29092/29093/29094, and this script makes those three addresses real.
#
# `cluster-ip` rather than `nodeport`, on macOS: kind's node IPs (172.22.0.x) live inside the
# Docker VM's network and are not routable from the host, so a NodePort would be allocated and
# permanently unreachable.
#
#   ./infra/k8s/scripts/port-forward.sh          # blocks; Ctrl-C to stop all three
#
# Then, with a credential from a Secret:
#   PW=$(kubectl get secret admin -n kafka -o jsonpath='{.data.password}' | base64 -d)
#   kcat -b localhost:29092 -L \
#        -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=SCRAM-SHA-512 \
#        -X sasl.username=admin -X sasl.password="$PW"
set -euo pipefail

NS="${NS:-kafka}"
CLUSTER="${CLUSTER:-psp}"

pids=()
cleanup() { echo; echo "stopping port-forwards"; for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM

# One forward per broker, local port matching the advertisedPort that broker announces.
#
# NOTE the service names. Strimzi names the per-broker services for a cluster-ip listener after
# the NODE POOL and node id - `psp-combined-0..2` - not after the listener. Only the bootstrap
# service carries the listener name (`psp-kafka-external-bootstrap`). Guessing
# `<cluster>-kafka-external-<n>` gets you "services not found"; `kubectl get svc -n kafka` is the
# authority.
POOL="${POOL:-combined}"
for i in 0 1 2; do
  local_port=$((29092 + i))
  svc="${CLUSTER}-${POOL}-${i}"
  kubectl port-forward -n "$NS" "svc/${svc}" "${local_port}:9094" >/dev/null 2>&1 &
  pids+=($!)
  echo "  localhost:${local_port}  ->  svc/${svc}:9094"
done

echo
echo "bootstrap on any of the three. Ctrl-C to stop."
wait
