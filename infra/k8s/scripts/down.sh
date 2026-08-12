#!/usr/bin/env bash
# infra/k8s/scripts/down.sh
#
# Tear down, at one of three depths. Nothing here is run by up.sh.
#
#   ./down.sh workload   - delete the Kafka cluster, topics, users. PVCs SURVIVE (deleteClaim:
#                          false on the node pool), so `up.sh` brings the same data back.
#   ./down.sh namespace  - the above plus the PVCs, plus the operator. The kind cluster survives.
#   ./down.sh cluster    - delete the kind cluster entirely.
#
# The default is `workload` because it is the only one that is reversible.
set -euo pipefail

MODE="${1:-workload}"
KIND_CLUSTER="kafka-psp"

case "$MODE" in
  workload)
    # Order matters: deleting the Kafka CR first would take the entity operator with it, and the
    # KafkaTopic/KafkaUser finalizers would then have nothing to run against and would hang.
    kubectl delete kafkatopic --all -n kafka --ignore-not-found
    kubectl delete kafkauser  --all -n kafka --ignore-not-found
    kubectl delete kafka psp -n kafka --ignore-not-found
    kubectl delete kafkanodepool combined -n kafka --ignore-not-found
    echo "PVCs kept:"; kubectl get pvc -n kafka
    ;;
  namespace)
    kubectl delete namespace kafka --ignore-not-found
    helm uninstall strimzi-cluster-operator -n strimzi-system 2>/dev/null || true
    kubectl delete namespace strimzi-system --ignore-not-found
    # Helm does NOT remove CRDs it installed. Left in place on purpose - deleting a CRD deletes
    # every CR of that kind cluster-wide, which is not something a teardown script should do
    # quietly. Remove by hand if you mean it:
    #   kubectl get crd -o name | grep strimzi.io | xargs kubectl delete
    echo "Strimzi CRDs left in place (see the comment in this script)."
    ;;
  cluster)
    kind delete cluster --name "${KIND_CLUSTER}"
    ;;
  *)
    echo "usage: $0 [workload|namespace|cluster]" >&2; exit 2 ;;
esac
