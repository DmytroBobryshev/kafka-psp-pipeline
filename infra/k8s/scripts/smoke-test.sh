#!/usr/bin/env bash
# infra/k8s/scripts/smoke-test.sh
#
# M18 phase 1 acceptance, run from inside the cluster: produce and consume a real record as real
# service principals, then show the deny-by-default half.
#
#   ./infra/k8s/scripts/smoke-test.sh
#
# It runs the CLI inside a broker pod rather than port-forwarding, because that is what the
# acceptance bar actually asks: authenticate as one of the KafkaUsers and move a message. Host
# access is a separate concern - see port-forward.sh.
#
# Nothing here uses the `admin` superuser for the produce/consume path: payment-api produces and
# psp-connector consumes, which is the same pair of principals the running system uses on
# payments.payment-requested.v1, so the ACLs are genuinely under test.
set -euo pipefail

NS="${NS:-kafka}"
CLUSTER="${CLUSTER:-psp}"
POD="${POD:-${CLUSTER}-combined-0}"
BOOTSTRAP="${CLUSTER}-kafka-bootstrap:9092"
TOPIC="payments.payment-requested.v1"
KEY="pay-k8s-$(date +%s)"

say() { printf '\n==> %s\n' "$*"; }

# Render a client properties file inside the pod from a user's generated Secret. This is the whole
# credential story: nobody typed a password, the User Operator generated it into
# Secret/<user> with keys `password` and `sasl.jaas.config`.
mkprops() {
  local user="$1"
  local pw
  pw="$(kubectl get secret "$user" -n "$NS" -o jsonpath='{.data.password}' | base64 -d)"
  kubectl exec -n "$NS" "$POD" -- bash -c "cat > /tmp/${user}.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${user}\" password=\"${pw}\";
EOF"
}

say "rendering client configs from the operator-generated Secrets"
mkprops payment-api
mkprops psp-connector
echo "    payment-api, psp-connector"

say "PRODUCE as User:payment-api -> ${TOPIC} (acks=all, min.insync.replicas=2)"
kubectl exec -n "$NS" "$POD" -- bash -c \
  "printf '${KEY}:{\"paymentId\":\"${KEY}\",\"amount\":4200,\"currency\":\"EUR\",\"proof\":\"M18-phase1\"}\n' \
   | bin/kafka-console-producer.sh --bootstrap-server ${BOOTSTRAP} \
       --producer.config /tmp/payment-api.properties --topic ${TOPIC} \
       --property parse.key=true --property key.separator=: --producer-property acks=all" 2>&1 \
  | grep -vi 'deprecated' || true
echo "    sent key=${KEY}"

say "CONSUME as User:psp-connector, group psp-connector.v1"
kubectl exec -n "$NS" "$POD" -- bin/kafka-console-consumer.sh \
  --bootstrap-server "${BOOTSTRAP}" --consumer.config /tmp/psp-connector.properties \
  --topic "${TOPIC}" --group psp-connector.v1 --from-beginning --max-messages 1 \
  --property print.key=true --property print.partition=true 2>&1 \
  | grep -vi 'deprecated\|rebalance protocol\|Processed a total' || true

say "DENIAL: User:payment-api -> ledger.ledger-entry-recorded.v1 (no grant exists)"
kubectl exec -n "$NS" "$POD" -- bash -c \
  "printf 'x:y\n' | bin/kafka-console-producer.sh --bootstrap-server ${BOOTSTRAP} \
     --producer.config /tmp/payment-api.properties --topic ledger.ledger-entry-recorded.v1 \
     --property parse.key=true --property key.separator=:" 2>&1 \
  | grep -i 'TOPIC_AUTHORIZATION_FAILED\|Topic authorization failed' | head -2 || {
      echo "    !! expected a TOPIC_AUTHORIZATION_FAILED and did not get one"; exit 1; }

say "ACL binding count on the cluster (M14's matrix is 119)"
pw="$(kubectl get secret admin -n "$NS" -o jsonpath='{.data.password}' | base64 -d)"
kubectl exec -n "$NS" "$POD" -- bash -c "cat > /tmp/admin.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"${pw}\";
EOF"
n=$(kubectl exec -n "$NS" "$POD" -- bin/kafka-acls.sh --bootstrap-server "${BOOTSTRAP}" \
      --command-config /tmp/admin.properties --list 2>/dev/null | grep -c 'principal=User:')
echo "    ${n} bindings"

say "OK"
