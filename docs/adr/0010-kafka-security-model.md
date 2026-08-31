# ADR-0010: Kafka security model — SASL/SCRAM per-service principals, deny-by-default ACLs

- **Status:** Accepted
- **Date:** 2026-08-31 (records the M14 decision of 2026-08-12, flagged as an open item in this
  index since 0006; M17's ACL diffs made the debt visible enough to pay)
- **Affects:** M14 (implements this), M18 (maps it onto Strimzi), M17 (extends it), every service

## Context

Until M14 the cluster trusted anyone who could reach a listener. That is the default Kafka
posture and it is indefensible even in a lab, because it makes every later security question
unanswerable: you cannot reason about "who can write to the ledger's input" when the answer is
"anything with a TCP route". 0001's prefix-friendly topic names and 0006's per-consumer DLQs
were both designed assuming per-service principals would eventually exist. M14 is where they do.

## Decision

**One SCRAM-SHA-512 principal per service**, plus `admin` and one per infrastructure client
(`connect`, `schema-registry`, `akhq`, `kafka-exporter`, `keda-scaler`). No shared application
credentials: a service's blast radius is its own grant list.

**Deny-by-default authorization** (`allow.everyone.if.no.acl.found=false`). Anything not
explicitly granted is refused - M14's proof was a forbidden write failing loudly
(`try-forbidden-write.sh`), and M19 part 1 ran every drill through these ACLs.

**Grant granularity, least privilege by kind of resource:**
- topics: **literal** grants per topic a service touches (Read/Write/Describe as needed). Kafka
  has no suffix wildcard, so `*.dlq` browsing (M17) enumerates DLQ topics individually.
- consumer groups: **prefix** grants (`ledger.` covers `ledger.v1` and `ledger.dlq-replay.v1`),
  because group names are service-owned namespaces per 0001.
- transactional ids: prefix grant only for the one EOS producer (the ledger, 0006/M7).
- cluster-level Describe only where inspection requires it (M17's ops API in realtime-gateway).

**The bootstrap exception:** SCRAM credentials live in the cluster metadata log, so the thing
that creates them cannot itself authenticate with SCRAM. The broker-internal superuser comes
from static broker config (PLAIN in compose, Strimzi's operator certs on k8s). This is the one
principal outside the model, and it exists precisely so the model can be installed.

**Two implementations, one model.** compose: imperative `kafka-init/init-security.sh` (idempotent
re-runs). Kubernetes: declarative Strimzi `KafkaUser` CRs (`infra/k8s/kafka/users/*.yaml`, one
file per principal, numbered by kind). Any ACL change lands in BOTH, in the same commit - the
k8s files are the reviewed source of truth, the script is their compose mirror.

**Transport is SASL_PLAINTEXT, not TLS - deliberately.** Both environments are single-host labs
(compose network, kind on one laptop); there is no untrusted network segment to encrypt. The
plan's "TLS" bullet is satisfied as a documented non-decision: enabling Strimzi's TLS listener
is config, not architecture, and would obscure every drill's tcpdump-ability for zero threat
reduction here. A real deployment flips the listener type and nothing else in this model.

## Consequences

- Every feature that touches a new topic ships an ACL diff - M17 was the model working as
  intended: the DLQ replay endpoints needed Read-on-own-DLQ + Write-on-input grants, the ops
  API needed Describe-everything + Read-on-DLQs, and each arrived as a reviewable two-file
  change (KafkaUser CR + init-security.sh) rather than a silent capability.
- Group-prefix grants can surprise: ledger's original literal-acting `ledger.v1` grant did NOT
  cover `ledger.dlq-replay.v1` and had to be widened - the failure was an explicit
  authorization error, which is the failure mode this ADR buys.
- The per-drill throwaway principals of M19 (`admin`-issued) bypass nothing: drills run as
  `admin` deliberately, because drills exist to break things the services must not be able to.
