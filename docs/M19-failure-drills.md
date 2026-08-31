# M19 - Operations & failure drills (part 1)

Five drills run against the live Strimzi cluster on kind: `kafka-psp` / namespace `kafka`,
Kafka 4.3.0, three combined-role nodes (`psp-combined-0..2`), KRaft, SASL/SCRAM-SHA-512,
deny-by-default ACLs.

This module has almost no code in it. The deliverable is **measured evidence** and the
explanation of what the numbers mean. Every block quoted below is real output captured during
the run; nothing is reconstructed.

| # | Drill | One-line result |
|---|---|---|
| 1 | `min.insync.replicas` 1 vs 2 under `acks=all` | 4800/4800 accepted vs 507 rejected — same failure, opposite behaviour |
| 2 | Unclean leader election off vs on | off: partition offline in 2 s. on: **27,000 acknowledged records destroyed** |
| 3 | Add partitions to a live topic | 6 of 8 merchant keys changed partition, silently |
| 4 | `RangeAssignor` vs `CooperativeStickyAssignor` | eager revokes 6/6 partitions, cooperative 2/6 — but cooperative settles 2.7 s *later* |
| 5 | Static membership (`group.instance.id`) | dynamic: survivors own nothing for 23.0 s. static: zero rebalances |

**Not covered here (part 2):** `kafka-reassign-partitions`, retention and segment rolling,
offset reset to a timestamp, node cordoning — now done, measured in
[`M19-failure-drills-part2.md`](M19-failure-drills-part2.md).

**Not possible in this cluster:** anything requiring Grafana or Prometheus. They lived in the
`infra/compose` stack, which is stopped; there is no metrics stack in the kind cluster. The lag
drill from `docs/PLAN.md` is therefore deferred, and every number below comes from the Kafka
protocol itself (AdminClient, `kafka-verifiable-*`, broker logs) rather than from a dashboard.

---

## Test harness

A single throwaway pod runs every client, so that killing a broker never kills the thing doing
the measuring:

```bash
kubectl run kafka-drill -n kafka --image=quay.io/strimzi/kafka:1.1.0-kafka-4.3.0 \
  --restart=Never --command -- sleep infinity

PW=$(kubectl get secret admin -n kafka -o jsonpath='{.data.password}' | base64 -d)
kubectl exec -n kafka kafka-drill -- bash -c "cat > /tmp/admin.properties <<EOF
bootstrap.servers=psp-kafka-bootstrap:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username=\"admin\" password=\"$PW\";
EOF"
```

Drill topics are created with `kafka-topics.sh`, **not** as `KafkaTopic` CRs. Strimzi's
Unidirectional Topic Operator only manages topics that have a CR, so a CLI-created topic is left
alone — which matters in drill 3, where the operator would otherwise fight the partition-count
change.

Measurement uses `kafka-verifiable-producer.sh` and `kafka-verifiable-consumer.sh`. Both emit
one JSON event per send / poll / rebalance callback with a millisecond timestamp, which is what
makes "how long did processing stop" answerable rather than a matter of opinion.

---

## Drill 1 - `min.insync.replicas=1` vs `=2` under `acks=all`

Extends the M3 leader-kill drill in
[`services/payment-api/README.md`](../services/payment-api/README.md), which measured 626 lost
records under `acks=1`. That drill compared `acks` settings. This one holds `acks=all` fixed and
varies only `min.insync.replicas`, because that is the setting that decides whether `acks=all`
means anything.

### Hypothesis

`acks=all` does not mean "all replicas". It means **all replicas currently in the ISR**. If the
ISR has shrunk to one member, `acks=all` is arithmetically identical to `acks=1` — the leader
alone acknowledges, and the write survives only as long as the leader's disk does.
`min.insync.replicas=2` converts that silent degradation into a loud rejection.

Prediction: with one replica down, the `min.isr=1` topic accepts every write without a murmur,
and the `min.isr=2` topic fails with `NotEnoughReplicasException` for the whole window.

### Method

Two topics identical in every respect except one config value — same replica set, same preferred
leader, so the only variable is `min.insync.replicas`:

```bash
kafka-topics.sh --create --topic drill.m19.minisr1 --replica-assignment 1:2 \
  --config min.insync.replicas=1 --config unclean.leader.election.enable=false
kafka-topics.sh --create --topic drill.m19.minisr2 --replica-assignment 1:2 \
  --config min.insync.replicas=2 --config unclean.leader.election.enable=false
```

```
Topic: drill.m19.minisr1  Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1,2
Topic: drill.m19.minisr2  Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1,2
```

Both producers run simultaneously, 40 rec/s, 4800 records (120 s), against a config chosen to
make the broker's answer visible instead of hiding it behind retries:

```properties
enable.idempotence=false
retries=0
request.timeout.ms=5000
delivery.timeout.ms=5000
```

```bash
kafka-verifiable-producer.sh --topic drill.m19.minisrN \
  --command-config /tmp/prod-loud.properties --acks -1 --throughput 40 --max-messages 4800
```

29 s into the run, **the follower** is killed:

```bash
kubectl delete pod psp-combined-2 -n kafka
```

Killing the follower rather than the leader is deliberate: no leader election happens, so the
only thing that changes is the size of the ISR. The variable stays isolated.

### Measured output

ISR five seconds after the delete:

```
isr1: Topic: drill.m19.minisr1  Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1  Elr:    LastKnownElr:
isr2: Topic: drill.m19.minisr2  Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1  Elr: 2  LastKnownElr:
```

Producer totals:

```
drill.m19.minisr1 : {"sent":4800,"acked":4800,"avg_throughput":40.00}     errors: 0
drill.m19.minisr2 : {"sent":4800,"acked":4293,"avg_throughput":35.77}     errors: 507
                       501 x org.apache.kafka.common.errors.NotEnoughReplicasException
                         6 x org.apache.kafka.common.errors.TimeoutException
```

The rejection, verbatim:

```json
{"name":"producer_send_error","topic":"drill.m19.minisr2","value":"1114",
 "exception":"class org.apache.kafka.common.errors.NotEnoughReplicasException",
 "message":"Messages are rejected since there are fewer in-sync replicas than required."}
```

Window and durability:

```
first error 1786582682721   last error 1786582690357   window = 7,636 ms
end offsets:  drill.m19.minisr1:0:4800      drill.m19.minisr2:0:4295
305 records ACKED on the min.isr=1 topic inside that same 7.6 s window
```

### What it means

The two topics received the same records, from the same producer config, at the same instant,
through the same leader. One accepted 4800 writes and one accepted 4293. The difference is one
integer in a topic config.

- **`min.isr=1` is the dangerous one.** For 7.6 seconds it accepted 305 records that existed on
  exactly one disk while telling the producer `acks=all` had been satisfied. Nothing in the
  producer's API surface distinguishes those 305 from the other 4495. There is no callback, no
  header, no metric on the client that says "this one was only replicated once". The application
  cannot tell, so it cannot compensate.
- **`min.isr=2` failed loudly and correctly.** `NotEnoughReplicas` is the broker refusing to
  accept a write it cannot make durable. That is availability being traded for durability, on
  purpose, at exactly the moment the trade matters.
- **507 rejected, not 305.** The min.isr=2 topic was unavailable for slightly longer than the
  min.isr=1 topic was degraded, because after the follower returns it must catch up before it
  re-enters the ISR, and the write is refused for that whole interval too.
- **4295 durable vs 4293 acked.** Two records reached the log but their acknowledgement never
  reached the producer (the 6 `TimeoutException`s). With `retries=0` and idempotence off, that is
  the classic ack-loss case: the producer believes it failed, the log says otherwise. Turning
  `enable.idempotence=true` back on is what makes the retry of those two records safe — the
  reason ADR-0003's global defaults pair `acks=all` with `enable.idempotence=true`.
- **`Elr: 2`** in the min.isr=2 describe output is Kafka 4.x's Eligible Leader Replicas
  (KIP-966). The controller now remembers which replicas held a complete log at the moment the
  ISR shrank. It becomes the whole story in drill 2.

The production cluster runs `min.insync.replicas=2` (`Kafka.spec.kafka.config`, see
[`infra/k8s/README.md`](../infra/k8s/README.md)). With RF=3 that survives one broker and refuses
two — which is the correct posture for a payments ledger and the reason this drill had to use
RF=2 topics to reach the interesting state with a single broker down.

> Nothing was lost in this drill: the leader never died, so the 305 single-replica writes
> survived. Drill 2 kills the leader that holds them.

---

## Drill 2 - Unclean leader election, off vs on

### Hypothesis

With `unclean.leader.election.enable=false`, a partition whose entire ISR is unavailable goes
**offline** rather than promoting a replica that is missing data. Availability is sacrificed to
preserve the meaning of an acknowledgement. Turn it on and the controller will promote an
out-of-sync replica, at which point every record the old leader acknowledged above the new
leader's log end offset ceases to exist — and the old leader, on return, *truncates its own log
to match*.

Prediction: off → `Leader: none` and a producer that cannot write. On → a leader elected in
seconds and a measurable drop in the topic's end offset.

### Method

The hard part is not the election; it is manufacturing a live-but-stale replica. Three combined
nodes form the KRaft quorum, so **only one node may be down at a time** — take two down and the
controller quorum is lost and the whole cluster stops. So the stale replica has to be alive.

Attempt one, which failed and is worth recording: replication throttling.

```bash
kafka-configs.sh --alter --entity-type topics --entity-name drill.m19.unclean \
  --add-config 'leader.replication.throttled.replicas=0:1,follower.replication.throttled.replicas=0:2'
kafka-configs.sh --alter --entity-type brokers --entity-name 2 \
  --add-config 'follower.replication.throttled.rate=1'
```

70 seconds of producing at 100 rec/s later:

```
t+5s  ... Isr: 1,2
...
t+70s ... Isr: 1,2
```

**A throttle cannot push an in-sync replica out of the ISR.** Kafka deliberately exempts
in-sync replicas from replication quotas — `shouldFollowerThrottle` requires
`!isReplicaInSync` — precisely so that an operator throttling a reassignment cannot accidentally
destroy the durability of a healthy partition. The safety property being demonstrated got in the
way of demonstrating the failure.

The throttle is still the right tool, just in the other order: knock the replica out first, then
let the throttle hold it out.

```bash
# 1. baseline: 1000 records with both replicas healthy
# 2. kill the follower, produce 24,000 records at 800/s while ISR = {1}
kubectl delete pod psp-combined-2 -n kafka --wait=false
kafka-verifiable-producer.sh --topic drill.m19.unclean --acks -1 --throughput 800 --max-messages 24000
```

The topic is `min.insync.replicas=1`, so those writes are accepted — this is drill 1's dangerous
configuration, now carried through to its conclusion.

Broker 2 restarts, is throttled to 1 byte/s, and cannot rejoin:

```
t+6s   off=...:12070  Leader: 1 Replicas: 1,2 Isr: 1
t+12s  off=...:16170  Leader: 1 Replicas: 1,2 Isr: 1
...
t+120s off=...:31000  Leader: 1 Replicas: 1,2 Isr: 1
```

Two minutes of a live broker that holds a stale copy and is not in the ISR. That is the state the
drill needed.

### Round 1 - `unclean.leader.election.enable=false` (the cluster default)

Kill the only in-sync replica:

```bash
kubectl delete pod psp-combined-1 -n kafka --wait=false
```

```
[03:08:32] BEFORE: Leader: 1 Replicas: 1,2 Isr: 1   endOffset=drill.m19.unclean:0:31000
[03:08:32] deleting psp-combined-1 -- the ONLY in-sync replica
[03:08:36] t+2s  Leader: none  Replicas: 1,2  Isr:   Elr: 1  LastKnownElr: 1
[03:08:36] >>> PARTITION OFFLINE
```

```bash
kafka-topics.sh --describe --topic drill.m19.unclean --unavailable-partitions
```
```
Topic: drill.m19.unclean  Partition: 0  Leader: none  Replicas: 1,2  Isr:   Elr: 1  LastKnownElr: 1
```

A producer aimed at it:

```json
{"name":"producer_send_error","exception":"class org.apache.kafka.common.errors.TimeoutException",
 "message":"Expiring 12 record(s) for drill.m19.unclean-0:5001 ms has passed since batch creation..."}
{"name":"tool_data","sent":12,"acked":0,"avg_throughput":0.0}
```

**Twelve sent, zero acked, zero lost.** Broker 1 returned 27 s later, was still the sole ELR, and
was cleanly re-elected. The partition had been unavailable and completely intact for the whole
outage.

### Round 2 - `unclean.leader.election.enable=true`

Same divergence re-established (kill follower, produce 27,000 records at 900/s while `Isr: 1`;
26,992 acked), then the config flipped and the in-sync replica killed again.

```
[03:11:34] acked in step 1: {"sent":27000,"acked":26992}
[03:11:35] leader end offset: drill.m19.unclean:0:58000
[03:11:37] sizes: b1:size=1022226  b2:size=755020        <-- 267 KiB of divergence
[03:11:37] STEP 2 - kill broker 1 (only ISR member) with unclean.leader.election.enable=TRUE
[03:11:40]   t+2s  Leader: 2 Replicas: 1,2 Isr: 2
[03:11:40] >>> UNCLEAN ELECTION: broker 2 (out-of-sync) is now LEADER
[03:11:41] end offset AFTER unclean election: drill.m19.unclean:0:31000
[03:11:42] sizes: b2:size=755020
```

**58,000 → 31,000. Twenty-seven thousand acknowledged records ceased to exist, in two seconds,
with no error anywhere.**

The controller's own record of it:

```
[QuorumController id=1] UNCLEAN partition change for drill.m19.unclean-0:
  isr: [1] -> [2], leader: 1 -> 2,
  leaderRecoveryState: RECOVERED -> RECOVERING, leaderEpoch: 2 -> 3, partitionEpoch: 5 -> 6
```

And broker 1, on return, destroying its own good data to agree with the new leader:

```
[Broker id=1] Follower drill.m19.unclean-0 starts at leader epoch 3 from offset 58000
              with partition epoch 6 and high watermark 58000. Current leader is 2.
[ReplicaFetcher replicaId=1, leaderId=2] Truncating partition drill.m19.unclean-0 with
              TruncationState(offset=31000, completed=true) due to leader epoch and offset
WARN [UnifiedLog partition=drill.m19.unclean-0] Truncating drill.m19.unclean-0 to offset 31000
              below high watermark 58000
```

Final state — both replicas agreeing on the shorter history:

```
broker 1  size=755020  offsetLag=0
broker 2  size=755020  offsetLag=0
drill.m19.unclean:0:31000
```

### What it means

- **`Leader: none` is a feature.** A partition with no in-sync replica available is *correctly*
  unavailable. Kafka refuses to answer the question "what is in this partition?" rather than
  answer it wrongly. Every payment write to it fails and can be retried; none of them silently
  become untrue.
- **`Truncating ... below high watermark` is the single most alarming line Kafka can log.** The
  high watermark is the acknowledgement boundary — the offset up to which Kafka has promised
  every consumer that data is committed and stable. Truncating below it is, by definition,
  deleting data that was promised. Kafka logs it at WARN and does it anyway, because that is
  exactly what the operator asked for by setting the flag.
- **Consumers had already read those records.** Any consumer that had progressed past offset
  31,000 now has a committed offset beyond the end of the log; on the next fetch it resets
  according to `auto.offset.reset`. Downstream systems keep the effects of records that no
  longer exist. In this pipeline that is a ledger entry whose originating event has vanished —
  irreconcilable by replay, because replay now produces a different history.
- **Drill 1 and drill 2 are one story.** `min.insync.replicas=1` is what allowed 27,000 records
  to be acknowledged into a single-replica ISR; `unclean.leader.election.enable=true` is what
  later threw them away. Either setting alone is survivable. Together they are a payment system
  that loses transactions and reports success.
- **The default in this cluster is correct**: `unclean.leader.election.enable=false` and
  `min.insync.replicas=2` cluster-wide. This drill had to override both, on a throwaway topic, to
  make loss possible at all.

### Honesty note

Round 1 was intended to end with a forced unclean election while the ISR member was still down,
but broker 1 came back in 27 s and was cleanly re-elected first; `kafka-leader-election.sh
--election-type UNCLEAN` replied `Valid replica already elected for partitions
drill.m19.unclean-0`. Round 2 solved it by pre-arming the topic config so the controller performed
the unclean election automatically the moment the leader was fenced — 2 seconds, no human in the
loop. That is arguably the more frightening version of the demonstration.

A second observation from round 1: when broker 1 restarted, throttled broker 2 caught up
instantly despite the 1 byte/s limit. Kafka's quotas are enforced *after* a window is exceeded,
and a leader restart resets the throttle sensors, so the follower is granted one full-size
unthrottled fetch. Replication throttles are a rate limit averaged over time, not a hard gate.

---

## Drill 3 - Add partitions to a live topic and break keyed ordering

The most important drill in this module, and the one that maps directly onto
[ADR-0003](adr/0003-partition-keys-and-counts.md), which stakes the ledger's single-writer
property on keyed ordering:

> "Partition counts can only go up, and going up breaks keyed ordering for in-flight keys."

### Hypothesis

Kafka's default partitioner sends a keyed record to
`Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions`. The key's hash is fixed; the divisor
is not. Increasing the partition count changes the divisor and therefore re-maps a large fraction
of existing keys to different partitions. Kafka orders records within a partition and nowhere
else, so the moment a merchant's key moves, that merchant's event stream is split across two
partitions with **no ordering relationship between them** — and two different consumers in the
group are now writing that merchant's balance.

Prediction, computed from the murmur2 formula *before* touching the cluster:

```
key                     n=3  n=5   moved?
merchant-acme             0    2   YES
merchant-globex           2    4   YES
merchant-initech          2    2   no
merchant-umbrella         2    0   YES
merchant-wayne            1    2   YES
merchant-stark            2    3   YES
merchant-cyberdyne        0    0   no
merchant-tyrell           1    2   YES
```

### Method

```bash
kafka-topics.sh --create --topic drill.m19.keys --partitions 3 --replication-factor 3

# 8 merchant keys x 3 records each, keys parsed from stdin
for K in merchant-acme merchant-globex ... ; do for I in 1 2 3; do
  echo "$K:{\"seq\":\"before-$I\",\"merchantId\":\"$K\"}"
done; done | kafka-console-producer.sh --topic drill.m19.keys \
  --property parse.key=true --property key.separator=:

kafka-console-consumer.sh --topic drill.m19.keys --from-beginning \
  --property print.key=true --property print.partition=true
```

### Measured output — before (3 partitions)

```
Partition:0 merchant-acme
Partition:0 merchant-cyberdyne
Partition:1 merchant-tyrell
Partition:1 merchant-wayne
Partition:2 merchant-globex
Partition:2 merchant-initech
Partition:2 merchant-stark
Partition:2 merchant-umbrella
```

Eight keys, eight stable placements, all eight matching the murmur2 prediction for `n=3`. (Note
in passing that four of the eight keys landed on partition 2 — the hot-partition skew ADR-0003
accepts by design.)

Then the capacity-management operation, which is one command and produces no warning of any kind:

```bash
kafka-topics.sh --alter --topic drill.m19.keys --partitions 5
```
```
Topic: drill.m19.keys  TopicId: IcbhPwokRZ-2AtOcxZbiug  PartitionCount: 5  ReplicationFactor: 3
```

The same eight keys are produced again.

### Measured output — after (5 partitions)

```
merchant-acme|0        before-1 before-2 before-3
merchant-acme|2        after-1  after-2  after-3       <-- moved
merchant-cyberdyne|0   after-1  after-2  after-3 before-1 before-2 before-3
merchant-globex|2      before-1 before-2 before-3
merchant-globex|4      after-1  after-2  after-3       <-- moved
merchant-initech|2     after-1  after-2  after-3 before-1 before-2 before-3
merchant-stark|2       before-1 before-2 before-3
merchant-stark|3       after-1  after-2  after-3       <-- moved
merchant-tyrell|1      before-1 before-2 before-3
merchant-tyrell|2      after-1  after-2  after-3       <-- moved
merchant-umbrella|0    after-1  after-2  after-3       <-- moved
merchant-umbrella|2    before-1 before-2 before-3
merchant-wayne|1       before-1 before-2 before-3
merchant-wayne|2       after-1  after-2  after-3       <-- moved
```

**Six of eight keys moved.** Only `merchant-cyberdyne` (0→0) and `merchant-initech` (2→2) kept
their partition, exactly as predicted. The empirical result and the arithmetic agree on all
sixteen placements.

### What it means

- **The mechanism is trivial and that is the problem.** `hash(key) % n` is a good partitioner
  and a terrible distributed-systems primitive: it is not a consistent hash, so changing `n`
  re-maps roughly `(n_new - n_old) / n_new` of the keyspace. Going 3→5 should move about 40% of
  keys; it moved 75% of this small sample, and the direction of the error is not the point — any
  non-zero fraction breaks the guarantee.
- **Nothing warns you.** `--alter --partitions` returned success and printed the new partition
  count. No broker log, no producer exception, no consumer signal. The only diagnostic Kafka
  offers is the one printed at topic creation, about periods and underscores in metric names.
- **What actually breaks in this system.** ADR-0003 keys `payments.payment-status-changed.v1`,
  `ledger.ledger-entry-recorded.v1`, `refunds.*` and the webhook topics by `merchantId`
  specifically so that one merchant's balance has a single in-flight writer. After a partition
  increase, `merchant-acme`'s history lives on partitions 0 and 2, consumed by two different
  members of `ledger.v1`, concurrently. The M7 exactly-once work assumed row contention was not
  a problem *because* of this key choice; that assumption is now false. Webhook callbacks to a
  merchant can be delivered out of order. The refund saga's per-merchant serialization is gone.
- **Records already written do not move.** Increasing partitions is not a rebalance of data;
  the existing 3 partitions keep every byte they had. So the break is permanent for the history,
  not just transitional: replaying the topic from the beginning reproduces the split.
- **The safe way to add capacity** is to create a new topic with the target partition count and
  cut producers over at a known offset boundary, or to accept the break during a maintenance
  window in which no key has in-flight state. Neither is what `--alter --partitions` looks like
  from the outside, which is why ADR-0003 treats partition count as effectively immutable and
  sizes it for the ceiling on consumer parallelism rather than for current throughput.
- **This is also the escape hatch's cost.** The composite-key idea in ADR-0003
  (`<merchantId>#<bucket>`) is the same operation performed deliberately and with the ordering
  consequences understood, on one whale merchant, instead of accidentally on all of them.

---

## Drill 4 - `RangeAssignor` vs `CooperativeStickyAssignor`

Extends the rebalance-storm section of
[`services/psp-connector/README.md`](../services/psp-connector/README.md) (M4), which measured 16
rebalances to process 8 records. That drill made rebalances happen; this one measures what a
single rebalance costs.

### Hypothesis

`RangeAssignor` uses the **eager** protocol: on any membership change every consumer invokes
`onPartitionsRevoked` for its entire assignment, stops, rejoins, and waits for a new assignment.
The group processes nothing for the duration. `CooperativeStickyAssignor` uses the **incremental**
protocol: only the partitions that actually change owner are revoked, in a first round; a second
round assigns them. Consumers keep processing everything they retain.

Prediction: eager revokes 100% of each incumbent's partitions, cooperative revokes only the
moving ones — and cooperative pays for it with an extra round trip.

### Method

`drill.m19.assignor`, 6 partitions, RF=3, a producer at 3000 rec/s. Two consumers start and
stabilise for 30 s; a third joins; everything is recorded as timestamped JSON.

```bash
kafka-verifiable-consumer.sh --topic drill.m19.assignor --group-id g-m19-range2 \
  --group-protocol classic --enable-autocommit --reset-policy latest \
  --assignment-strategy org.apache.kafka.clients.consumer.RangeAssignor
# ... and again with CooperativeStickyAssignor
```

`--group-protocol classic` is explicit because Kafka 4.x also ships the KIP-848 server-side
protocol (`--group-protocol consumer`), where `partition.assignment.strategy` no longer applies
at all and this comparison would be meaningless.

### Measured output

```
==============================================================================
RangeAssignor (EAGER)                          c3 joins a 2-member group at t=0
==============================================================================
  c1: held [3, 4, 5] before the join
      revoked [3, 4, 5]   (3/3 of what it held)
      retained without interruption: []
      revoke -> assign callback gap: 4 ms   (owns nothing during this)
      partitions_assigned t+2.63s -> [4, 5]
  c2: held [0, 1, 2] before the join
      revoked [0, 1, 2]   (3/3 of what it held)
      retained without interruption: []
      revoke -> assign callback gap: 6 ms
      partitions_assigned t+2.63s -> [0, 1]
  c3: got partitions at t+2.64s, first records at t+3.72s
  group fully settled at t+2.63s

==============================================================================
CooperativeStickyAssignor (INCREMENTAL)
==============================================================================
  c1: held [0, 2, 4] before the join
      revoked [4]         (1/3 of what it held)
      retained without interruption: [0, 2]
      partitions_assigned t+2.35s -> []          <-- round 1: revocation only
      partitions_assigned t+5.35s -> []          <-- round 2
  c2: held [1, 3, 5] before the join
      revoked [5]         (1/3 of what it held)
      retained without interruption: [1, 3]
      partitions_assigned t+2.35s -> []
      partitions_assigned t+5.35s -> []
  c3: got partitions at t+5.35s, first records at t+5.39s
  group fully settled at t+5.35s
```

| Metric | RangeAssignor | CooperativeSticky |
|---|---|---|
| Partitions revoked group-wide | **6 of 6** | **2 of 6** |
| …of which handed back to the same consumer | 4 (pure churn) | 0 |
| Incumbent partitions retained uninterrupted | 0 | 4 |
| Rebalance rounds | 1 | 2 |
| Group settled | t+2.63 s | t+5.35 s (**+2.72 s**) |
| New member consuming | t+3.72 s | t+5.39 s (**+1.67 s**) |
| Incumbent revoke→assign gap | 4 ms / 6 ms | 2 ms |

### What it means

- **The structural result is unambiguous and matches the hypothesis.** Eager revoked every
  partition from every incumbent, and then handed four of the six straight back to the consumer
  that had just given them up. Cooperative revoked exactly the two partitions that were moving.
  That 6-vs-2 ratio is the whole design difference, and it scales: at 12 partitions and 6
  consumers, eager churns 12 assignments to move 2.
- **The surprise: cooperative was measurably *slower*.** It needed two rounds, settled 2.72 s
  later, and put the new consumer to work 1.67 s later than eager did. Cooperative rebalancing is
  usually sold as "faster"; it is not. It is *less disruptive*. The total wall-clock time to
  reach a stable assignment is longer, because incremental convergence costs an extra
  JoinGroup/SyncGroup round trip. What you buy is that during that longer window, four of six
  partitions never stopped.
- **What this drill could not measure, honestly.** The incumbent stop-the-world under eager came
  out at 4–6 ms. That is a floor, not a representative number, for two reasons. First,
  `kafka-verifiable-consumer` does nothing in `onPartitionsRevoked` — no offset-commit flush, no
  database transaction to close, no state store to write. A real consumer's revocation callback
  is where the expensive work lives, and eager runs it for *every* partition while cooperative
  runs it for the ones actually moving. Second, an eager rebalance blocks until the slowest
  member reaches `poll()`; with three healthy JVMs on one node that is instant, whereas the M4
  storm drill showed what happens when a member is 10 s deep in processing — under eager, that
  10 s is the whole group's downtime.
- **Attempting to measure per-partition downtime failed and is worth recording.** Steady-state
  per-partition delivery gaps were median 8 ms and p95 10 ms, but with a tail max of 7–11 s
  caused by producer burstiness. The rebalance signal (single-digit ms) is far below that noise
  floor, so per-partition "downtime" numbers computed this way measured the load generator, not
  Kafka. The revoke/assign callback timeline is the honest instrument here; the consumption-gap
  metric is not.
- **The operational conclusion holds regardless.** `psp-connector` is KEDA-scaled from 1 to 6
  replicas on lag (M18). Every scale event is a membership change. Under eager, each one revokes
  every partition in the group; under cooperative, only the partitions being handed to the new
  pod. For a service whose entire purpose is to scale out under load, eager assignment means the
  group stops precisely when it is busiest.

---

## Drill 5 - Static membership (`group.instance.id`)

### Hypothesis

Without `group.instance.id`, a consumer that restarts is a *new* member: its old member id must
time out and its new one triggers a rebalance. With `group.instance.id` set, the coordinator
recognises the returning instance and hands it back its previous assignment without disturbing
the group — provided it returns within `session.timeout.ms`.

Prediction: dynamic → the survivors revoke everything. Static → no rebalance events at all.

### Method

Same 6-partition topic, three consumers, `session.timeout.ms=30000`. `c3` is SIGKILLed (no
graceful `LeaveGroup` in either case, so the comparison is fair) and restarted 5 s later.

```bash
kafka-verifiable-consumer.sh --topic drill.m19.assignor --group-id g-m19-static \
  --group-protocol classic --session-timeout 30000 --enable-autocommit \
  --group-instance-id psp-connector-c3        # omitted for the dynamic run
```

### Measured output

```
==============================================================================
DYNAMIC membership (no group.instance.id)
  c3 SIGKILLed at t=0; restarted at t+5.0s; session.timeout.ms=30000
==============================================================================
  c1 (survivor, held [0, 1]):
      t  +6.81s  partitions_revoked   [0, 1]
      t +29.83s  partitions_assigned  [2, 3]
  c2 (survivor, held [2, 3]):
      t  +6.81s  partitions_revoked   [2, 3]
      t +29.83s  partitions_assigned  [4, 5]
  c3: held [4, 5] before the kill  ->  came back with [0, 1]

  c1: owned NOTHING for 23.02s; actual consumption halt 31.48s
  c2: owned NOTHING for 23.02s; actual consumption halt 24.21s

==============================================================================
STATIC membership (group.instance.id set)
  c3 SIGKILLed at t=0; restarted at t+5.0s; session.timeout.ms=30000
==============================================================================
  c1 (survivor, held [0, 1]):  NO REBALANCE EVENTS AT ALL after the restart
  c2 (survivor, held [2, 3]):  NO REBALANCE EVENTS AT ALL after the restart
  c3: t-20.34s [4, 5]  ->  t+6.01s [4, 5]     same partitions, straight back to work
```

### What it means

- **23.02 seconds of a three-member group owning nothing, caused by restarting one pod.** That
  is the headline. The survivors revoked at t+6.81 s — when the restarted `c3` sent JoinGroup
  with a fresh member id — and could not be given a new assignment until t+29.83 s, because the
  coordinator was still waiting out the *dead* member's 30 s session before it could complete the
  round. The restart cost the group roughly `session.timeout.ms` of total downtime, not the 5
  seconds the pod was actually gone.
- **Static membership removed it entirely.** Not reduced: removed. Zero revocation callbacks on
  either survivor, and `c3` resumed partitions `[4, 5]` — the exact set it held before — 6.01 s
  after being killed, of which ~5 s was the deliberate restart delay and ~1 s was JVM start plus
  a `JoinGroup` that the coordinator answered from memory.
- **Assignment stability is the second prize.** In the dynamic run `c3` came back holding
  `[0, 1]` instead of `[4, 5]`, and both survivors were handed partitions they had never seen.
  Every one of those moves invalidates whatever local state the consumer had built for its old
  partitions — dedup caches, Streams RocksDB stores, in-flight batches. Static membership makes
  a restart a no-op for assignment, so warm state stays warm.
- **Why this matters specifically on Kubernetes.** A rolling deployment restarts *every* pod in
  the Deployment, one at a time, by design. With N replicas and dynamic membership, a single
  `kubectl rollout restart` costs N rebalances — and the measurement above says each one can cost
  the whole group tens of seconds. A 6-replica `psp-connector` rollout is therefore not "six pods
  restarting"; it is six group-wide stalls of up to `session.timeout.ms` each, during a window
  when the operator believes they are performing a zero-downtime deploy. Static membership turns
  the same rollout into six pods that leave and return to their own partitions with the group
  never noticing.
- **The constraint to respect.** `group.instance.id` must be **stable and unique per instance**,
  which means it must come from the StatefulSet ordinal or the pod name, not from a random UUID
  generated at startup — a duplicate id causes the coordinator to fence one of the two instances.
  And the restart must complete inside `session.timeout.ms`: exceed it and the coordinator
  evicts the instance and you get the dynamic behaviour anyway, so the timeout has to be sized
  against realistic pod start time (JVM boot, readiness probe, image pull on a cold node), not
  against the happy path. That is the trade: a genuinely dead consumer is now not detected for
  `session.timeout.ms`, and its partitions sit unconsumed for that long.

---

## Cluster state after the drills

```
16/16 application and Kafka pods Running
NAME   READY   WARNINGS   KAFKA VERSION   METADATA VERSION
psp    True               4.3.0           4.3-IV0

kafka-topics.sh --describe --under-replicated-partitions   -> (empty)
kafka-topics.sh --describe --unavailable-partitions        -> (empty)
```

End-to-end verified after all four broker kills:

```
POST /api/payments -> 201 {"id":"7f6bf24b-...","merchantId":"merchant-acme","status":"CREATED"}
psp-connector : Consumed payment-requested paymentId=7f6bf24b-...
psp-connector : Provider call ... latencyMs=3325 outcome=APPROVED
psp-connector : Published payments.payment-status-changed.v1 ... status=SUCCEEDED
ledger        : Consumed payment-status-changed ... status=SUCCEEDED
```

### Leftovers, deliberate

The drill topics are kept, holding the evidence quoted above — the same convention M3 and M4
used for `drill.acks1-loss`, `drill.acksall`, `drill.consumer-ratio` and `drill.dup-vs-loss`.
Total footprint is **22.89 MiB across all replicas**.

| Topic | Partitions | RF | Non-default config | Holds |
|---|---|---|---|---|
| `drill.m19.minisr1` | 1 | 2 (replicas 1,2) | `min.insync.replicas=1` | 4,800 records, 305 of them accepted into a single-replica ISR |
| `drill.m19.minisr2` | 1 | 2 (replicas 1,2) | `min.insync.replicas=2` | 4,295 records; 507 rejected writes are the result |
| `drill.m19.keys` | **5** (was 3) | 3 | — | the broken keyed ordering, replayable |
| `drill.m19.unclean` | 1 | 2 (replicas 1,2) | `min.insync.replicas=1` | 31,000 records; 27,000 more were acknowledged and are gone |
| `drill.m19.assignor` | 6 | 3 | — | ~20 MiB of rebalance-drill traffic |

Reverted, because leaving them would have been a live hazard:

- `unclean.leader.election.enable` deleted from `drill.m19.unclean` (back to the cluster default
  `false`).
- All replication throttles removed — `leader.replication.throttled.replicas` and
  `follower.replication.throttled.replicas` from the topic, and
  `leader/follower.replication.throttled.rate` from brokers 1 and 2. `kafka-configs --describe
  --entity-type brokers` returns empty for all three brokers.
- Drill consumer groups `g-m19-{range,coop,range2,coop2,dyn,static}` deleted.

The `kafka-drill` client pod **was deleted** after the run, so the namespace is back to the same
16 pods it had before. Recreate it with the two commands in [Test harness](#test-harness) when
running part 2.

To remove the drill topics as well:

```bash
kubectl run kafka-drill ...            # see "Test harness" above
for T in minisr1 minisr2 keys unclean assignor; do
  kubectl exec -n kafka kafka-drill -- bin/kafka-topics.sh \
    --bootstrap-server psp-kafka-bootstrap:9092 --command-config /tmp/admin.properties \
    --delete --topic drill.m19.$T
done
kubectl delete pod kafka-drill -n kafka
```

---

## What the five drills add up to

Three of them are the same bug wearing different clothes: **an operation that looks routine,
succeeds, and destroys a guarantee the system depends on, without producing an error.** Setting
`min.insync.replicas=1` looks like a durability tweak. `--alter --partitions 5` looks like
capacity planning. `unclean.leader.election.enable=true` looks like an availability improvement.
Each one is a config change a competent operator might make on a Tuesday, and each one silently
converts an acknowledged write into something that might not be true.

The other two are about the cost of *coordination*: a group of consumers cannot change shape
without agreeing to, and both drills measure what that agreement costs. Drill 4 shows the price
of the assignment protocol, drill 5 the price of not telling the coordinator who you are. Drill 5
is much the larger number — 23 seconds against milliseconds — which is the useful ordering: fix
membership identity before optimising the assignor.

The cluster's shipped defaults (`min.insync.replicas=2`, `unclean.leader.election.enable=false`,
RF=3, `acks=all`, `enable.idempotence=true`) survive all of this untouched. Every drill above had
to override one of them, on a throwaway topic, to make anything bad happen at all.
