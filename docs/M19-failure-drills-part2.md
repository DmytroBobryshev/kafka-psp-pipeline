# M19 - Operations & failure drills (part 2)

Continuation of [part 1](M19-failure-drills.md), against the same cluster: `kafka-psp` /
namespace `kafka`, Kafka 4.3.0, three combined-role nodes (`psp-combined-0..2`), KRaft,
SASL/SCRAM-SHA-512, deny-by-default ACLs. The `kafka-drill` client pod was recreated with the
two commands from part 1's [Test harness](M19-failure-drills.md#test-harness); drills 6-9 run
inside it. Drill 10 is the exception — it needs no Kafka client at all, only `kubectl`, `helm`
and Prometheus's HTTP API from the host.

Same rule as part 1: the deliverable is **measured evidence**. Every block quoted below is real
output captured during the run; nothing is reconstructed.

| # | Drill | One-line result |
|---|---|---|
| 6 | `kafka-reassign-partitions`, throttled vs not | 61.4 MiB moved in 62-67 s at a 1 MiB/s throttle, in ≤3 s without one — and the throttle is a config, not a lease |
| 7 | `retention.bytes` vs `retention.ms`, segment rolling | asked for 5 MiB, kept 5.9 — size retention is a floor; asked for 60 s, got **everything deleted including the active segment**, 298 s late |
| 8 | Offset reset to a timestamp | a datetime in a 20 s gap between batches landed the group on offset 1000, exactly — after two refusals worth knowing: active groups and the dry-run default |
| 9 | Chaos: cordon + broker kill, ledger kill mid-transaction | nothing was lost — 6000/6000 through a broker outage, EOS held through a `--grace-period=0` kill, and the "3 lost payments" turned out to be **designed TIMEOUTs, a false alarm** — which nevertheless surfaced a real latent loss window in the non-transactional hop, since fixed and regression-proven |
| 10 | The deferred lag drill, on a real metrics stack | peak lag **2,396** records, KEDA 1 → 3 → 5 → 6 in 72 s, drained at **2.3 rec/s** (0.38/pod × 6, exactly linear) — 3 min of load took **19 min** to clear, and the first dashboard render exposed **five orphaned consumer groups** carrying more lag than the service under test |

Part 1 opened by saying a lag drill was **not possible in this cluster** — no Prometheus, no
Grafana, nothing to plot. Drill 10 is that sentence retracted: the metrics stack now exists on
Kubernetes ([`infra/k8s/README.md` → Monitoring](../infra/k8s/README.md#monitoring-prometheus--grafana-on-the-cluster-m19-revisit)),
and the drill deferred in `docs/PLAN.md` has been run and measured.

---

## Drill 6 - `kafka-reassign-partitions`: move a partition across brokers

Every drill so far changed *what the cluster believes* (ISR membership, leadership, partition
count). This one changes *where the bytes physically live*, which is the operation a real
cluster runs when a broker fills its disk or a new broker joins and owns nothing. The questions
worth measuring: does moving a replica interrupt clients, how precisely does the replication
throttle obey the number it is given, and what does the tool leave behind.

### Hypothesis

Reassignment is copy-then-switch: the partition's replica set temporarily becomes the union of
old and new, the added replica catches up as a follower, and the swap at the end is atomic from
a client's point of view — so producing into the partition mid-move should work uninterrupted.

Predictions: moving a 60 MiB replica under a 1,048,576 B/s throttle takes ~60 s, arithmetically;
the same move unthrottled takes seconds; the removed replica's data is deleted, so a rollback is
a full re-copy, not a cheap flip.

### Method

A topic with one partition, RF=2, pinned to brokers 0 and 1, loaded with 100,000 x 1 KiB records:

```bash
kafka-topics.sh --create --topic drill.m19.reassign --replica-assignment 0:1 \
  --config min.insync.replicas=1
kafka-producer-perf-test.sh --topic drill.m19.reassign --num-records 100000 \
  --record-size 1024 --throughput -1 --producer-props acks=all
```

```
100000 records sent, 24588.148512 records/sec (24.01 MB/sec), 1010.89 ms avg latency
```

97.7 MiB on the wire becomes 60.2 MiB on disk — the cluster default `compression.type=zstd`
squeezes the perf-test payload by ~40%. `kafka-log-dirs.sh` confirms both replicas:

```
broker 0: drill.m19.reassign-0 size=60.2 MiB
broker 1: drill.m19.reassign-0 size=60.2 MiB
```

The move is `[0,1] -> [1,2]`: broker 2 must fetch the full log, broker 0 must drop its copy,
broker 1 stays. The plan is hand-written JSON (the `--generate` flow produces the same shape):

```json
{"version":1,"partitions":[{"topic":"drill.m19.reassign","partition":0,
  "replicas":[1,2],"log_dirs":["any","any"]}]}
```

```bash
kafka-reassign-partitions.sh --reassignment-json-file /tmp/reassign-12.json \
  --execute --throttle 1048576
```

Progress is polled every ~5 s with `--list` and `kafka-topics.sh --describe`; 14 s into the
move, a producer fires 2,000 more records at the partition with `acks=all` to test availability.

### Measured output — throttled move

The execute output itself says three things worth keeping (started `22:40:59`):

```
Save this to use as the --reassignment-json-file option during rollback
Warning: You must run --verify periodically, until the reassignment completes,
  to ensure the throttle is removed.
The inter-broker throttle limit was set to 1048576 B/s
Successfully started partition reassignment for drill.m19.reassign-0
```

What `--throttle` actually did — two config surfaces, written by the tool:

```
topic drill.m19.reassign:
  leader.replication.throttled.replicas=0:0,0:1     # partition 0 on brokers 0,1 (the old replicas)
  follower.replication.throttled.replicas=0:2       # partition 0 on broker 2 (the one catching up)
brokers 0, 1, 2 — each:
  leader.replication.throttled.rate=1048576
  follower.replication.throttled.rate=1048576
```

During the move, the replica set is visibly the union, with the direction of travel spelled out:

```
[22:41:08] Leader: 0  Replicas: 1,2,0  Isr: 0,1  Adding Replicas: 2  Removing Replicas: 0
```

The mid-move produce probe, against a partition whose log was being copied at that moment:

```
2000 records sent, 5847.95 records/sec (5.71 MB/sec), 142.95 ms avg latency, 268.00 ms max
```

Zero errors, every record acked. Then, between the `22:41:19` and `22:41:25` polls, leadership
left the departing broker **mid-move**, long before broker 2 finished catching up:

```
[22:41:19] Leader: 0  Replicas: 1,2,0  Isr: 0,1  Adding Replicas: 2  Removing Replicas: 0
[22:41:25] Leader: 1  Replicas: 1,2,0  Isr: 0,1  Adding Replicas: 2  Removing Replicas: 0
```

Completion lands between the `22:42:01` and `22:42:06` polls:

```
[22:42:01] list: drill.m19.reassign-0: replicas: 1,2,0. adding: 2. removing: 0.
[22:42:06] list: No partition reassignments found.
[22:42:06] Leader: 1  Replicas: 1,2  Isr: 1,2
```

**62-67 s from execute to completion.** The log had grown to 61.4 MiB (the probe's 2,000
records, zstd'd); 61.4 MiB at 1 MiB/s is 61.4 s — the throttle honoured its number to within
the polling interval. Then `--verify`:

```
Reassignment of partition drill.m19.reassign-0 is completed.
Clearing broker-level throttles on brokers 0,1,2
Clearing topic-level throttles on topic drill.m19.reassign
```

`kafka-configs.sh --describe` afterwards: no throttle config left on the topic or on any broker.
And broker 0's copy is gone — `kafka-log-dirs.sh` now shows only:

```
broker 1: size=61.4 MiB
broker 2: size=61.4 MiB
```

### Measured output — the same move back, unthrottled

Same JSON with `replicas:[0,1]`, `--execute` without `--throttle`. Broker 0's replica was
deleted, so this is a full 61.4 MiB re-copy, not an incremental catch-up:

```
=== EXECUTE 22:43:11 ===
[22:43:14] list: No partition reassignments found.
[22:43:14] Leader: 1  Replicas: 0,1  Isr: 0,1
```

**Complete in ≤3 s** — the first poll after execute already found nothing in flight. Against
62-67 s throttled, that is the whole argument for the throttle in one pair of numbers: left
alone, replication traffic moves at whatever the disks and the network will give it (≥20 MiB/s
here, on a laptop's kind cluster), and on a production broker that bandwidth comes out of the
same budget as live traffic.

One thing did *not* come back on its own:

```
[22:43:14] Leader: 1  Replicas: 0,1  Isr: 0,1     # preferred leader is 0, actual leader is 1
```

The outbound move switched leadership away from broker 0 because broker 0 was being removed —
it had no choice. The return move removed nobody that held leadership, so nothing forced an
election, and the partition ends fully healthy but leader-imbalanced. Restoring that is a
separate, explicit operation:

```bash
kafka-leader-election.sh --election-type preferred --path-to-json-file /tmp/elect.json
```
```
Successfully completed leader election (PREFERRED) for partitions drill.m19.reassign-0
Leader: 0  Replicas: 0,1  Isr: 0,1
```

### What it means

- **Clients never noticed.** 2,000 records at `acks=all`, 143 ms average latency, produced into
  a partition mid-copy; leadership changed hands twice across the drill and no request failed.
  Reassignment is designed to be invisible to clients, and measurably is — the availability risk
  of a move is not the move itself, it is the *bandwidth* the move consumes.
- **The throttle does exactly what it says, which is why it is dangerous.** 61.4 MiB / 1 MiB/s
  = 61.4 s predicted, 62-67 s measured. But look at what it is made of: plain dynamic configs —
  a rate on the brokers, a replica list on the topic. Nothing expires them. The tool's own
  warning ("You must run --verify... to ensure the throttle is removed") is the tell: skip
  `--verify` and the throttle stays forever, silently capping **all** replication for those
  replicas — including ISR catch-up after the next broker restart, which is the moment the
  cluster can least afford it. That is the same shape as part 1's drills 1-3: a routine
  operation that succeeds and leaves a guarantee quietly broken. This one at least prints a
  warning.
- **Removal deletes data, so a rollback is a re-copy.** Broker 0 held 61.4 MiB before the move
  and zero after it; bringing the replica back cost the full transfer again. "Undo" of a
  reassignment is not free, and the execute output handing you a rollback JSON does not change
  the physics.
- **Leadership is placement's poor cousin.** The reassignment API guarantees where replicas
  live, not who leads. One direction of the move happened to fix leadership (forced, the leader
  was leaving), the other direction left it wrong. After any reassignment campaign, leader
  balance needs its own pass — `kafka-leader-election.sh --election-type preferred` — or the
  cluster serves all traffic from wherever leaders happened to land.
- **Throttle scope is coarser than it looks.** The rate limit was written to all three brokers,
  and `*.throttled.rate` is per-broker, not per-partition: any other reassignment running at the
  same time shares the same 1 MiB/s pipe. Kafka 4.3 still ships the classic throttle; there is
  no per-move budget.

### Leftovers

`drill.m19.reassign` is kept (61.4 MiB x 2 replicas, back on brokers 0,1 with leader 0, as it
started). All throttles were cleared by `--verify` — confirmed empty on topic and all three
brokers. Delete the topic with the same loop as part 1's leftovers section.

---

## Drill 7 - `retention.bytes` vs `retention.ms`, and what a segment really is

Retention is the config everyone sets and nobody watches happen. The claim under test: Kafka
never deletes *records*, it deletes *segments* — whole files — so every retention promise is
quantised to segment boundaries and delayed by the cleaner's schedule. The cluster does not
override `log.retention.check.interval.ms`, so the checker runs on the default **every 300 s**,
and that number is about to be visible in the data.

### Hypothesis

With `segment.bytes=1048576`, 10,000 x 1 KiB records should land in ~10 segment files. Setting
`retention.bytes=5242880` (5 MiB) then deletes whole old segments but never cuts below the
threshold — the survivor is *more* than 5 MiB, not less. Setting `retention.ms=60000` on
minutes-old data deletes everything it can, with one open question worth an honest experiment:
does the *active* segment — the file currently open for writes — survive, leaving the last
partial segment forever, or does the broker roll it just to delete it?

Both enforcement events should lag the config change by up to 300 s, because a config change
does not trigger a retention pass; the scheduler's clock does.

### Method

```bash
kafka-topics.sh --create --topic drill.m19.retention --replica-assignment 1:2 \
  --config segment.bytes=1048576 --config compression.type=producer \
  --config min.insync.replicas=1
kafka-producer-perf-test.sh --topic drill.m19.retention --num-records 10000 \
  --record-size 1024 --throughput -1 --producer-props acks=all
```

`compression.type=producer` overrides the cluster's `zstd` so the perf-test's uncompressed
batches hit disk at face value and the byte arithmetic stays legible. State is polled every
~15 s: earliest/latest offsets via `kafka-get-offsets.sh`, the segment files via `ls` inside
the leader's pod (`psp-combined-1`), sizes via `du`. Then two config changes in sequence, each
followed by watching until something happens:

```bash
kafka-configs.sh --alter --entity-name drill.m19.retention --add-config retention.bytes=5242880
# ... watch ...
kafka-configs.sh --alter --entity-name drill.m19.retention --delete-config retention.bytes
kafka-configs.sh --alter --entity-name drill.m19.retention --add-config retention.ms=60000
# ... watch ...
```

### Measured output — segment rolling

10,000 records became exactly ten files in the leader's log dir:

```
1042252  00000000000000000000.log
1042252  00000000000000001005.log
1042252  00000000000000002010.log
...
1042252  00000000000000008040.log
 990419  00000000000000009045.log
```

The file name is the **base offset** of the segment. 1,005 records x (1,024 B + batch overhead)
fits under `segment.bytes=1 MiB`; the append that would cross the line triggers the roll, so
every closed segment is 1,042,252 bytes and only the active one is ragged. One honesty note on
`du`: the directory reported ~31 MB against ~10.4 MB of `.log` data — the balance is the active
segment's preallocated `.index`/`.timeindex` (10 MiB each, trimmed on roll) plus, later,
`*.deleted` files awaiting their final unlink. The `.log` listing is the source of truth;
`du` on a Kafka log dir routinely lies by 2-3x.

### Measured output — `retention.bytes=5242880`

Config applied at `23:01:15`. Three polls of nothing, then:

```
[23:01:36] earliest=0     latest=10000  segments=10
[23:01:53] earliest=0     latest=10000  segments=6      <- files renamed *.deleted
[23:02:11] earliest=4020  latest=10000  segments=6
>>> size retention enforced after 55 s
```

Segments 0, 1005, 2010, 3015 died; 4020 onward survived. Do the arithmetic the broker did:
remaining data was 6,201,679 bytes — **5.9 MiB survives a 5 MiB limit**. Deleting one more
segment would have left 5,159,427 < 5,242,880, so the broker stopped. The predicate is "delete
a whole segment while what remains is still ≥ `retention.bytes`" — the config is a floor, not a
ceiling, and the worst-case disk usage of a partition is `retention.bytes + segment.bytes`, not
`retention.bytes`.

Deletion itself is two-phase, and both phases are visible above: at `23:01:53` the files are
already renamed `*.deleted` (the `ls` count dropped) while `du` still charges for them; the
physical unlink lands ~60 s later (`file.delete.delay.ms`, its own default). The broker log
names all four in one line:

```
Deleting segment files LogSegment(baseOffset=0, ...), LogSegment(baseOffset=1005, ...),
  LogSegment(baseOffset=2010, ...), LogSegment(baseOffset=3015, ...)
```

### Measured output — `retention.ms=60000`

Config applied at `23:02:11`, on data all older than a minute — every byte in the topic
instantly in breach. What followed was the most instructive nothing of the module:

```
[23:02:16] earliest=4020  latest=10000  segments=6
[23:03:08] earliest=4020  latest=10000  segments=6
[23:04:34] earliest=4020  latest=10000  segments=6
[23:06:36] earliest=4020  latest=10000  segments=6
[23:06:53] earliest=4020  latest=10000  segments=1
[23:07:11] earliest=10000 latest=10000  segments=1
>>> time retention emptied the log after 298 s
```

**298 s of breach with zero enforcement** — the checker's 300 s schedule, measured almost
exactly. Then, in a single pass, the broker log (UTC timestamps, +2 h to the poll times) answers
the hypothesis's open question:

```
21:06:52 Rolled new log segment at offset 10000 in 0 ms.
21:06:52 Incremented log start offset to 10000 due to segment deletion
21:06:52 Deleting segment LogSegment(baseOffset=4020, ...) due to log retention time 60000ms
           breach based on the largest record timestamp in the segment
   ... (5 more, through baseOffset=9045)
```

The broker **rolled the active segment specifically so it could delete it**. What remains is a
single zero-byte file:

```
0  00000000000000010000.log
```

So the two retention axes are not symmetric: `retention.bytes` can never empty a partition (it
stops at the last segment boundary above the floor), `retention.ms` can and did. Note also the
eligibility rule in the log line: "based on the **largest record timestamp** in the segment" —
retention time is data time, not file mtime. A segment is deletable only when its *newest*
record breaches, which is why a segment holding one fresh record keeps every stale record
around it alive.

And offsets are forever: 100 more records produced after the wipe landed at offsets
10000-10099, in a new segment named `00000000000000010000.log` — 103,727 bytes. Deletion moves
the log start offset; it never rewinds anything.

### What it means

- **Retention promises are quantised twice.** Once in space — whole segments only, so real
  disk floor is `retention.bytes + segment.bytes` per partition — and once in time — nothing
  happens between cleaner passes, so add up to `log.retention.check.interval.ms` (300 s
  default) to every SLA. "retention.ms=60000" actually means "somewhere between 60 and 360
  seconds, rounded up to segment granularity".
- **`segment.bytes` is the resolution knob for both axes.** The default is 1 GiB. On a
  low-traffic topic, a 1 GiB active segment can take *weeks* to roll, and until it rolls,
  time-based retention cannot touch anything in it (its largest record timestamp keeps
  advancing with each fresh write). That is the classic "why is my 7-day topic holding a month
  of data" — the answer is one unrolled segment, not a bug. The M13 windowed-analytics topics
  in this project would show exactly this if their retention were ever tightened.
- **Size retention cannot empty a log, time retention can.** The active-segment roll at
  21:06:52 is the proof for the time side; the 5.9-MiB survivor is the proof for the size side.
  If a runbook needs a topic actually emptied, `retention.ms=small` (then restored) works where
  `retention.bytes=small` silently would not — and `kafka-delete-records.sh` does it without
  the timing lottery.
- **The delete is two renames and a delay, not an unlink.** `*.deleted` files linger for
  `file.delete.delay.ms` (60 s), which means disk does not come back the moment the offsets
  move. Monitoring that alerts on `du`-style disk numbers will disagree with
  `kafka-log-dirs.sh` for a minute after every retention pass. Neither is lying; they are
  measuring different stages of the same funnel.
- **This is the quiet member of part 1's family.** No error, no warning, nothing rejected —
  retention config changes always "succeed" instantly and act later, on the scheduler's clock,
  at segment granularity. The gap between what the config says and what the disk does is where
  the surprises live, on both axes, in both directions: keeping more than you asked (5.9 vs 5
  MiB) and deleting more than you expected (the active segment included).

The production topics in this cluster all ride the cluster default `log.retention.ms=604800000`
(7 days) with default 1 GiB segments — which, given the traffic volumes here, means retention
in this project is enforced by segment *count* never reaching a roll at all. The drill topics
from part 1 are the only ones that ever grew enough to roll a segment naturally.

### Leftovers

`drill.m19.retention` is kept, but after the time-retention wipe it holds only the 100
post-wipe records (offsets 10000-10099, ~104 KB): the drill destroyed its own evidence, which
is rather the point. `retention.bytes` and `retention.ms` were both removed from the topic —
`kafka-configs.sh --describe` shows only the creation-time configs — so it is back on the
cluster's 7-day default.

---

## Drill 8 - Offset reset to a timestamp

The scenario this rehearses is concrete: a consumer deployed at 14:00 had a bug, processed
three hours of events wrongly, and the fix ships at 17:00. Replaying from `--from-beginning`
reprocesses days of data; replaying from "where it happens to be" skips the damage. The tool
for "rewind to 14:00 exactly" is `kafka-consumer-groups.sh --reset-offsets --to-datetime`, and
this drill measures how exact it is — plus the two refusals standing between an operator and a
successful reset.

### Hypothesis

Kafka can resolve a timestamp to an offset because every segment carries a `.timeindex` (drill
7 showed them being preallocated and deleted). The resolution rule should be: **the first
offset whose record timestamp is ≥ the target**. So with three 1,000-record batches produced
20 s apart, a target datetime inside the first gap should land the group at offset exactly
1000 — the first record of batch 2, no scanning, no approximation.

Also expected: the reset refuses to touch an active group, and the tool without `--execute`
does nothing (a dry run is the default) — both worth capturing verbatim.

### Method

Topic `drill.m19.tsreset` (RF=2, brokers 1,2). Three batches of 1,000 records
(`batch1-1`...`batch3-1000`) via `kafka-console-producer.sh`, with 20 s pauses, the pod clock
recorded around each batch (all times UTC):

```
batch1: 1787936403436 .. 1787936404645     (17:00:03.4 .. 17:00:04.6)
batch2: 1787936424743 .. 1787936425976     (17:00:24.7 .. 17:00:25.9)
batch3: 1787936446077 .. 1787936447267     (17:00:46.0 .. 17:00:47.2)
```

Group `g-m19-tsreset` consumes all 3,000 and commits: `CURRENT-OFFSET 3000, LAG 0`. The reset
target is the middle of the first gap — `2026-08-28T17:00:14.694` (epoch 1787936414694) — a
moment at which **no record was ever produced**.

### Measured output — two refusals first

With a live consumer in the group:

```
Error: Assignments can only be reset if the group 'g-m19-tsreset' is inactive,
  but the current state is Stable.
```

Committed offsets are group state, and group state belongs to the group while it has members.
There is no `--force`; the consumers must actually stop. (In this project's terms: scale the
service to zero replicas first.)

Consumer stopped, flag forgotten:

```
WARN: No action will be performed as the --execute option is missing. In version 5.0,
  this command will require either --dry-run or --execute to be specified. ...

GROUP           TOPIC             PARTITION  NEW-OFFSET
g-m19-tsreset   drill.m19.tsreset 0          1000
```

The table looks exactly like success — it even computed the correct target — but the describe
immediately afterwards shows `CURRENT-OFFSET 3000`. Nothing happened. The default is a dry
run, the warning scrolls past, and the operator walks away believing the replay is armed. The
5.0 deprecation notice exists because this footgun is common enough to redesign the CLI over.

### Measured output — the reset itself

```
$ ... --reset-offsets --to-datetime "2026-08-28T17:00:14.694" --execute
GROUP           TOPIC             PARTITION  NEW-OFFSET
g-m19-tsreset   drill.m19.tsreset 0          1000

CURRENT-OFFSET  LOG-END-OFFSET  LAG
1000            3000            2000
```

Offset 1000 — the first record of batch 2 — from a timestamp that matches no record. The rule
from the hypothesis holds: first offset with timestamp ≥ target. And the group reads exactly
where the arithmetic says it should:

```
batch2-1
batch2-2
batch2-3
Processed a total of 3 messages
```

The same resolution is available without touching any group — it is a plain `ListOffsets` API
call, the machinery underneath both the reset tool and `kafka-get-offsets.sh`:

```
$ kafka-get-offsets.sh --topic drill.m19.tsreset --time 1787936414694
drill.m19.tsreset:0:1000
```

### Measured output — a timestamp after the last record

`--time <now + 1 h>` through `kafka-get-offsets.sh` returns nothing at all (no offset has a
timestamp ≥ target). The reset tool handles the same case by falling back — with a message
that deserves a moment of scrutiny:

```
Warn: Partition 0 from topic drill.m19.tsreset is empty. Falling back to latest known offset.
GROUP           TOPIC             PARTITION  NEW-OFFSET
g-m19-tsreset   drill.m19.tsreset 0          3000
```

The partition holds 3,000 records; it is not empty. "Empty" here means "the timestamp lookup
returned no offset" — the warning describes the API response, not the partition. The fallback
itself is sensible (future datetime → start at the end), but an operator reading that message
during an incident would reasonably conclude their data is gone.

### What it means

- **Timestamp resolution is exact, cheap, and quantised to records, not time.** The
  `.timeindex` maps timestamps to offsets per segment; the answer is always the first record
  at-or-after the target instant. A target in a quiet gap snaps forward to the next record —
  which is precisely the replay semantic you want: "everything from 14:00 onwards" includes the
  first event after 14:00.
- **A reset is just a commit with no consumer attached.** Nothing about the log changes — no
  data moved, no data deleted (compare drill 7, where the log changed and offsets stood
  still). `__consumer_offsets` gets a new entry for the group, the same way a consumer's
  auto-commit would write one. That is why the group must be inactive: a live member's next
  auto-commit would silently overwrite the reset, so the tool refuses rather than race.
- **The two refusals are load-bearing.** The active-group check prevents the race above; the
  dry-run default prevents an accidental rewind of a production group. But the dry-run output
  is indistinguishable from success at a glance — the discipline is to always follow a reset
  with `--describe` and read `CURRENT-OFFSET`, not the tool's own table.
- **Record time is producer time.** The timestamps in the `.timeindex` are `CreateTime` —
  stamped by the producer's clock, the same clock drill 7 showed governing retention
  ("largest record timestamp"). A producer with a skewed clock shifts both what retention
  deletes and where a datetime reset lands. Replay boundaries are only as trustworthy as the
  producers' NTP.
- **Replay is at-least-once, by construction.** Rewinding to 17:00:14 reprocesses batches 2
  and 3 in full. Every downstream effect of those 2,000 records happens again — which is
  exactly why the pipeline's consumers were built idempotent back in M5/M8 (the ledger's
  unique-key upserts, the webhook notifier's delivery dedup). The reset tool is safe because
  the consumers were designed for it; on its own it is a duplication machine.

### Leftovers

`drill.m19.tsreset` is kept (3,000 tiny records, well under 1 MiB). The group `g-m19-tsreset`
was deleted after the drill — `Deletion of requested consumer groups ('g-m19-tsreset') was
successful` — after a first attempt failed with `GroupNotEmptyException` while the stray
consumer from the active-group refusal test was still connected: even *deleting* a group
requires it to be inactive, the same rule as resetting one.

---

## Drill 9 - Chaos: cordon a node, kill a broker, kill the ledger mid-transaction

The last drill stops rehearsing single mechanisms and attacks the running system in two acts.
Act A goes after the infrastructure: cordon a Kubernetes node and delete the broker pod on it,
under sustained producer load. Act B goes after the application's strongest guarantee: a
`--grace-period=0 --force` kill of the ledger — the one service with a transactional producer
(`transactional.id` `ledger-tx-0-0`, offsets committed via `sendOffsetsToTransaction`) — while
real payments flow through the full chain: gateway → payment-api → outbox → Debezium →
`payment-requested` → psp-connector → `payment-status-changed` → ledger → `ledger-entry-recorded`.

### Hypothesis

Act A: with RF=3, `min.insync.replicas=2` and `acks=all`, one broker down is the survivable
case — the producer should ride through with zero errors while ISR shrinks to 2. And because
the broker's storage is a PersistentVolume pinned to the cordoned node, Kubernetes should *not*
reschedule the pod elsewhere: it should stay Pending until the node returns. Strimzi cannot
"heal" what the PV nails down.

Act B: exactly-once should hold. Every `SUCCEEDED` status event gets exactly one committed
ledger entry — no losses, no duplicates — even if the ledger dies mid-transaction. The
replacement pod re-registers the same `transactional.id`, gets a bumped epoch, and the
coordinator aborts whatever the dead instance left open.

### Act A - measured output

`drill.m19.chaos` (RF=3, leader broker 1), `kafka-verifiable-producer` at 40 rec/s, 6,000
records. 10 s in: `kubectl cordon kafka-psp-worker`, `kubectl delete pod psp-combined-2`.

```
[22:03:43] cordoned + deleted
[22:03:44] pod=Running  Isr: 1,0        <- ISR already shrunk, one second in
[22:03:50] pod=Pending  Isr: 1,0
   ... Pending for the entire cordon window ...
```

Why the pod cannot come back — the scheduler's own words:

```
Warning  FailedScheduling  0/3 nodes are available: 1 node(s) didn't match
  PersistentVolume's node affinity, 1 node(s) had untolerated taint(s),
  1 node(s) were unschedulable.
```

One node is cordoned (ours), one is the tainted control-plane, and the only node the PV's
affinity allows is the cordoned one. A Kafka broker on local storage is not a stateless pod
that reschedules; it is welded to its node. Meanwhile the blast radius, cluster-wide:

```
total under-replicated partitions: 333
```

Every RF=3 partition in the cluster lost its broker-2 replica. And the producer, through all
63 s of it:

```
{"name":"tool_data","sent":6000,"acked":6000,"avg_throughput":39.998}
producer_send_error count: 0
```

`kubectl uncordon` at 22:04:46; the pod schedules, the broker rejoins, and the drill topic's
ISR is back to `0,1,2` at 22:04:53 — **7 s from uncordon to full ISR**, catch-up included
(the broker had missed only ~60 s of traffic).

### Act B - measured output

120 payments POSTed through the api-gateway (one per 300 ms, all `201 Created` — the
rate-limited route absorbed 3.3 req/s without a single 429). 15 s into the load, with events
in flight on every hop:

```
pod "ledger-84568f58cb-7q8lq" force deleted        (20:06:10 UTC)
ledger-84568f58cb-rtwxw   0/1   Running   5s
```

The replacement's transactional bootstrap, from its log:

```
[Producer clientId=ledger-producer-1, transactionalId=ledger-tx-0-0]
  Instantiated a transactional producer.
  Invoking InitProducerId for the first time in order to acquire a producer ID
  Discovered transaction coordinator psp-combined-0...
  ProducerId set to 6002 with epoch 1
```

Same `transactional.id`, **epoch bumped to 1** — this is fencing happening: any write the dead
incarnation might still attempt is now rejected by epoch, and whatever transaction it left
open is aborted by the coordinator. The consumer repositioned to the last *committed* offsets
at 20:06:28 (18 s after the kill — pod restart plus group join) and lag was zero by 20:07:02.

The books, audited by consuming every topic end-to-end (values are Avro; counted byte-exact):

```
payment-requested for merchant-m19-chaos:   120
payment-status-changed:                     117  (106 SUCCEEDED + 11 DECLINED)
ledger-entry-recorded, read_committed:      106 entries, 106 unique paymentIds
ledger-entry-recorded, read_uncommitted:    identical to read_committed
duplicate ledger entries:                   none
SUCCEEDED without a ledger entry:           0
```

Exactly-once held. 106 successes → 106 entries, zero duplicates across a hard kill of the
producer that wrote them. `read_uncommitted == read_committed` also says the kill landed
*between* transactions — the ledger's transactions are per-poll-batch and short, so the window
for catching one open was small; the fencing evidence above is what proves the machinery, not
luck. The 11 `DECLINED` correctly produced no entries.

### Act C - the false alarm that found a real bug

The arithmetic above has a hole: **120 requested, 117 statuses.** psp-connector's consumer
group shows lag 0 on all 12 partitions — it consumed and acknowledged all 120 — and its DLQ
holds 0 records. Three payments went into the non-transactional hop and nothing came out.

The connector's own log offered a suspect, at the exact minute of the load:

```
20:06:31  psp-connector.v1: partitions revoked:  [payment-requested-0..11]
20:06:35  psp-connector.v1: partitions assigned: [payment-requested-0..3]    <- only 4 of 12
20:07:02  psp-connector.v1: partitions revoked:  ...   <- members leaving
20:07:17  psp-connector.v1: partitions assigned: [payment-requested-0..2]
```

Nobody killed psp-connector — **KEDA scaled it** (the M18 `ScaledObject`: min 1, max 6,
trigger: consumer lag > 25) out on the burst's lag and back in a minute later. The three
missing payments sat on partitions 7, 9 and 10, owned by the ephemeral replicas whose logs
died with them. Circumstantial case: offsets committed, no status events, no DLQ, pods gone —
scale-in ate them through the one consume→produce hop with no transaction.

**That conclusion was wrong.** The ground truth was one Postgres query away the whole time —
the attempt log records what the provider actually answered:

```sql
SELECT outcome, count(*) FROM payment_attempts
 WHERE merchant_id='merchant-m19-chaos' GROUP BY outcome;
 outcome  | count
----------+-------
 APPROVED |   106
 DECLINED |    11
 TIMEOUT  |     3
```

106 + 11 + 3 = 120. The three "lost" payments are the simulated provider's `timeout-rate:
0.05` firing three times in 120 calls — and a TIMEOUT, by ADR-0006 category A, **never
publishes a status event**: the row is recorded, the exception propagates, and after the
redelivery is deduplicated the event is acknowledged with no output, by design (M12's
provider-status query is the exit for those payments). Nothing was lost. The drill survived
its chaos completely; the investigator did not survive his own pattern-matching — a rebalance
in the logs at the right minute plus missing events made a story too plausible to check.

And yet the false alarm was the most productive finding of the module, because chasing it
forced a real read of the hop's code, and the code had an actual loss window:

```java
// KafkaPaymentStatusPublisher, before the fix:
kafkaTemplate.send(record).whenComplete((result, ex) -> { if (ex != null) log.error(...); });
// listener, immediately after:
ack.acknowledge();
```

Fire-and-forget: the offset commit raced the broker acknowledgement, and a send failure was
*logged and acknowledged anyway*. Worse, the M5 dedup made any such loss permanent: a
redelivery found the attempt row (written before the publish) and skipped the publish as
"already processed" — the row's existence never proved the event's. The window simply never
happened to fire during the drill.

**The fix** (commit alongside this doc): the publish now blocks until the broker acknowledges
and propagates failure to the listener (no ack, redelivery); every attempt row stores the
outbound event's envelope `eventId` (`status_event_id`, migration V4); and every dedup hit
REPUBLISHES the stored event under that same id instead of skipping — downstream dedup (the
ledger's `uq_ledger_entries_inbound_event_id`) sees a byte-identical idempotency key and drops
the copy. `ProcessPaymentRequestUseCaseTest#crashBetweenRecordAndPublishIsRepairedByRedelivery`
is the loss scenario as a unit test: publish fails, listener never acks, redelivery republishes
the same eventId, nothing charged twice.

**The regression run**, against the fixed build on the live cluster — same shape as act B but
harder (KEDA went 1 → 5 replicas and back, ledger hard-killed mid-flight, 150 payments):

```
150 requested = 123 APPROVED + 15 DECLINED + 12 TIMEOUT     (Postgres, the ground truth)
138 status events, 138 unique paymentIds                     (= 123 + 15, exactly)
123 committed ledger entries, 0 duplicates, set == APPROVED set
LOST: none
```

### What it means

- **Verify the boring hypothesis before the dramatic one.** The 3-in-120 gap had two candidate
  explanations: a 2.5% sampling of a documented 5% timeout rate, or a novel distributed-systems
  loss mechanism. The base-rate answer was checkable in one SQL query; the dramatic answer got
  written up first because a rebalance log line at the right timestamp made it *feel* proven.
  Correlation in chaos drills is treacherous precisely because chaos makes everything correlate.
- **The false alarm still paid for itself.** The fire-and-forget publish and the
  dedup-that-skips were real defects — real enough that the fix changed a migration, a domain
  model, two publishers and two use cases. A latent loss window that hasn't fired yet is
  exactly what failure drills exist to surface; this one was surfaced by code review under the
  pressure of a wrong hypothesis, which is still surfacing.
- **Storage pins brokers; cordon ≠ drain for Kafka.** The PV's node affinity means a cordoned
  node's broker cannot be "healed" anywhere else — by design. The operational play for node
  maintenance is Strimzi's rolling machinery (or accept the under-replicated window), not the
  stateless-app instinct of "delete the pod, it'll come back somewhere".
- **333 under-replicated partitions is one broker.** A single-broker outage degrades every
  RF=3 partition in the cluster simultaneously. `min.insync.replicas=2` is what turned that
  from an availability incident into a non-event — the same config drill 1 (part 1) showed
  rejecting writes when it *couldn't* be satisfied.
- **Epoch fencing is the whole EOS story in one log line.** `ProducerId set to 6002 with
  epoch 1` — a stable `transactional.id` surviving its pod is what lets the coordinator
  distinguish "restarted" from "duplicated". Kill -9 is safe *because* the identity is durable
  and the epoch is not.
- **Autoscaling a consumer is still a correctness surface.** The KEDA cycle didn't lose
  anything this time, but scale-in does terminate pods at their busiest, and before the fix the
  hop's guarantee under any such termination was at-most-once. After it: at-least-once with a
  stable event identity, which downstream idempotency converts to effectively-once — the same
  contract the rest of the pipeline already ran on.

### Leftovers

`drill.m19.chaos` is kept (6,000 records, RF=3). The node is uncordoned, all 16 service pods
plus `kafka-drill` are Running, psp-connector is back to 1 replica. The three TIMEOUT payments
from act B and the twelve from the regression run sit in `payment_attempts` with no status
event, exactly as ADR-0006 intends. Part 2's drill topics
(`drill.m19.{reassign,retention,tsreset,chaos}`) follow part 1's convention: kept as evidence,
deletable with the same loop.

---

## Drill 10 - The deferred lag drill: a metrics stack on Kubernetes

Part 1 could not run this one. Its preamble says so: *"Not possible in this cluster: anything
requiring Grafana or Prometheus. They lived in the `infra/compose` stack, which is stopped; there
is no metrics stack in the kind cluster."* Nine drills were measured out of the Kafka protocol
instead — AdminClient, `kafka-verifiable-*`, broker logs — and the one drill from `docs/PLAN.md`
that genuinely needs a time series ("deliberately induce lag, watch it recover on a dashboard")
was deferred.

This drill closes that. It is two pieces of work that only look like one: **building the metrics
stack**, which is infrastructure and has its own failure modes, and then **running the lag drill
against it**, which is the drill PLAN.md actually asked for.

### Hypothesis

About the stack:

1. Turning on Kafka metrics gives you consumer lag. **(This is wrong, and it is the interesting
   part.)** Lag is not a broker MBean — a broker knows every partition's end offset and holds
   every group's committed offset in `__consumer_offsets`, but it never subtracts them. So
   `Kafka.spec.kafka.metricsConfig` alone should produce a large, healthy pile of broker series
   and **zero** lag series; `Kafka.spec.kafkaExporter` is a separate switch for a separate
   process.
2. M15's dashboards should import unchanged. They were written against kafka-exporter's metric
   names, and Strimzi's `kafkaExporter` runs that same exporter.

About the drill:

3. `payments.payment-requested.v1` has 12 partitions; psp-connector's per-pod throughput is
   ~0.39 rec/s (M18 phase 3). Three k6 pods at 5 req/s = 15 req/s in, which no replica count
   under the `maxReplicaCount: 6` ceiling can match — so lag should rise for the whole load
   window, KEDA should walk the deployment 1 → 6, and the backlog should then drain at a rate
   that is **linear in replicas** (6 pods over 12 partitions is still below the one-consumer-per-
   partition ceiling, so nothing should be idle).
4. Scale-in should lag the drain: KEDA's `cooldownPeriod` is 60 s but the HPA's own
   `scaleDown.stabilizationWindowSeconds` is what actually governs, and the HPA's arithmetic is
   `ceil(lag / lagThreshold)` — with `lagThreshold: 25`, replicas should not start coming down
   until total lag is under 150.

### Method

**Part A — the stack.** Two `kubectl apply`s and one script, in this order:

```bash
kubectl apply -f infra/k8s/kafka/15-metrics-configmap.yaml    # jmx_exporter rules
kubectl apply -f infra/k8s/kafka/20-kafka.yaml                # metricsConfig + kafkaExporter
infra/k8s/scripts/install-monitoring.sh                       # Prometheus + Grafana
```

The Kafka CR gains exactly two blocks — one on `spec.kafka`, one at the top level:

```yaml
    metricsConfig:
      type: jmxPrometheusExporter
      valueFrom:
        configMapKeyRef: { name: kafka-metrics, key: kafka-metrics-config.yml }
...
  kafkaExporter:
    topicRegex: ".*"
    groupRegex: ".*"
```

Prometheus (chart 29.27.0, app v3.14.0) and Grafana (chart 10.5.15, app 12.3.1) go into a
`monitoring` namespace as plain charts, not `kube-prometheus-stack` — reasoning and every trimmed
value in [`infra/k8s/README.md` → Monitoring](../infra/k8s/README.md#monitoring-prometheus--grafana-on-the-cluster-m19-revisit).

**Part B — the drill.** The existing M18 load generator, at the same shape as M18's KEDA proof:

```bash
infra/k8s/scripts/load-test.sh --pods 3 --duration 3m     # 3 pods x 5 req/s = 15 req/s
```

Three pods rather than a higher rate because api-gateway rate-limits at 5/s **per client IP**
(M16), so load scales by source, not by throughput knob. Two independent samplers run alongside:
`kubectl get deploy psp-connector` every 5 s for the replica trajectory, and — the thing that was
impossible in part 1 — Prometheus's own `query_range` API for the lag series:

```bash
kubectl port-forward -n monitoring svc/prometheus-server 9090:9090 &
curl -s http://localhost:9090/api/v1/query_range \
  --data-urlencode 'query=sum(kafka_consumergroup_lag{consumergroup="psp-connector.v1",topic="payments.payment-requested.v1"})' \
  --data-urlencode "start=$START" --data-urlencode "end=$END" --data-urlencode 'step=10'
```

The `topic=` filter is not cosmetic. kafka-exporter reports **`-1`**, not 0, for a partition a
group has never committed to, so a bare `sum(kafka_consumergroup_lag{consumergroup="psp-connector.v1"})`
on an idle cluster returns **`-4`** — psp-connector has never consumed four partitions of
`refunds.funds-reserved.v1`.

### Measured output — Part A, the stack

The rolling restart, which is the price of `metricsConfig` and cannot be avoided:

```
22:13:07  kubectl apply -f kafka/20-kafka.yaml
22:13:11  psp-combined-1  recreated
22:13:46  psp-combined-2  recreated
22:14:21  psp-combined-0  recreated
22:14:56  all three 1/1 Running
22:15:25  Kafka psp  Ready=True
```

**2 min 18 s**, one broker at a time, ISR recovering in between — considerably faster than the
5–10 min a three-broker roll is usually budgeted at, because these brokers hold ~1.5 GiB of log
between them, not terabytes. The kafka-exporter Deployment appeared at **22:14:56**, *after* the
last broker: Strimzi rolls the cluster first and only then reconciles the auxiliary components.

Hypothesis 1 confirmed, and it is a bigger split than it sounds. The JMX agent on the three
brokers produces **30,396 series**; not one of them is lag:

```
$ curl -s 'localhost:9090/api/v1/query?query=count({job="kafka-brokers"})'      -> 30396
$ curl -s 'localhost:9090/api/v1/query?query=count(kafka_consumergroup_lag)'    ->   474
```

The 474 come entirely from the fourth target, the exporter. All five targets up:

```
kafka-brokers    http://10.244.2.14:9404/metrics  up
kafka-brokers    http://10.244.1.20:9404/metrics  up
kafka-brokers    http://10.244.2.15:9404/metrics  up
kafka-exporter   http://10.244.2.16:9404/metrics  up
prometheus       http://localhost:9090/metrics    up
```

Getting to four healthy targets took one wrong turn worth recording, because it fails *silently*.
The broker scrape job first selected on `strimzi.io/kind=Kafka` + port name `tcp-prometheus` —
which reads like "the Kafka pods". The operator stamps `kind=Kafka` on the **kafka-exporter** pod
too, and names its metrics port `tcp-prometheus` as well, so the broker job swallowed the exporter
and two jobs reported one target with no error anywhere. `strimzi.io/name` is the per-component
label (`psp-kafka` vs `psp-kafka-exporter`) and is what actually separates them.

Hypothesis 2 confirmed: both M15 dashboards imported with **zero query changes**. Every metric a
panel asks for exists, verified against the live cluster rather than assumed:

| Panel's query | Series |
|---|---|
| `kafka_consumergroup_lag` | 474 |
| `kafka_consumergroup_current_offset` | 474 |
| `kafka_topic_partition_in_sync_replica` | 334 |
| `kafka_topic_partition_leader` | 334 |
| `kafka_topic_partitions` | 43 |
| `kafka_brokers` | **3** |

The one thing that did need adapting was not in the JSON: the **datasource uid**. Every panel
refers to `{"type": "prometheus", "uid": "prometheus"}`, a fixed id chosen in M15 for portability,
so Grafana's datasource is provisioned with `uid: prometheus` explicitly. Any other uid imports
both dashboards successfully and renders every panel as *"Datasource prometheus was not found"* —
a per-panel error, not an install failure.

### Measured output — Part B, the lag drill

k6 ran 22:19:56 → 22:23:00. Three pods, `constant-arrival-rate`, no errors anywhere:

```
201 Created (real payments):  901   +   901   +   900   =  2702
429 rate-limited by gateway:    0        0       0
5xx server errors:              0        0       0
gateway p95 (ms):            12.5
```

The lag series, straight out of Prometheus (`sum(kafka_consumergroup_lag{consumergroup=
"psp-connector.v1", topic="payments.payment-requested.v1"})`, 10 s step), against the replica
count sampled independently with `kubectl get deploy psp-connector`:

```
  time      lag   spec/ready   event
22:19:56      0      1/1        k6 starts
22:20:10      1      1/1        first record visible - a full 14 s after the load began
22:20:12      1      3/1        KEDA scales 1 -> 3
22:20:22    283      3/3
22:20:42    568      5/3        1 -> 3 -> 5, two steps in 30 s
22:20:52    708      5/5
22:21:08    850      6/5        ceiling reached, 72 s after the first record
22:21:23   1115      6/6
   ... lag keeps rising for another 2 minutes, at max replicas ...
22:23:00   2275      6/6        k6 stops
22:23:10   2396      6/6        PEAK
```

**Peak lag 2,396 records at 22:23:10**, ten seconds *after* the load stopped — the backlog kept
growing past the end of the load because the in-flight requests were still landing.

Then the drain, which is the half worth measuring. Sampling the committed offset alongside the lag
turns the curve into a throughput number:

```
  time      lag   committed   Δcommitted/10s   spec/ready
22:23:10   2396      3507          -            6/6
22:25:10   2133      3780         24            6/6
22:29:10   1584      4329         23            6/6
22:33:10   1042      4871         24            6/6
22:37:10    492      5421         28            6/6
22:39:10    219      5694         25            6/6
22:40:10     89      5824         25            6/6
22:40:40     47      5866          -            5/5   <- scale-in begins
22:41:10     26      5887          7            4/4
22:41:41     10        -           -            3/3
22:42:10      0      5913          1            2/2
22:42:42      0      5913          0            1/1   <- back to baseline
```

**Time to drain: 19 min 0 s** (22:23:10 → 22:42:10) for a backlog built in 3 min 4 s. The
build-to-drain ratio is **1 : 6.2**.

The steady `Δcommitted` of **23–25 records per 10 s = ~2.3 rec/s** across six replicas is
**0.38 rec/s per pod** — M18 phase 3 measured per-pod throughput at ~0.39 rec/s from a completely
different direction (timing a single pod). Six pods delivered six times one pod, with no
sub-linearity: 6 consumers over 12 partitions is still under the one-consumer-per-partition
ceiling, so nothing was idle and nothing was contended. Hypothesis 3 confirmed, including the
prediction that the load could not be matched — 15 rec/s in against 2.3 rec/s out is why lag rose
for the entire window even at `maxReplicaCount`.

Hypothesis 4 confirmed as well, and the arithmetic is visible in the table. The HPA's target is
`25` lag per replica, so its recommendation is `ceil(lag / 25)`; six replicas are justified while
lag ≥ 150. Lag crossed 150 between 22:39:10 and 22:40:10, and the **first scale-in came at
22:40:40** — the delay is the HPA's `scaleDown.stabilizationWindowSeconds: 60`, which makes it
take the *maximum* recommendation over the trailing minute rather than the current one. From there
it stepped down one replica roughly every 30 s: **6 → 5 → 4 → 3 → 2 → 1 in 2 min 2 s.**

Nothing was lost across all of it — the sum the drill exists to check:

```
k6 201 Created                                   2702
committed offset delta (3211 -> 5913)            2702
final lag                                           0
```

And the accidental finding, visible the moment the lag dashboard rendered for the first time.
The top panel — `sum(kafka_consumergroup_lag)` across every group — was not dominated by
psp-connector at all:

```
realtime-gateway.realtime-gateway-86f49fcbff-ndffv.42b4b03f-…   3337
realtime-gateway.realtime-gateway-86f49fcbff-ndffv.9c203f6d-…   3101
realtime-gateway.realtime-gateway-6479bb56f8-fx9pk.c1a9ee68-…   2812
realtime-gateway.realtime-gateway-5969dc595b-rqmlg.7faf10f2-…   2812
realtime-gateway.realtime-gateway-5969dc595b-rqmlg.fcd66b15-…   2812
psp-connector.v1                                                2271
connect-mongo-audit-sink                                         225
analytics-streams.v1                                              55
realtime-gateway.realtime-gateway-9b9d4ddbd-pcj2b.f5cff326-…       1
```

Only `9b9d4ddbd-pcj2b` is a running pod. The other five groups belong to realtime-gateway
instances that were deleted on earlier redeploys and are never coming back — their group ids
embed the pod name by design (M12: every gateway instance needs *every* message, so every
instance is its own consumer group), which means each redeploy strands one forever. They are not
broken and they harm nothing, but they carry more "lag" than the service under test, and any
alert on total cluster lag would fire on them permanently.

### What it means

- **"Kafka metrics" and "consumer lag" are two different products.** This is the one line worth
  taking away from part A. `metricsConfig` gave 30,396 series and answered nothing about the
  backlog; `kafkaExporter` gave 474 and answered the whole drill. The reason is structural, not a
  Strimzi quirk: lag is a *subtraction across two pieces of cluster state* (a partition's end
  offset, and a group's committed offset in `__consumer_offsets`) and no single broker MBean
  spans both. Anyone who turns on JMX metrics, sees a thousand `kafka_server_*` series and
  concludes monitoring is done has monitored everything except the number that pages you.
- **The dashboard's headline panel was the least trustworthy thing on it.** M15's
  `sum(kafka_consumergroup_lag)` over all groups is the first panel a human looks at, and on this
  cluster it is wrong twice over — it counts `-1` sentinels for never-consumed partitions, and it
  is dominated by five abandoned groups belonging to pods that no longer exist. The queries that
  survived contact with a real cluster were the *specific* ones, filtered by group and topic. A
  panel that aggregates over a dimension nobody is curating is a panel that measures the mess, not
  the system.
- **A metric nobody reads decays silently.** Those orphaned `realtime-gateway.<pod>.<uuid>` groups
  have been accumulating since M12 — each redeploy of realtime-gateway leaves one behind, because
  the group id embeds the pod name (the deliberate M12 design: every gateway instance needs *every*
  message, so every instance is its own group). Nothing was broken by it, and nothing surfaced it,
  until a dashboard existed to make it visible. That is the actual argument for the metrics stack,
  independent of the drill: it did not answer a question anyone had asked.
- **Turning on metrics restarts Kafka.** 2 min 18 s here, and it is a rolling restart of the
  data plane triggered by an observability change. On a real cluster that is a change-window
  conversation, and it is a strong argument for enabling `metricsConfig` on day one rather than
  the day something is wrong — which is precisely the position this cluster was in when part 1
  was written.
- **Lag is a leading indicator; replica count is a lagging one.** The two series tell the same
  story ~15 s apart, and the offset is the pipeline: kafka-exporter's own poll, Prometheus's 10 s
  scrape, KEDA's 15 s `pollingInterval`, then the HPA's sync. Every one of those is a delay
  between *the backlog growing* and *anything reacting*. `activationLagThreshold: 5` means the
  first pod arrives fast, but "fast" here still means the backlog was already ~140 records deep.
- **The drain is the honest half of the drill.** Building lag is easy; the useful measurement is
  what it costs to get rid of it. This is the asymmetry every lag-based autoscaler has to be sized
  against: three minutes of a load the consumer cannot match takes many times three minutes to
  clear, because the drain rate is not the load rate — it is the consumer's throughput minus zero.
  A `maxReplicaCount` chosen so that peak throughput is *below* peak load is a choice to accumulate
  a backlog, and it should be made deliberately, with this ratio in hand.

### Leftovers

Unlike drills 6–9, this one leaves things **running on purpose**. The `monitoring` namespace stays
up — Prometheus (`prometheus-server`) and Grafana (`grafana`), two pods, no PVCs — because the
whole point was that the next drill should not have to open with part 1's disclaimer. The Kafka CR
keeps `metricsConfig` and `kafkaExporter`, so `psp-kafka-exporter` is now a permanent fourth
workload in the `kafka` namespace.

```bash
kubectl port-forward -n monitoring svc/prometheus-server 9090:9090   # /targets, /graph
kubectl port-forward -n monitoring svc/grafana 3000:80               # folder "Kafka", user admin
kubectl get secret grafana -n monitoring -o jsonpath='{.data.admin-password}' | base64 -d ; echo
```

The Grafana admin password is generated once at install and lives only in that Secret — there is
no `adminPassword:` in `infra/k8s/monitoring/grafana-values.yaml`, deliberately.

No drill topic this time: the drill ran against the real pipeline
(`payments.payment-requested.v1`, group `psp-connector.v1`) rather than a `drill.m19.*` topic, so
the only residue is the payments themselves, which are indistinguishable from any other load-test
run. psp-connector is back at 1 replica. Neither did this drill use the `kafka-drill` client pod
the rest of part 2 runs inside — everything here is `kubectl`, `helm` and Prometheus's HTTP API
from the host. (That pod had failed on its own, `Exit Code: 255`, during an unrelated Docker
restart before this drill started; it was removed rather than left `Failed` in `kubectl get pods`,
and part 1's [Test harness](M19-failure-drills.md#test-harness) recreates it in two commands.)

Retention is **6 h**, on `emptyDir`. The series quoted above are gone by now; that is why they are
in this document.
