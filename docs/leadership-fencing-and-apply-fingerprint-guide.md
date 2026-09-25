# Kahuna leadership fencing and apply-fingerprint guide

This guide explains two defenses against a write that a client saw acknowledged and the cluster
later did not have: the **quorum-confirmed gate on actor-only mutations**, which stops a leader that
lost its voters from staging writes and handing out locks from a memory nobody else sees, and the
**per-partition apply fingerprint**, which makes a replica whose apply stream diverged from the log
visible in the cluster's own signals and stops a range split from copying out of it. It is written
for operators running a clustered Kahuna deployment.

Related guides: [cluster membership operations](cluster-membership-operations-guide.md),
[replication factor](replication-factor-guide.md), and
[load-based range splitting](load-based-range-splitting-guide.md).

---

## 1. Two leaders of one partition

A Raft leader that is cut off from its voters keeps believing it leads until a message from a higher
term reaches it. In that window the other voters elect a second leader. Kahuna handles the window
in three layers.

**Replication fails on its own.** Every direct write and every durable transaction phase is a Raft
proposal. A proposal on the cut-off leader never reaches a quorum, so the caller gets `MustRetry`.
This layer needs no configuration.

**Reads confirm leadership through a quorum.** An authoritative read first runs a Raft read-index
round (`ConfirmLeadershipAsync`). A leader that cannot confirm answers `MustRetry`.

**Actor-only mutations confirm leadership through a quorum.** Some operations change in-memory actor
state without a proposal:

- a set, delete or extend inside an interactive transaction (a staged MVCC entry plus a write intent),
- an exclusive point, prefix or range lock, and its release,
- a prepare, commit or rollback mutation ticket.

These operations now pass the same read-index confirmation the reads pass. A cut-off leader that
receives one answers `MustRetry`. The client retries against the current view, and nothing is left in
the cut-off leader's memory.

### The check-quorum step-down

Kommander can also make the cut-off leader step down by itself: a leader that hears no same-term ack
from a majority of its voters for `HeartbeatInterval × CheckQuorumIntervalMultiplier` reverts to
follower. This bounds the two-leader window to a few seconds instead of the length of the network
fault.

| Setting | Server option | Embedded option | Default |
|---|---|---|---|
| Check-quorum step-down | `--raft-enable-check-quorum` | `EmbeddedKahunaOptions.EnableCheckQuorum` | on |
| Window, in heartbeat intervals | `--raft-check-quorum-interval-multiplier` | `EmbeddedKahunaOptions.CheckQuorumIntervalMultiplier` | 0 = derived from the election timeout |

The server switch is a bare flag and cannot express "off". Set the environment variable
`KAHUNA_CHECK_QUORUM=0` to turn the step-down off.

With the multiplier at 0 the window equals the start of the election timeout (2 s by default), which is
the largest window that still steps an isolated leader down before any follower can start an election.
An explicit multiplier must keep the window at or below that start; Kommander refuses to start otherwise.

### The term fence and the leadership-lost purge

Kommander 1.7.4 adds two primitives, and Kahuna uses both.

**Every proposal carries the term it was admitted under.** A direct write, a lock transition and
every durable transaction phase read `GetPartitionTerm` at admission and stamp the Raft proposal
with it. The Raft executor compares the stamp with its current term before it appends anything. A
node that lost leadership and won it again in a newer term between admission and flush refuses the
proposal with `TermMismatch`, which Kahuna answers as `MustRetry`. The retry re-judges the write under
the current leadership.

**Leadership loss drops belief-only state.** When Raft reports that this node stopped leading a
partition, every key-value shard drops the staged transactional entries, their write intents, and
the exclusive prefix and range locks of that partition. Committed entries stay. The node logs:

```
KeyValues: leadership of partition 2 lost in term 1; dropping the staged transactional writes and exclusive locks admitted under it
```

A transaction that staged on the old leader fails deterministically at its next step and retries
against the current leader.

### What to look for in the logs

A node that refuses an actor-only mutation because it believes it leads but its quorum did not
confirm logs, at warning level and at most once per partition every 5 s:

```
Refusing an actor-state mutation on partition 2: node n4:8082 believes it leads but a quorum of voters did not confirm the leadership. A second leader may be serving this partition; the caller gets MustRetry
```

One of these lines is the two-leader window itself. Expect a few during a failover. A stream of them
on one node means that node is cut off from its voters and the step-down has not fired.

---

## 2. The apply fingerprint

Every node keeps, per partition, an **apply fingerprint**:

| Field | Meaning |
|---|---|
| applied kv log id | the highest key-value log id the node applied for the partition |
| committed heads | the number of keys the node's committed-head ledger holds for the partition |
| live intents | the number of prepared intents the node holds whose keys route to the partition |

The committed-head ledger and the prepared-intent slice are pure functions of the log. Two replicas at
the same applied log id must therefore hold the same number of committed heads and the same number of
live intents. A replica that does not is a replica whose apply stream diverged: a snapshot install
marked entries applied without delivering them, an import rewound the application below its cursor,
or it alone rejected a bundled commit. Such a replica misses acknowledged writes and holds settled
transactions' intents as read-only keys, and nothing else in normal operation reveals that.

The three fields are read as one snapshot: the apply path brackets every entry with a per-partition
version, and the read retries until it sees the same even version before and after, so an apply
landing between the reads cannot pair one entry's counts with the previous entry's id.

### Where the fingerprint is visible

- **Log line at every leadership change**, one per node:
  `KeyValues: leader for partition {P} is now {Node} (local applied kv log id {Id}, committed heads {N}, live intents {M})`.
- **Gauges** on the `Kahuna` meter, tagged by `partition`:
  `kahuna.keyvalues.applied_log_id` and `kahuna.durable_tx.committed_head_ledger_entries`.
- **Inter-node read** `GetPartitionApplyFingerprint`, answered by any replica from its own memory.

### The comparison at every leader change

When a partition's leader changes, **every replica** of the partition (the new leader and each
follower) compares the leader's fingerprint with every other replica's, off the notification path.
One attempt is bounded to 5 s and 2 s per peer; the comparison retries every 250 ms for up to 10 s
until it is *conclusive* (every peer compared at the leader's applied log id) or a divergence is
found, and runs once more for a leader change that lands while it is running. A window that closes
without comparing every peer is logged at warning level and counted in
`kahuna.keyvalues.apply_fingerprint_inconclusive`: that is not a pass, an uncompared peer may be the
diverged one.

A peer at the same applied log id with a different committed-head or live-intent count is reported
by the leader:

```
KeyValues: apply divergence on partition 2 at leader change: leader n4:8082 holds 107 committed heads and 0 live intents but replica n5:8084 holds 122 heads and 0 intents at the same applied kv log id 4909. One replica's apply stream diverged from the log (leader n4:8082 is the incomplete one); served from the incomplete state, reads miss acknowledged writes and settled intents hold their keys
```

The line is at error level and the counter `kahuna.keyvalues.apply_divergence_detected` increases
once per divergent peer. The line names the side: **fewer committed heads** at the same applied id is
the incomplete replica (heads only grow along the log). When only the live intents differ the counts
do not name a side — a replica that missed settlements holds more, a replica whose prepares were
erased holds fewer — so the divergence is reported but nothing is contained; the heads name the side
once the affected commits settle, and recovery's peer cross-check (transaction lifecycle guide, §6.7)
names it for a held intent.

### Containment

Detection alone changed nothing in the fault soaks that found these shapes: the short replica led,
served reads that missed acknowledged writes, and the partition went read-only on its held intents.
A divergence that indicts the local node is confirmed by a second comparison and then contained:

- **The partition is gated on that node.** Every locally served read or write of the partition
  answers `MustRetry` and routes to the leader: the locator's leadership helpers answer "not the
  leader here", a leader resolution naming the node answers "no target", and the non-locating
  inter-node batch reads refuse the key. Logged once at error level
  (`KeyValues: partition {P} is gated on {Node} after leader change: …`), counted as
  `kahuna.keyvalues.apply_divergence_contained{action=gated}`.
- **A gated leader relinquishes.** It hands the partition to the fullest peer the evidence names
  (`TransferLeadershipAsync`; `action=transferred`), or steps down when the transfer is refused
  (`action=stepped_down`; `action=relinquish_failed` when neither worked). A gated node elected again
  relinquishes at once on the flag, without re-probing.
- **A gated replica withholds its candidacy** (`IRaft.SetCandidacyWithheld`, Kommander 1.8.1): it
  never campaigns and ignores a leadership transfer aimed at it, so no election seats the incomplete
  projection. Released when the gate clears.
- **A gated replica asks to be re-seeded** (`IRaft.RequestReseedAsync`, renewed every minute while
  gated; `apply_divergence_contained{action=reseed_requested}`). Kommander holds the replica's
  committed applies, the leader takes a fresh checkpoint and ships a whole-partition snapshot marked
  as requested, and the replica installs it even though its own log already covers the index. The
  request is refused while the replica still leads, so it follows the relinquish.
- **The gate clears when the whole-partition install replaces the projection**
  (`kahuna.keyvalues.apply_divergence_repaired`; the install boundary also moves the fingerprint's
  applied id to the checkpoint entry, so the fingerprints line up at the next committed entry). In
  the in-process tests the whole cycle, from detection to a repaired and re-electable replica, takes
  well under a second.

The split's pre-copy comparison (below) reports through the same path but does not contain: it runs
on the meta-partition leader, not on the diverged node.

### The comparison before a range split

A range split copies the moving half through the source partition's leader. Before the bulk copy,
the split runs the same comparison on the source partition. If the comparison names the leader as the
incomplete side, the split is refused with the retryable outcome `SourceStateIncomplete`, and
`kahuna.range.split.incomplete_source_refusals` increases. The trigger retries the split on its next
cadence. A replica behind the leader is reported through the leader-change path and the split
proceeds, because the copy reads the leader.

### Why the leader's apply order is the log order

The apply fingerprint detects a divergence; the rule below prevents the one in which an ex-leader alone
rejects a bundled commit its peers admitted, seconds after a graceful handover, and one acknowledged
write is then missing on that replica.

The transaction-record and prepared-intent stores have exactly one live writer on every node: the
per-partition consumer apply that Raft drives in log order. The write scheduler's completion for a
locally proposed durable entry runs when the proposal is quorum-durable, which is before the leader's own
consumer apply of that entry and of the entries below it, and can also trail that apply by an arbitrary
delay (a handover storm is exactly such a delay). It therefore never applies the delta. It waits for the
ordered apply of its entries and reads the result that apply recorded, so a prepare is judged against a
competitor's intent, and a bundled commit against the committed-head ledger, at the entry's own log
position and only there.

The wait is only served while the node leads the partition. A leader whose device stalls is stepped
down by Kommander's durable-write watchdog (3 s by default), and its own apply then cannot advance until
the device heals, while the entries it proposed are quorum-durable and are applied and judged by the new
leader. Serving the wait out there would only turn the stall into a long unknown outcome at the client, so
the moment leadership moves away every parked completion is released and answered as **unobserved**, which
the producer treats exactly like "not committed": it re-drives against the current leader, where the same
entries are idempotent in the log. One bound (10 s, the proposal timeout) covers a whole submission, never
one per entry, and a completion that exhausts it is answered the same way.

Three counters watch the rendezvous:

| Metric | Meaning |
|---|---|
| `kahuna.durable_tx.ordered_apply_waits_released_on_leadership_loss` | Completions released before the ordered apply because the node stopped leading the partition. Bursts at every leadership change with writes in flight; the producers retried against the new leader. |
| `kahuna.durable_tx.ordered_apply_wait_timeouts` | Completions that did not see the ordered apply of their committed entry within the bound while the node still led. The producer retried. A sustained rate means a leader's apply stream is stalled without the watchdog stepping it down, or completions and applies disagree on log identity. |
| `kahuna.durable_tx.ordered_apply_results_displaced` | Completions that trailed the ordered apply by more than the result window and read the acknowledgement back from the store. Expected to stay at zero. |

The node logs the release once per leadership change, at information level, and a timed-out completion at
warning level:

```
Released 116 durable completions parked on the ordered apply of partition 1 (leadership lost in term 3); this node no longer leads it, so their producers re-drive against the current leader, where the same entries are idempotent
Completion of committed durable entry #4917 on partition 2 (PreparedIntent) did not see its ordered apply within 10000ms while this node still led the partition; answering the producer as unobserved so it re-drives against the current leader instead of applying the entry out of log order here
```

---

## 3. Metrics

| Metric | Type | Meaning |
|---|---|---|
| `kahuna.keyvalues.applied_log_id{partition}` | gauge | Highest kv log id this node applied for the partition |
| `kahuna.durable_tx.committed_head_ledger_entries{partition}` | gauge | Committed heads this node holds for the partition |
| `kahuna.keyvalues.apply_divergence_detected` | counter | Replicas found divergent at a leader change or before a split |
| `kahuna.keyvalues.apply_fingerprint_inconclusive` | counter | Leader-change comparisons that could not compare every peer inside the retry window |
| `kahuna.keyvalues.apply_divergence_contained{action}` | counter | Containment actions on this node: `gated`, `transferred`, `stepped_down`, `relinquish_failed` |
| `kahuna.keyvalues.apply_divergence_repaired` | counter | Gated partitions whose projection a whole-partition install replaced |
| `kahuna.transactions.recordless_intents_stale_detected` | counter | Record-less holds a majority of the replica set had already settled (transaction lifecycle guide, §6.7) |
| `kahuna.range.split.incomplete_source_refusals` | counter | Splits refused because the source leader's state was incomplete |
| `kahuna.durable_tx.ordered_apply_waits_released_on_leadership_loss` | counter | Durable completions released because the node stopped leading the partition |
| `kahuna.durable_tx.ordered_apply_wait_timeouts` | counter | Durable completions that never saw the ordered apply of their committed entry |
| `kahuna.durable_tx.ordered_apply_results_displaced` | counter | Durable completions that read their acknowledgement back from the store |

Alert on any increase of the divergence counters; `apply_divergence_contained{action=gated}` without a
matching `apply_divergence_repaired` within a few minutes is a re-seed that is not landing (check the
leader's `Re-seed of …` and the replica's `Re-seed request expired` lines).
