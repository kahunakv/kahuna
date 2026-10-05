# Kahuna leadership fencing and apply-fingerprint guide

This guide explains two defenses against a write that a client saw acknowledged and the cluster
later did not have: the **quorum-confirmed gate on actor-only mutations**, which stops a leader that
lost its voters from staging writes and handing out locks from a memory nobody else sees, and the
**per-partition apply fingerprint**, which makes a replica whose apply stream diverged from the log
visible in the cluster's own signals, contains an identified incomplete projection and stops a range
split from copying out of an identified incomplete leader. It is written
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
proposal. While the leader cannot reach a voter quorum it cannot confirm a proposal, and the
caller receives a retryable unresolved result such as `MustRetry`. A missing acknowledgement is
not proof that the proposal cannot later commit; durable finalize retries retain their identity.
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

The standalone `EmbeddedKahunaNode(options)` constructor always disables check-quorum and backfill.
Its in-process phantom witnesses auto-acknowledge but store no data and cannot become leaders; scheduler
or GC pauses should not depose the sole real node. The cluster constructor retains the configured
check-quorum setting, including `EmbeddedKahunaCluster` members. This is not additional failure tolerance
for standalone hosts: only their own backend/WAL can preserve data through process loss.

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

A transaction that staged on the old leader does not fail at its next step by itself: the current
leader has no memory of the key, so a read or a write of it pins the committed head as if the
transaction had never touched the key. The coordinator catches it instead. Every staging folds its
revision into the coordinator, and a restaging of the same key must continue that chain (one above
after a set or delete, equal after an extend); a point read of a staged key must answer the staged
revision. A restaging or a read that does not match marks the transaction, and its commit is refused
with `Aborted` and the reason `Lost staging: …`. The counters are `kahuna.kv.staged_chain_breaks`
(detections) and `kahuna.kv.staged_chain_break_aborts` (commits refused). A blind write with no
earlier staging of the key needs no fence: it is last-writer-wins by design, and a write under a
point lock carries the lock's committed base, which the prepare compares with the head.

### Locks lost at a leader change

The purge also drops every lock of the partition: point locks (a point lock is a write intent), prefix
locks, and range locks in every mode. A lock is what makes a pessimistic transaction's reads one
consistent cut, and nothing tells the transaction that it is gone. Its coordinator still lists the lock.
The new leader answers a renewal, a Shared-to-Exclusive upgrade or a repeated acquire as a fresh grant.
In between, the new leader grants the same keys to another transaction.

The committed base a point lock reports does not cover this. A consumer that reads a row under a Shared
range lock, then upgrades and takes the exclusive point lock, has its base observed at that last grant.
When the Shared lock was lost before it, the base is the competitor's commit, every base check passes,
and the value written was computed from the row as it was before. A transaction that only reads has no
base at all.

So the lock itself is proven, by the Raft term:

- **Every lock grant reports the term it was issued under.** The locator reads `GetPartitionTerm`
  before the leadership confirmation that admits the grant, and reports it with the partition and a key
  that routes to it (`LockGrantTerm`). The value travels through an ambient capture (`LockGrantScope`),
  across the gRPC lock responses, and into the operation's completion, so the coordinator folds it with
  the lock. The read comes before the confirmation on purpose: a node deposed in between would otherwise
  report the new leader's term for a lock that exists only in its own memory.
- **The coordinator keeps one term per partition.** The first grant on a partition fixes it. A later
  grant on the same partition under another term marks the transaction: the leadership changed, and the
  locks granted before it are gone. The coordinator's own range-lock renewals are folded the same way.
- **The commit proves the term.** A probe with the `LeaderTerm` check asks the confirmed leader of each
  partition whether it is still in that term. A term has one leader, and a node that stops leading can
  only lead again in a later term, so the same term means the same leader, uninterrupted since the grant,
  with the lock still in its memory. The node that answers confirms its own leadership with a quorum
  first: a follower shares the leader's term and must not vouch for it. A node already past the term
  answers the change at once, and a node that cannot confirm answers `MustRetry`. The probe runs in the commit-conflict barrier: for the two-phase
  flow that is after the prepares are durable, when replicated intents have taken over the protection of
  the written keys. A transaction with no writes runs it as its whole commit.
- **The same probe proves the lease.** The same leader is not always the same exclusion: a range lock is a
  lease, and when it runs out the leader lets other transactions into the range without telling the
  holder. The actor that first finds a range lock dead records its holder in a node-wide registry
  (`LapsedRangeLockRegistry`) in the same turn, before it grants or admits anything the lock would have
  refused. The `LeaderTerm` probe reads the registry after the leadership confirmation and answers
  `Unlocked` for a transaction found there, which the coordinator refuses as a lost lock. A renewal that
  re-creates the lock does not remove the record. The lease of a lock an actor holds is measured on the
  node's monotonic clock (`KeyValueRangeLock.LeaseEndsAtTick`), not on the hybrid logical clock: a stepped
  wall clock anywhere in the cluster moves the HLC forward by the step at once, and must not end leases
  that have time left. The HLC deadline (`Expires`) is the form the lease travels in, recomputed from the
  time left whenever a lock is copied out of its actor.
- **The one-phase bundle carries the term.** The bundle validates before it proposes, so a leader change
  between the two would let a leader that never held the locks accept it. With
  `OnePhaseApplyTimeValidation` the bundled commit carries the term of the anchor partition's grants
  (`lockGrantTerm`), and every replica rejects it at apply unless the log entry itself was proposed in
  that term. Without apply-time validation a multi-process group sends a transaction that holds a lock
  through the two-phase flow (gate outcome `held_lock`).

A failed proof refuses the commit with `Aborted` and the reason `Lost lock: …`. Nothing the transaction
staged was committed, so the client restarts it. The proof also fails closed when the lock's key now
routes to another partition (the range moved) and when no leader confirms the term after a few attempts.

```
Refusing to commit transaction HLC(1:…): partition 2 is no longer led under term 3, in which it granted this transaction a lock; the lock was dropped by the leader change
```

Two limits. A lock whose lease lapses without a leader change is not covered by this proof; the base
checks on the written keys still are. A node that predates the term report answers none, and the locks
it granted are not checked.

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

This is a count-based fingerprint, **not a hash of values or a complete equivalence proof**.
Equal counts cannot detect different values or a different key/intent set of the same size.
An inconclusive comparison does not certify a replica.

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
  applied id to the checkpoint entry, so the fingerprints line up at the next committed entry).
  Recovery time depends on checkpoint, export, staging, installation and quorum availability; there
  is no subsecond operational guarantee. See the [snapshot guide](snapshot-and-raft-recovery-guide.md).

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
| `kahuna.transactions.lock_grant_term_changes` | counter | Transactions whose lock grants on one partition reported two leadership terms |
| `kahuna.transactions.lost_lock_aborts{detected}` | counter | Commits refused because a lock could not be proven held: `regrant`, `commit_probe`, `range_moved`, `unconfirmed`, `bundle_apply`, `lease_lapsed` |
| `kahuna.durable_tx.one_phase_gated_commit_leader_change_rejections` | counter | One-phase bundled commits rejected at apply because another term than the lock grants' proposed them |
| `kahuna.range.split.incomplete_source_refusals` | counter | Splits refused because the source leader's state was incomplete |
| `kahuna.durable_tx.ordered_apply_waits_released_on_leadership_loss` | counter | Durable completions released because the node stopped leading the partition |
| `kahuna.durable_tx.ordered_apply_wait_timeouts` | counter | Durable completions that never saw the ordered apply of their committed entry |
| `kahuna.durable_tx.ordered_apply_results_displaced` | counter | Durable completions that read their acknowledgement back from the store |

Alert on any increase of the divergence counters; `apply_divergence_contained{action=gated}` without a
matching `apply_divergence_repaired` within a few minutes is a re-seed that is not landing (check the
leader's `Re-seed of …` and the replica's `Re-seed request expired` lines).
