# Kahuna replication factor operations guide

This guide explains how to operate Kahuna with a **per-partition replication factor** — what it
buys, how to choose a factor, how to turn it on, how to read and steer the placement table, what a
replica move looks like while it is happening, and what to watch. It is written for operators
running a clustered Kahuna deployment.

Related guides: [cluster membership operations](cluster-membership-operations-guide.md) (roster,
join/leave, readiness), [leader balancing](leader-balancing-operations-guide.md) (which replica
*leads*, as opposed to which nodes *hold* a range), and
[backups and point-in-time recovery](backups-and-point-in-time-recovery-guide.md) (coverage rules
under a replication factor).

---

## 1. What it is, in one minute

By default every Kahuna voter node hosts **every** partition — full replication. Adding a node adds
capacity to serve reads and spread leaders, but not storage capacity: every node still stores the
whole keyspace, and every write is replicated to every voter.

With a replication factor (RF), each data partition gets an explicit **replica set** of RF nodes.
Only those nodes store the partition's data and participate in its Raft quorum. A six-node cluster
at RF 3 stores each range on three nodes: half the storage and write fan-in per node, and adding
nodes now adds real capacity.

Two things do **not** change:

- **Any node still serves any key.** A node that does not host a key's partition forwards the
  operation to a node that does. Clients keep connecting to whatever node they like.
- **Consistency.** Quorum is computed per range over its voter replicas; commits, transactions,
  locks, and fencing tokens keep exactly the guarantees they have under full replication.

RF 0 (the default) is full replication, bit-for-bit the pre-placement behavior.

---

## 2. Choosing a factor

- **Prefer odd values.** An even factor tolerates no more failures than the next odd factor down —
  a quorum of 4 is 3, the same single-failure tolerance as 3 replicas — while paying for an extra
  copy. The server logs a warning at startup for even factors.
- **RF 3 is the standard choice**: survives one node failure per range, three copies of the data.
  RF 5 survives two failures at five copies.
- **RF larger than the cluster** degrades gracefully: ranges hold one replica per available node
  until enough nodes join (a startup warning says so). RF equal to the node count is just full
  replication with extra bookkeeping.
- **Zones.** Set `--raft-zone <name>` on each node (rack, availability zone) and the placement
  planner prefers spreading each range's replicas across distinct zones, so a zone outage does not
  take out a whole quorum.

---

## 3. Turning it on (new cluster)

Set the factor on **every** node at bootstrap:

```bash
kahuna-server \
  --raft-replication-factor 3 \
  --raft-enable-placement-rebalancer true \
  --raft-zone rack-a \
  ...
```

Embedded hosts use the equivalent `EmbeddedKahunaOptions` properties (`ReplicationFactor`,
`EnablePlacementRebalancer`, `Zone`, `PlacementPassInterval`, `MaxConcurrentReplicaTransfers`,
`MaxConcurrentReplicaRepairs`).

**The leader balancer is not required.** Placement runs its own controller pass on its own
`--raft-placement-pass-interval` (default 5 s), so a cluster with `--raft-enable-leader-balancer`
off — the default — still repairs, trims and rebalances replicas. Earlier builds scheduled the pass
from the leader balancer's timer, which meant a placement-only cluster never ran one: replica sets
sat unchanged through node departures and replication-factor overrides alike.

- **Initial placement is applied at bootstrap regardless of the rebalancer switch**: each data
  partition gets a replica set of RF nodes, spread evenly by replica count (and by zone when zones
  are configured).
- `--raft-enable-placement-rebalancer` is the master switch for **ongoing** moves: repairing
  under-replicated ranges when a node dies, trimming over-replication, and smoothing skew as nodes
  join and leave. With it off, in-flight moves still finish, but nothing new is planned.
- Load reports (leader hints, zone gossip) are enabled automatically whenever a replication factor
  is set.

Each node logs a one-line banner when its first partition map applies, so you can confirm the mode
from the logs:

```
Partition placement: replication factor 3 (per-partition placement), rebalancer enabled, hosting 4 of 8 partitions
```

The readiness probe (`GET /v1/cluster/health`) reports the same count as an informational
`hostedPartitions` field. **A node hosting zero data partitions is still ready** — it serves every
key by forwarding — so never gate traffic on that number.

---

## 4. Reading the placement table

Every node answers with the same committed map; only the "hosted here" perspective differs.

```bash
kahuna-cli --cluster-placement
kahuna-cli --cluster-placement --format json    # machine-readable
```

or over REST: `GET /v1/cluster/placement`. Per partition you get:

| Column | Meaning |
|---|---|
| **State** | `Active` serving; `Draining`/`Removed` appear transiently around range splits and merges. |
| **Generation** | Bumps on every placement or split/merge change. Two snapshots with the same generation describe the same placement — useful for scripting "did anything move?". |
| **Effective RF** | The per-range override when one is set, else the global factor. 0 = full replication. |
| **Hosted here** | Whether the answering node materializes this partition locally. |
| **Replicas** | The committed replica set with roles. Empty means legacy full replication (every voter hosts it). |

Replica roles:

- **Voter** — full member; counts toward the range's quorum.
- **Learner** — catching up after being added; receives replication but is excluded from quorum
  and never campaigns. Promoted to Voter automatically once caught up.
- **Removing** — marked for removal; still serves while it counts down to the final drop.

A range never has more than one Learner/Removing replica at a time (single mover per range), so
successive configurations always overlap by a quorum.

---

## 5. Steering placement

### Removing a node: it drains first

`POST /v1/cluster/leave` on a placed node does **not** simply drop it from the roster. Its replicas
are evacuated onto survivors first, and only then does the removal commit — otherwise every range it
held would silently be a replica short the moment you stopped the process. The response's `drained`
field reports whether evacuation actually happened; `--raft-decommission-drain-timeout` (default
2 minutes) bounds the wait, and only one node may drain at a time. The full outcome table is in the
[cluster membership guide](cluster-membership-operations-guide.md#5-removing-a-node).

A node lost *without* a leave (crash, or eviction by the failure detector) gets the same replicas
restored, but after the fact: the planner sees the under-replicated ranges and repairs them at
priority 1, paced by `--raft-max-concurrent-replica-repairs`.

### Per-range replication-factor override

```bash
kahuna-cli --set-replication-factor 5 --partition 3
kahuna-cli --set-replication-factor 0 --partition 3    # clear: inherit the global factor
```

REST: `POST /v1/cluster/replication-factor` with `{"partitionId": 3, "replicationFactor": 5}`.

- The override is a partition-map mutation, so **only the meta-partition leader accepts it**. A
  follower refuses with the reason; the CLI tries each connected endpoint until one commits (or
  target a node with `--node`).
- The change adjusts the **target only**. The rebalancer moves replicas toward it on later passes
  (so with the rebalancer off, the target changes and nothing else happens). Routing is unchanged
  until replicas actually move.
- With the rebalancer on, a pass is kicked as soon as the change commits: watch the range's replica
  set and generation in `GET /v1/cluster/placement` to see it converge, and the losing nodes log
  `Stopped hosting`. If the target changes and the replica set never does, that is a bug, not
  pacing — the pacing bound is in §5.

### Ranges created by a split

A key-range split creates a **new partition**, which the placement planner then places like any
other: it enters the map with a replica set of RF nodes and shows up in `GET /v1/cluster/placement`
with the generation the cutover committed. The split response reports the destination partition id,
so the two views join up — `--split-range` tells you which partition now serves the upper half, and
`--cluster-placement` tells you which nodes serve that partition. Administering splits and merges
is covered in the
[key-range sharding guide](key-range-sharding-guide.md#9-administering-ranges-from-outside-the-process).

### Rebalancer pacing

| Flag | Default | What it bounds |
|---|---|---|
| `--raft-placement-pass-interval` | 5000 ms | How often the controller pass runs. Independent of the leader balancer's interval. Every relocation costs several passes, so this sets the floor on convergence speed. |
| `--raft-max-replica-moves-per-pass` | 4 | New moves initiated per controller pass, across all priorities — the blast radius of a bad plan. Keep it at or above repairs + transfers, or it binds first and starves repairs. |
| `--raft-max-concurrent-replica-repairs` | 3 | In-flight **repair** moves: re-replicating under-replicated ranges and shedding replicas stranded on departed nodes. Separate from the balance budget so restoring durability is never serialized behind cosmetic rebalancing. |
| `--raft-max-concurrent-replica-transfers` | 1 | Ranges with an in-flight Learner/Removing replica initiated by **balance** moves — caps concurrent backfill so skew-smoothing never starves client traffic. |
| `--raft-replica-count-deadband` | 1 | Per-node imbalance tolerated above the even spread before balancing moves are planned. Under-replicated ranges bypass the deadband. |

An in-flight transitional replica counts against **both** budgets, so total concurrent transfers
stay bounded by the larger of the two and balance moves pause while a repair wave runs.

A controller pass also runs immediately after the two events that create placement work — a
committed replication-factor change and a committed roster removal — so neither waits out a full
interval before converging.

---

## 6. How a replica move proceeds

A move is a sequence of committed map changes, driven by the placement controller on the
meta-partition leader. Every step survives a controller crash — the new leader re-derives all
in-flight moves from the committed map.

1. **Add** — the target node enters the range's replica set as a **Learner**. It materializes the
   partition and starts receiving replication.
2. **Seed** — the learner catches up from the leader's log; if the log has already been compacted
   below what it needs, it is seeded with a **whole-partition snapshot** (the full key-value,
   lock, and transaction state for that partition) and then replays the retained tail.
3. **Promote** — once the learner's lag has stayed within `--raft-learner-promotion-lag` (default
   10 entries) for `--raft-learner-promotion-stable-window` (default 3 s), it is committed as a
   **Voter**. Quorum now includes it.
4. **Remove** — for a move (as opposed to a repair), the outgoing replica is marked **Removing**
   (out of quorum, still serving), then dropped from the set by a second commit.
5. **Purge** — the dropped node stops hosting the partition: the consensus layer reclaims its
   write-ahead log, and Kahuna purges the partition's key-value rows, resident lock leases,
   transaction bookkeeping, and durability floors. A crash mid-purge is repaired on the next
   startup from the committed map.

Client-visible effect: operations on a range whose leadership or hosting changes mid-flight answer
the retryable `MustRetry`; clients that follow the documented retry contract see latency, not
errors.

**How long does convergence take?** Repairs run at most `--raft-max-concurrent-replica-repairs` at
a time and balance moves at most `--raft-max-concurrent-replica-transfers`, and each move costs
roughly *seed time* (data-volume dependent; the dominant term) plus the promotion stable window
plus two map commits — and, because each stage is decided on a separate pass, at least three
`--raft-placement-pass-interval` ticks. N pending moves therefore take about
`N / concurrency × (seed + stable-window + 3 × pass-interval)`. Raise the matching cap to converge
faster at the cost of more concurrent backfill traffic; shorten the pass interval when the tick
term dominates, which it does whenever ranges are small.

---

## 7. What to watch

Metrics (OpenTelemetry counters on the `Kahuna` meter — all flat at RF 0):

| Metric | Healthy looks like |
|---|---|
| `kahuna.placement.replicas_gained` / `replicas_lost` | Moves only when you expect them (node joins/leaves, factor changes). A steady trickle with a stable roster means the planner is thrashing — check the deadband. |
| `kahuna.placement.forwards_resolved` | Proportional to how much traffic lands on non-hosting nodes. High absolute values are fine; they just mean clients are not topology-aware. |
| `kahuna.placement.forwards_unresolved` | ~0. Sustained growth means nodes cannot resolve where partitions live (placement views lagging badly). |
| `kahuna.placement.leader_hint_hits` / `leader_hint_misses` | Miss rate (`misses/(hits+misses)`) should drop toward ~0 shortly after startup as gossip converges. A permanently high miss rate adds a redirect hop to every forwarded operation. |

Logs at Information: the startup banner (§3), and `Started hosting N partition(s): …` /
`Stopped hosting N partition(s): …` on every replica transition.

---

## 8. Backups under a replication factor

A backup captures the **taking node's** data — under placement, only the partitions that node
hosts. The manifest records both the cluster's partition set and the covered subset:

- Taking a backup on a placed cluster **succeeds** and records its restricted coverage.
- **Restore and chain validation refuse** an artifact set whose covered partitions do not reach
  every cluster partition, naming the missing ones (`RestrictedCoverage`). A partial restore would
  be indistinguishable from data loss, so it fails closed before touching the destination.
- Composing a cluster-wide backup from per-node artifacts is **not supported yet**; until it is,
  whole-cluster backups require a node that hosts everything (RF 0, or RF ≥ node count).

PITR WAL-retention floors are tracked per hosted partition and are cleaned up automatically when a
partition leaves a node. See the
[backups guide](backups-and-point-in-time-recovery-guide.md) for the full backup story.

---

## 9. Migration note: enabling RF on an existing cluster

**There is currently no in-place migration from full replication to placed ranges.** Restarting an
existing cluster with `--raft-replication-factor 3` changes the configured factor, but the
already-committed ranges keep their **empty replica sets** — and an empty replica set means legacy
full replication, permanently:

- The placement planner only considers ranges that already have a replica set; legacy ranges are
  invisible to it, so the rebalancer never trims them down to RF.
- Replica mutations refuse legacy ranges outright ("assign an initial placement first"), so they
  cannot drift into a half-placed state by accident.

What an operator should expect to see after such a restart: the startup banner reports the new
factor, `--cluster-placement` shows every existing partition with an empty replica set
("all voters (full replication)") and the effective RF, and nothing moves. This **mixed mode is
safe** — legacy ranges keep behaving exactly as before — it just does not deliver the storage
savings.

The supported path to a placed cluster today is to **bootstrap a new cluster with the factor set**
and move the data into it (via the client, or via backup/restore subject to the coverage rules in
§8). An operation that assigns an initial placement to a legacy range — enabling true in-place
migration — is future work in the consensus layer.

---

## 10. Current limits, in one place

- No in-place migration from full replication (§9); RF applies to clusters bootstrapped with it.
- No composed cluster-wide backup from per-node artifacts (§8); restores require full coverage.
- Per-range RF override changes the target only; convergence needs the rebalancer on (§5).
- A node hosting zero partitions is normal and ready (§3) — do not alarm on it.
- Only one node can drain at a time; scale down in sequence, not in parallel (§5).
