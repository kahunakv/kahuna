# Load-based range splitting — operations guide

This guide is for operators (and embedders such as CamusDB) who want a hot Raft partition to relieve
itself by **splitting under load**, not just under size. It explains what the feature does, the one
prerequisite that makes or breaks it, how to enable and tune it, and how to tell from metrics whether
it is working, thrashing, or silently doing nothing.

It builds on two siblings — read those first if the concepts are new:
- [Key-range sharding guide](key-range-sharding-guide.md) — how ranges, descriptors, and splits work.
- [Leader balancing operations guide](leader-balancing-operations-guide.md) — how leadership is
  redistributed across nodes.

The feature is **off by default**. Existing deployments are unaffected until you opt in.

---

## 1. The 60-second model

Kahuna shards a `KeyRange` key space into range descriptors, each backed by its own Raft partition.
Historically a range split only when it grew past a **key-count** threshold. That misses the case that
actually limits a NewSQL workload: a range that is **small but white-hot** — high write QPS concentrated
on a narrow key span. It never grew, so it never split, so its single leader stayed the bottleneck.

Load-based splitting adds a **second trigger**: split a range when its partition is **hot and backlogged
for a sustained window**, regardless of key count, placing the split at the **write centroid** (the key
that bisects the write traffic) so each half carries about half the load.

There are now two independent split triggers; either can fire:

| Trigger | Fires on | Cadence | Motivation |
|---|---|---|---|
| **Count branch** | sampled key count ≥ `RangeSplitThreshold` | slow (~`CollectionInterval`, 60s) | size / storage distribution |
| **Load branch** | rate **and** saturation sustained for `RangeSplitLoadWindow` | fast poll (~`RangeSplitLoadPollInterval`, 5s) | hotness / write-throughput relief |

---

## 2. The one prerequisite that makes or breaks it: enable the leader balancer

> **Load-based splitting requires `EnableLeaderBalancer = true`. With the balancer off, the feature is
> silently inert for the partitions it is meant to relieve.**

There are two independent reasons, and both bite:

1. **You can't see the load.** The per-partition load signals (ops/sec, WAL queue depth, commit-wait)
   are only **gossiped across the cluster when the balancer is on**. The split trigger runs on the
   meta/system-partition leader and evaluates ranges whose leaders live on *other* nodes — and for those
   it reads `0` ("not hot") unless the balancer is gossiping. With the balancer off, only ranges led by
   the trigger node itself are even visible.
2. **A split with nowhere to go relieves nothing.** Splitting a fsync-bound partition into two leaders
   **on the same node** adds Raft consensus overhead and relieves *zero* throughput — the two halves
   still share one disk and one fsync. Relief only happens when the new child's leader is **relocated to
   a different, less-loaded node**, and that relocation is the balancer's job.

So the deployment rule is simple: **turn the balancer on whenever you turn load splitting on.** Kahuna
guards the clearest failure (a single-node cluster, or all peers silent) by skipping the split and
incrementing `kahuna.range.split.no_relief_skips` — but it cannot force the balancer on for you.

---

## 3. Enabling it

Minimum viable configuration (embedded host shown; the standalone server exposes the same knobs):

```csharp
var options = new EmbeddedKahunaOptions
{
    // ... your normal options ...

    // Turn on load-based splitting:
    RangeSplitLoadThreshold = 2000,    // writes/sec a partition must sustain to be a candidate
    EnableLeaderBalancer    = true,    // REQUIRED — see section 2

    // Optional: disable the count branch if you only want load-based splits
    // RangeSplitThreshold  = 0,
};
```

That's it. Everything else has a sensible default. The feature stays off as long as
`RangeSplitLoadThreshold == 0`.

---

## 4. How a load split is decided (so the knobs make sense)

For each `KeyRange` descriptor, the fast poll runs this gate. **All** conditions must hold, **continuously
for `RangeSplitLoadWindow`**, before a split fires:

1. **Rate** — `LogOpsPerSecond ≥ RangeSplitLoadThreshold`. *Magnitude:* how much log-replication work.
2. **Saturation** — `WalQueueDepth ≥ RangeSplitLoadMinQueueDepth`. *The critical second gate.* Rate
   alone is a trap: throughput plateaus at the fsync ceiling, so a partition at 1.1× capacity and one at
   10× capacity report the *same* rate. What distinguishes "busy but keeping up" from "drowning" is the
   **backlog**. A high rate with a near-zero queue is healthy — **don't split it**.
3. **(optional) Commit-wait** — if `RangeSplitLoadMinCommitWaitMs > 0`, also require
   `CommitWaitMs ≥` it. Off by default; it's a finer fsync-pressure signal but "sticky when idle," so it
   is only ever combined behind the rate gate.

If the gate holds for the full window, Kahuna samples the range, places the split at the **write
centroid**, and — unless the range is **indivisible** (essentially all writes on one key; see
`indivisible_refusals`) — performs the split. Both children then enter a **settle window** during which
neither is re-evaluated, giving the balancer time to relocate one of them first.

The **debounce window matters** because the gossiped signals lag ~10s; without it a single stale or
bursty report could trip a split. Keep `RangeSplitLoadWindow` ≥ ~10s.

---

## 5. The knobs

### Load-split knobs

| Knob | Default | What it controls |
|---|---|---|
| `RangeSplitLoadThreshold` | `0` (off) | Writes/sec a partition must sustain. **Set > 0 to enable.** |
| `RangeSplitLoadMinQueueDepth` | `8` | WAL backlog required alongside the rate. The saturation gate. |
| `RangeSplitLoadMinCommitWaitMs` | `0` (off) | Optional commit-wait (ms) floor. Leave off unless depth is too coarse. |
| `RangeSplitLoadWindow` | `15s` | How long the gate must hold before splitting (debounce). Keep ≥ ~10s. |
| `RangeSplitLoadPollInterval` | `5s` | How often the cheap load poll runs. Keep < the window. |
| `RangeSplitLoadImbalanceMax` | `0.8` | A range is "indivisible" if no split key gets each child below this fraction of the writes. |
| `RangeSplitSettleWindow` | `10s` | Post-split cooldown; neither child is re-evaluated until it elapses. **Must be ≥ `MinLeaderStability`** (validated at startup). |
| `RangeSplitIndivisibleCooldown` | `5min` | How long an indivisible range is skipped before the count branch re-samples it. |

### Count-split knobs (pre-existing)

| Knob | Default | What it controls |
|---|---|---|
| `RangeSplitThreshold` | `1000` | Key count that triggers a size-based split. Set `0` to disable the count branch. |
| `RangeSplitMinRangeSize` | `10` | Minimum keys each child must keep. |

### Balancer knobs (required for load splitting — see the [balancing guide](leader-balancing-operations-guide.md))

`EnableLeaderBalancer` (set `true`), `LeaderBalancerReportInterval` (5s), `LeaderBalancerInterval` (30s),
`LeaderBalancerReportTtl` (20s), `MinLeaderStability` (5s), `LeaderBalancerOpsWeight` (1.0),
`LeaderBalancerQueueWeight` (0.5).

**Validated constraints (Kahuna throws at startup if violated):**
- `RangeSplitSettleWindow ≥ MinLeaderStability` — otherwise the splitter would reconsider a child before
  the balancer is even allowed to move it, defeating the settle window.
- `LeaderBalancerReportInterval < LeaderBalancerReportTtl` — otherwise the balancer treats every node as
  silent and never rebalances.

---

## 6. Monitoring

Five counters are published under the **`Kahuna`** meter (OpenTelemetry / `dotnet-counters`), each tagged
with `keyspace`:

| Metric | Meaning | Watch for |
|---|---|---|
| `kahuna.range.splits` | Splits committed (count + load branches) | Steady growth then plateau = healthy relief. Never plateaus = workload outrunning splits. |
| `kahuna.range.split.no_relief_skips` | Load splits skipped: no peer node to host the child | **Any sustained value = load splitting is inert.** Single-node cluster, or balancer off / peers silent. |
| `kahuna.range.split.indivisible_refusals` | Splits refused: all writes on one key | Persistently high = a **hot-key** workload that splitting cannot relieve (re-key, don't expect splits to help). |
| `kahuna.range.split.settle_skips` | Descriptors skipped inside the settle window | Normal during active splitting. Never dropping to 0 may mean the settle window is too long. |
| `kahuna.range.merge.warm_skips` | Merges skipped because a side is still warm | The oscillation guard working — merge declined to coalesce an active range. |

---

## 7. Troubleshooting

**"It splits, but throughput doesn't improve."** The child isn't being relocated, so both halves still
share one node. Check `no_relief_skips` (rising = no target) and confirm `EnableLeaderBalancer = true`
and that the cluster has a less-loaded node. This is the known limitation in section 9.

**"It never splits a range I know is hot."** Most likely the balancer is off, so the trigger reads `0`
for that (remotely-led) partition — enable it. Otherwise the **saturation** gate isn't met: a high rate
with a shallow queue is "busy but keeping up," and Kahuna intentionally won't split it. Lower
`RangeSplitLoadMinQueueDepth` only if you're sure the partition is actually backlogged.

**"Ranges merge and re-split repeatedly."** Shouldn't happen — the merge trigger refuses to merge a warm
range (`warm_skips`). If you see it, a side's rate is below `RangeSplitLoadThreshold` (so "cold" to the
merge guard) but its centroid still splits; widen the gap or raise the threshold.

**"`indivisible_refusals` keeps climbing."** A single key (or tiny key span) holds essentially all the
writes. No split key can balance it — this is a workload/schema problem (a hot row, a monotonic counter),
not a tuning problem. Splitting cannot relieve a single hot key.

**"Startup throws about `RangeSplitSettleWindow` or `LeaderBalancerReportInterval`."** You violated a
validated constraint — see section 5.

---

## 8. What is and isn't counted

- **Reads are never counted.** Kahuna reads are served from local state and never hit the consensus log;
  splitting wouldn't relieve them. Read capacity is scaled by adding **replicas**, not by splitting.
- **Ephemeral writes are excluded.** They bypass Raft entirely, so a workload of ephemeral writes shows a
  ≈0 split rate and never load-splits. This is intended.
- **Only `KeyRange` spaces split.** The `Hash`-routed system/meta partitions are never load-split.

---

## 9. Limitations and safety

- **Placement is best-effort, not guaranteed (known limitation).** Kahuna skips a split when there is
  *no* peer at all, but it does **not** verify that the balancer will actually move the child to a colder
  node, nor that the balancer is even on. In a balancer-off cluster *with* live peers, a load split can
  still land in place and relieve nothing. Mitigation: follow the section-2 rule (enable the balancer),
  and watch `no_relief_skips` and `splits`-without-improvement. A fully placement-aware relief loop is a
  planned enhancement.
- **No storage relief.** Splitting distributes Raft **leadership/coordination** (the write-throughput
  ceiling), not bytes. Every replica still holds all keys. Storage partitioning is a separate effort.
- **Advisory and reversible.** The signals are advisory; a stale or imprecise value can at worst cause a
  suboptimal split decision, never a correctness problem. A split that fails mid-flight cleans up after
  itself (the partially-created partition is removed), and the range remains fully served throughout.
- **Off by default.** Nothing changes until you set `RangeSplitLoadThreshold > 0`.
