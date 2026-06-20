# Kahuna leader balancing operations guide

This guide explains how to operate Kahuna's **leader balancer** — what it does, when to turn it on,
how to configure it, and what to watch. It is written for operators running a clustered Kahuna
deployment (directly or as the storage engine behind a database such as CamusDB).

No prior knowledge of the internals is assumed. For the full mechanism — the planner, the suggestion
protocol, and the safety proofs — see the upstream
[Kommander leader balancer developer guide](https://github.com/.../leader-balancer-developer-guide.md)
(`docs/leader-balancer-developer-guide.md` in the Kommander repo); this guide is the operator-facing
companion focused on Kahuna's flags.

---

## 1. What it is, in one minute

Kahuna splits data into many **partitions**, each its own little Raft group with one **leader** that
does all the real work for that partition (writes, replication, heartbeats). Leaders are elected
independently and somewhat randomly, so over time they can pile up: one node ends up leading most
partitions while its peers sit idle. That node becomes a bottleneck even though you have spare
machines.

The **leader balancer** fixes this. It watches how leaders are spread across the cluster and, when one
node is carrying too much, politely asks busy nodes to hand some leaderships to idle ones — using
Kahuna's normal, safe Raft leadership-handoff path.

```
Before:                          After balancing:

 node A: ████████████ (12)        node A: ████ (4)
 node B: ██ (2)                    node B: ████ (4)
 node C: ██ (2)                    node C: ████ (4)
        ^ overloaded                      ^ evenly spread
```

It balances on two signals: first **count** (equalize how many leaders each node holds), then **load**
(spread the *hot* partitions, by throughput and queue depth, off the busy nodes).

> **It only moves leadership, never data.** Every transfer goes through the same Raft handoff Kahuna
> uses internally, which re-validates term and leadership at the moment of the move. The worst a
> wrong decision can do is cause a *pointless* move, which self-corrects on the next pass. There is no
> path to data loss or split-brain through this feature.

---

## 2. When should I enable it?

Enable it when **both** of these are true:

- You run a **multi-node cluster** with **many partitions** (the more partitions per node, the more
  there is to balance).
- You observe — or want to prevent — **leader skew**: one node hosting a disproportionate share of
  leaders, or the hot partitions clustering on one node.

**Leave it off (the default) when:**

- You run Kahuna **standalone / embedded** (single real node). The balancer has nothing to balance —
  it only runs on the system-partition leader and there are no peers to move work to. The flags are
  ignored in this mode.
- You have a small, already-even cluster. The balancer would simply stay quiet, but there is no
  benefit either.

> **Decision — off by default, fully advisory.** Leader balancing is an optimization, not a
> correctness feature. A cluster runs perfectly well without it; it just may run *lopsided*. Shipping
> it disabled means zero overhead and zero behavior change until you explicitly opt in.

---

## 3. How to enable it

Set `--raft-enable-leader-balancer true` on **every node** in the cluster, then do a rolling restart.

```
kahuna server \
  --raft-enable-leader-balancer true \
  ... (your usual flags) ...
```

> **Enable it on all nodes, not just one.** Each node emits a load report and acts on suggestions
> addressed to it; the planning itself runs only on the leader of the system partition (P0), which can
> be any node and can move. If only some nodes have the feature on, balancing is partial and the
> reports are incomplete — which the planner detects and responds to by skipping passes. Roll it out
> everywhere, ideally in the same deploy.

When enabled, every node periodically gossips a small **load report** (what it leads and how busy each
one is), the P0 leader runs a **planning pass** on a timer, and it dispatches **suggestions** —
"please consider handing partition P to node C" — to the nodes that currently lead the partitions it
wants moved. Each recipient re-validates and performs the handoff itself. Nothing is forced; nothing
is durable; if the P0 leader changes, the new one simply starts fresh from the next round of reports.

---

## 4. Configuration reference

All flags below take effect only when `--raft-enable-leader-balancer` is `true`. Defaults are
deliberately conservative — slow and gentle. **Start with the defaults.**

| Flag | Default | What it controls |
|---|---|---|
| `--raft-enable-leader-balancer` | `false` | Master switch. When off: no reports, no passes, zero overhead. |
| `--raft-leader-balancer-interval` | `30000` ms | How often the P0 leader runs a planning pass and dispatches moves. |
| `--raft-leader-balancer-report-interval` | `5000` ms | How often each node emits its load report on the gossip path. |
| `--raft-leader-balancer-report-ttl` | `20000` ms | A report older than this is ignored (the node is treated as silent). Must exceed the report interval. |
| `--raft-count-deadband` | `1` | How far above the ideal per-node leader count a node may drift before it's acted on. Anti-oscillation. |
| `--raft-load-imbalance-threshold` | `0.25` | How skewed load must be (when counts are already even) to trigger a count-neutral swap. |
| `--raft-min-leader-stability-ms` | `5000` ms | A leadership younger than this is not eligible to move (don't reshuffle a just-elected leader). |
| `--raft-move-cooldown` | `60000` ms | After a partition moves, how long before it can move again. Prevents ping-ponging. |
| `--raft-max-moves-per-pass` | `4` | Most moves planned in a single pass. Limits blast radius. |
| `--raft-max-concurrent-transfers` | `2` | Most moves in flight cluster-wide at once. |
| `--raft-suggestion-timeout` | `15000` ms | How long to wait for a suggested move to show up before giving up and retrying later. |
| `--raft-leader-balancer-ops-weight` | `1.0` | Weight of throughput (ops/sec) in a partition's load score. |
| `--raft-leader-balancer-queue-weight` | `0.5` | Weight of queue depth (pending work) in a partition's load score. |

Balance is reached **gradually over several passes**, never in one disruptive burst — that is what
the deadband, cooldown, stability gate, and per-pass caps enforce together.

---

## 5. Tuning

- **Start with defaults.** They converge slowly and gently. Only tune if you have a specific symptom.
- **Rebalancing feels too sluggish?** Lower `--raft-leader-balancer-interval`, or raise
  `--raft-max-moves-per-pass` / `--raft-max-concurrent-transfers`. Expect more churn in exchange.
- **The balancer is fidgeting (moving things back and forth)?** Raise `--raft-count-deadband` and/or
  `--raft-load-imbalance-threshold`. These are your anti-oscillation knobs. Raising
  `--raft-move-cooldown` also helps.
- **Keep `--raft-suggestion-timeout` comfortably larger than `--raft-leader-balancer-report-interval`
  plus your gossip propagation time.** If it's too tight, *successful* moves get mis-counted as
  failures — their "I'm the new leader" report hasn't reached the P0 leader yet — and get needlessly
  retried. On larger clusters, raise it.
- **Tuning the load score:** raise `--raft-leader-balancer-ops-weight` to make balancing more
  sensitive to throughput differences, or `--raft-leader-balancer-queue-weight` to react more to
  pending-work backlogs. Setting queue weight to `0` makes balancing purely throughput-driven.

---

## 6. Observability

The balancer is advisory and quiet by design — it logs little and never errors — so you confirm it is
working by **metrics**, not logs. Kommander emits four instruments under its meter (meter name
`Kommander`):

| Metric | Type | What it tells you |
|---|---|---|
| `raft.balancer.moves_total` | counter | Moves tagged by outcome: `planned`, `succeeded`, `timed_out`. |
| `raft.balancer.skipped_passes_total` | counter | Passes skipped because the load-report view was incomplete. |
| `raft.balancer.count_imbalance` | gauge | How far the most-loaded node is above the ideal count. Trends to ~0 as it converges. |
| `raft.balancer.load_imbalance` | gauge | Fractional load skew across nodes. Also trends down. |

**Reading them:**

- **Healthy:** a burst of `planned` followed by `succeeded`, and both imbalance gauges drifting toward
  zero, then going quiet once balanced.
- **High `timed_out` rate:** suggestions aren't landing — recipients are dropping them, or
  `--raft-suggestion-timeout` is too tight for your gossip latency.
- **High `skipped_passes_total`:** the view is often incomplete — a node may be silent, or the report
  TTL/interval are mismatched (TTL must comfortably exceed the report interval).
- **Nothing moving at all:** check the two counters above, and confirm the imbalance actually exceeds
  `--raft-count-deadband` — if the cluster is already near even, there is correctly nothing to do.

> The two imbalance gauges are meaningful **only on the current P0 leader** (other nodes report `0`).
> Tag or filter your dashboards by node, and remember the P0 leader can move.

> **Exporting these metrics:** the instruments above are always emitted by the underlying library, but
> Kahuna must be configured to *export* a meter for an operator to see them. If your Kahuna build does
> not yet expose a metrics endpoint/exporter, the balancer still works — it is simply running blind
> from the operator's side. Surfacing the `Kommander` (and Kahuna's own) meters is tracked separately
> in the metrics work; consult your deployment's metrics configuration for whether an OTLP/scrape
> exporter is enabled.

---

## 7. Safety and failure modes

The one property worth internalizing: **the balancer can only ever cause a wasted or skipped move,
never an unsafe one.**

- Every leadership change goes through the normal Raft handoff, which re-validates term and leadership
  at the moment of the move. The balancer only *suggests*; the node that owns the partition is the
  final authority and silently ignores any suggestion that no longer applies (e.g. it already lost
  leadership of that partition).
- There is a **single decider** (the P0 leader), so two nodes can't issue conflicting plans.
- An **incomplete picture makes the controller wait**, not guess.
- All balancer state is **in memory**; losing the P0 leader just means the next one starts fresh — at
  worst one extra pass before it re-converges.
- During a node failure or election, a partition with no current leader simply doesn't appear in any
  report, so the balancer ignores it that pass. It never starts or speeds up elections — it only
  relocates leaderships that already exist.

In short, the failure modes are "didn't rebalance" or "rebalanced something pointlessly," and both
self-correct on the next pass.

---

## 8. FAQ

**Does enabling this risk my data?**
No. It moves only *leadership*, never data, through the same safe handoff Raft uses internally.

**Will it constantly move things around?**
No. Cooldowns, deadbands, stability gates, and per-pass caps make it gentle and convergent. Once the
cluster is balanced it goes quiet until something changes.

**Do I need to enable it on every node?**
Yes — see §3. Partial rollout yields incomplete reports and partial balancing.

**It's on but nothing is moving — is it broken?**
Probably not. Check `raft.balancer.skipped_passes_total` (incomplete view — a node may be silent) and
`raft.balancer.moves_total{outcome=timed_out}` (suggestions not landing — possibly the suggestion
timeout is too small). Also confirm the imbalance is actually beyond `--raft-count-deadband`.

**Does it work in standalone / embedded mode?**
No effect. There is a single real node and no peers to balance to; the flags are ignored.

**Where does the planning run?**
Only on the leader of the system partition (P0). Every other node just emits load reports and acts on
the suggestions it receives. The P0 leader can be any node and can change over time.
