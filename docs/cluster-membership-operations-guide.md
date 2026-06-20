# Kahuna cluster membership operations guide

This guide explains how a Kahuna cluster's set of nodes changes over time — adding a node, removing
one, replacing a failed one — and how the cluster keeps itself consistent while that happens. It is
written for two audiences:

- **Operators** running a multi-node Kahuna cluster who need to add capacity, decommission a node,
  recover from a failure, and see who is in the cluster right now.
- **Developers** maintaining the code who need the mental model and the invariants that must hold.

No prior knowledge of Kahuna's internals is assumed. Concepts are introduced as they come up, and the
key design decisions are called out with the reasoning behind them.

> **Scope.** This guide is about **multi-node clusters**. The embedded/standalone single-node mode is
> deliberately unaffected by everything here — none of the flags below apply to it.

---

## 1. The big picture

A Kahuna cluster is a set of nodes that agree, by consensus, on both your data *and* on **who the
members are**. That second part — the membership roster — is not a static config file you edit by
hand. It is a **committed record inside the cluster**, changed one node at a time through the same
consensus mechanism that protects your data.

That single idea drives everything in this guide:

- Adding a node is a *committed change* to the roster, not a restart of every peer.
- Removing a node is a *committed change* too — whether the node leaves politely or is detected dead.
- Because membership is consensus-backed, the cluster never loses track of who can vote, and it
  refuses any change that would break its own ability to make decisions.

> **Decision — membership is a committed record, not a hand-edited peer list.** The alternative —
> every node carries a static list of its peers — means adding a node requires editing and restarting
> all of them, and any disagreement between those lists is a split-brain risk. Making the roster a
> consensus record means there is exactly one authoritative answer to "who is in the cluster," it
> changes atomically, and a joining or leaving node can never inflate or corrupt the vote count.

---

## 2. The moving parts (membership in 60 seconds)

A few terms you need for the rest of the guide.

**Roles.** Every node in the roster has one of these roles:

| Role | Meaning |
|---|---|
| **Voter** | A full member. Counts toward quorum; can be elected leader. |
| **Learner** | A node that has joined but is still catching up. Receives data but does **not** vote yet. |
| **Leaving** | A node that has begun a graceful exit. On its way out; no longer counted as a full member. |
| **NotMember** | Not part of the cluster. |

**Quorum** is a strict majority of **Voters** — and *only* Voters. Learners, leaving nodes, and any
gossip/discovery chatter never enter the math. A 3-Voter cluster needs 2 to make progress; a 5-Voter
cluster needs 3.

**The system partition (P0).** Beyond your data partitions, the cluster keeps a special internal
partition that holds the membership roster. Its leader is the node that commits roster changes. When
this guide says "the cluster commits a membership change," it means a write to this partition.

**How a node becomes a full member — the Learner→Voter path:**

```
   new node                     existing cluster
   ────────                     ────────────────
      │   join (seed-based)            │
      ├──────────────────────────────►│   commit: add as Learner
      │                               │   (does NOT change quorum)
      │   ◄── backfill: log entries ──┤   Learner catches up
      │                               │
      │   caught up + stable a while  │   commit: promote to Voter
      │   ◄────────────────────────── │   (now counts toward quorum)
      ▼                               ▼
   serving as a Voter
```

A node always enters as a **Learner** first, catches up by receiving log entries (called *backfill*),
and is **automatically promoted** to Voter once it is close enough to current and has stayed stable
for a short window. The existing Voters keep quorum the entire time — the new node doesn't count until
it's ready.

> **Concept — why join as a non-voting Learner first?** If a brand-new, empty node counted as a Voter
> immediately, it would change the quorum math before it had any data — and could even be needed to
> form a majority while still catching up, stalling the cluster. Entering as a Learner means a join is
> always safe: the cluster's ability to make decisions is never weakened by a node that isn't ready.

---

## 3. Two ways a node enters the cluster

There are two distinct entry paths, and choosing the right one matters.

### Greenfield boot — forming a fresh cluster
When you start a brand-new N-node cluster, each node is given the initial peer list
(`--initial-cluster`) and they discover each other and seed an all-Voters roster on first leadership.
This is the default; you do nothing special. Use it **only** when standing up a cluster from scratch.

### Runtime join — adding a node to a cluster that is already running
To add a node to a **live** cluster, start it with **`--join-existing`** and point `--initial-cluster`
at some existing members (used as *seeds* to contact). The node calls in, is admitted as a Learner,
catches up, and is promoted — while the existing Voters keep serving throughout.

```
  kahuna --join-existing \
         --initial-cluster "node-a:8001,node-b:8001,node-c:8001" \
         ...other flags...
```

> **Decision — the join mode is an explicit flag, never guessed.** A node could in principle try to
> infer "am I forming a new cluster or joining an existing one?" from what it sees — but guessing wrong
> is catastrophic: a node that thinks it's greenfield can seed a *second*, competing roster. Making it
> an explicit choice (`--join-existing` or not) means a fresh node can never accidentally fork the
> cluster. The seed list reuses `--initial-cluster` so there is only one place to configure endpoints.

---

## 4. Adding a node — step by step

1. **Provision the node** with its identity (node id/endpoint) and the same cluster configuration
   (partition count, storage, etc.) as the rest of the cluster.
2. **Start it with `--join-existing`** and `--initial-cluster` listing one or more reachable current
   members as seeds.
3. The node blocks during startup until it has been **promoted to Voter** — it does not begin serving
   as a full member until it's caught up. (If it cannot be promoted, startup fails loudly rather than
   running half-joined — see §8.)
4. **Verify** with the roster read (§7): the new endpoint should appear as `Voter`, and the membership
   version should have advanced.

That's the whole flow. You do **not** restart or reconfigure the existing nodes.

---

## 5. Removing a node

### Graceful leave (decommissioning)
A node can commit its own removal on shutdown so the roster shrinks immediately instead of waiting for
failure detection. This is **off by default** and enabled with **`--graceful-leave-on-shutdown`**. Use
it when you are **retiring** a node for good.

> **Decision — graceful leave is OFF by default, and that is deliberate.** The tempting default is
> "always leave cleanly on shutdown." But a **rolling restart** — bouncing each node to deploy a new
> version — is also a shutdown, and you absolutely do **not** want each restart to evict the node from
> the cluster and then re-admit it. So leave is opt-in and means one thing: *this node is going away
> for good.* A plain restart (the common case) leaves the roster untouched.

### The safety floor: the cluster won't let you remove the last Voter
A graceful leave that would drop the cluster to **zero Voters** is **refused** — the cluster tells the
node "no" and the node simply stops without committing a removal. Going from 2 Voters to 1 is allowed;
going from 1 to 0 is not.

> **Concept — refusing the last-Voter leave.** Committing "remove the last Voter" would leave the
> cluster with no one able to make decisions — permanently bricked, with no quorum even to *undo* the
> removal. The cluster treats this as a terminal refusal: it never retries, and the leaving node, on
> seeing the refusal, just shuts down. Your data and roster are left intact.

### Automatic eviction of a dead node (SWIM)
If a node crashes or becomes unreachable **without** leaving, the cluster detects it through a gossip +
failure-detector layer (SWIM) and evicts it through the committed roster path. Detection is governed by
a few timeouts (§9): the cluster pings members, marks an unresponsive one *suspect*, waits out a
suspicion window, then waits a final grace period before committing the removal.

> **Concept — suspicion and grace exist to absorb hiccups.** A node that's briefly slow (a GC pause, a
> network blip) is not dead. The suspicion window and eviction grace give it time to respond before the
> cluster commits an irreversible "dead" verdict. Set these too tight and you'll evict healthy nodes on
> transient stalls; too loose and you'll carry a dead node longer. §9 covers tuning.

---

## 6. Replacing a node

Replacing a failed node is just the two operations above, in order:

1. The old node is gone (crashed). Either let SWIM evict it (§5), or, if you took it down on purpose,
   it may already have left.
2. **Add the replacement** as a fresh runtime join (§4).

There is one caveat that can bite a long-running cluster: if the failed node was down for a *long*
time, or you're seeding a fresh node into a cluster that has been running for a while, the join can
fail because the cluster has already discarded the old log the new node would need to catch up from.
That's the compaction floor — §8.

---

## 7. Seeing who is in the cluster

The roster is readable three ways; all return the same thing — the membership version, every member
(endpoint, node id, role, the version at which it joined), and **this** node's role.

- **CLI (the operator's tool):**
  ```
  kahuna.control --cluster-members
  kahuna.control --cluster-members --format json     # machine-readable
  ```
  In the interactive console, type `cluster members`.
- **REST:** `GET /v1/cluster/membership`
- **gRPC:** the `Cluster.GetMembership` call

The human-readable CLI output prints a version/local-role header and a table with the role
colour-coded (Voter green, Learner yellow, Leaving red). The `--format json` form emits compact JSON
suitable for piping into other tools.

Use this to confirm a join completed, watch a leave take effect, or check the roster after a suspected
failure.

---

## 8. The compaction floor (the one caveat that matters)

To bound disk use, each node periodically **compacts** its log — discarding old entries it no longer
needs. A joining Learner catches up by replaying log entries from where it left off. **If the cluster
has already compacted past the point a Learner needs to start from, there is nothing to replay it
forward from, and the join cannot complete.**

When this happens, the join does **not** hang silently or run a half-joined node — it **fails fast with
a clear error** pointing at the compaction floor as the cause. That's the signal to use one of the two
remedies:

- **The general fix** is an upstream snapshot-transfer mechanism (ship the whole state to the new node
  instead of replaying the log). This is tracked but not yet available.
- **The available remedy today** is to **seed the node from a recent backup** before joining. If you
  restore the node from a point-in-time backup taken *recently enough*, its starting point lands
  **above** the compaction floor, and the normal catch-up (backfill) finishes the job. See the
  [backups and point-in-time recovery guide](backups-and-point-in-time-recovery-guide.md) — the
  "restoring a node vs. adding it to the cluster" section explains how a restored image and a cluster
  join fit together.

> **Concept — why a restore + join, not just a restore.** A restored backup gives the node the *data*
> as of some recent moment, but it does not make the node a *member* — membership is a separate
> committed step. The restore gets the node's starting point above the compaction floor; the join
> admits it and lets normal catch-up cover the gap since the backup. Neither alone is enough for a
> long-down node; together they work.

---

## 9. Tuning the failure detector and catch-up

These knobs are all optional — the defaults are sensible. Tune them only with a reason. Failure
detection requires the periodic ping to be enabled (it is, for cluster mode, by default).

| Flag | Controls | Default |
|---|---|---|
| `--raft-backfill-threshold` | How far behind a follower can be before catch-up kicks in | `10` |
| `--raft-max-backfill-entries-per-round` | Max log entries shipped per catch-up round | `128` |
| `--raft-learner-promotion-lag` | How close a Learner must get before it's eligible for promotion | `10` |
| `--raft-learner-promotion-stable-window` | Milliseconds it must stay caught up before promotion | `3000` |
| `--raft-gossip-interval` | Milliseconds between roster/health gossip rounds | `5000` |
| `--raft-gossip-fanout` | How many peers each gossip round contacts | `2` |
| `--raft-ping-timeout` | Milliseconds to wait for a direct ping reply | `500` |
| `--raft-indirect-ping-fanout` | How many peers are asked to ping a suspect indirectly | `2` |
| `--raft-suspicion-timeout` | Milliseconds a member stays *suspect* before being declared dead | `5000` |
| `--raft-dead-member-eviction-grace` | Milliseconds of cushion before the dead member's removal is committed | `30000` |

> **Guidance — set the suspicion and eviction-grace timeouts generously.** A "dead" verdict is
> irreversible. On a network with occasional latency spikes or nodes that pause under load, give
> `--raft-suspicion-timeout` and `--raft-dead-member-eviction-grace` enough room to ride out a
> blip. The cost of being slightly slow to evict a truly dead node is far lower than the cost of
> evicting a healthy one mid-pause.

---

## 10. Operational invariants (gotchas worth memorizing)

- **A rolling restart must NOT use `--graceful-leave-on-shutdown`.** Leave is for decommissioning, not
  restarts. Restart a node without that flag and the roster is untouched; the node simply rejoins its
  partitions when it comes back.
- **You cannot remove the last Voter.** The cluster refuses it (§5). Plan capacity so you never need to.
- **Only Voters count for quorum.** A cluster with 3 Voters and 2 Learners still needs 2 (a majority of
  the *Voters*) to make progress — the Learners don't help quorum until promoted.
- **A join into a long-running cluster can hit the compaction floor** (§8). For re-introducing a
  long-down node, expect to seed it from a backup first.
- **Adding/removing a node does not move your data.** Membership controls *who votes*; which partition
  owns which keys is separate and does not shift just because a Voter joined or left.
- **Embedded/standalone mode ignores all of this.** The single-node model and its flags are unchanged;
  none of the cluster flags above apply to it.

---

## 11. The mental model in one paragraph

A Kahuna cluster's membership is a consensus-committed roster, changed one node at a time. A new node
joins a running cluster with `--join-existing` (seeding off `--initial-cluster`), enters as a
non-voting Learner, catches up, and is auto-promoted to Voter — quorum never weakened in the process.
A node leaves either politely (`--graceful-leave-on-shutdown`, for decommissioning only — never for
restarts) or by being detected dead and evicted via SWIM, with suspicion and grace windows tuned to
absorb transient hiccups. The cluster refuses any change that would remove the last Voter. You watch
all of it with `kahuna.control --cluster-members`. The one failure mode to know is the compaction
floor: a node that needs log the cluster has already discarded can't catch up by replay alone — seed
it from a recent backup first, then let it join.
