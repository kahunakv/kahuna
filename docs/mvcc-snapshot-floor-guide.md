# Kahuna MVCC snapshot floor guide

This guide explains how Kahuna lets a client **pin historical versions of data so they stay readable
at a chosen point in time**, even while the data keeps changing — and how it makes those historical
versions actually reachable from every read path. It is written for two audiences:

- **Operators** running Kahuna (directly or as the storage engine behind a database such as CamusDB)
  who need to understand what the feature guarantees, what it costs, and what to watch.
- **Developers** maintaining the code who need the mental model and the invariants that must hold.

No prior knowledge of Kahuna's internals is assumed. Concepts are introduced as they come up.

---

## 1. The big picture

Kahuna keeps a bounded history of each key's past values so that a read can ask for "the value as of
timestamp `T`" (an **as-of read**, expressed by passing a `readTimestamp`). This is what powers, for
example, CamusDB **database branching**: a branch reads its parent's data as of the fork instant and
keeps reading it that way for days or weeks while the parent evolves.

Two things have to be true for that to work reliably:

1. **The version that was current at `T` must survive** — reclamation (the machinery that trims old
   history to bound memory and disk) must not throw it away while someone still cares about it.
2. **Every read shape must be able to find it** — a point read, a range scan, and a bucket/prefix
   scan at `readTimestamp = T` must all return the as-of version, wherever it happens to live.

The **snapshot floor** provides (1): a client registers a **hold** at timestamp `T`, and while that
hold is live Kahuna refuses to reclaim the revision current at `T` (and everything after it) on
*every* key. The **as-of read fallbacks** provide (2): each read path that honors `readTimestamp`
consults on-disk history when the in-memory copy no longer reaches back far enough.

The rest of this guide covers both halves.

---

## 2. Where a key's history lives

Every persistent key has its history spread across three layers, from hottest to coldest:

- **The live value** — the current committed version (`Value` / `Revision` / `LastModified`).
- **A bounded in-memory revision archive** — the newest `RevisionRetention` revisions (default
  **16**), kept per key so recent as-of reads are answered without touching disk.
- **On-disk revision history** — the full run of past revisions in the backend (SQLite or RocksDB),
  bounded only by the persistent retention knobs (by default **kept forever**).

Reclamation actively trims the in-memory archive on every write: once a key has been overwritten more
than `RevisionRetention` times, its older revisions fall out of memory (they remain on disk). So the
in-memory archive is a *cache of recent history*, not the source of truth. This is the key fact
behind everything below: **deep history lives on disk; memory keeps only a small, recent window.**

---

## 3. Holds, leases, and the effective floor

A client protects history by acquiring a **hold**:

- A hold names a **holder id** (who is holding), a **timestamp** `T` (what instant to protect), and a
  **lease** in milliseconds (how long before it lapses if not renewed). Acquiring returns a stable
  **hold id** and the lease expiry.
- Holds are **refcounted**: many independent holds may protect the same or different timestamps.
- Holds are **leased**: a hold must be renewed before its lease expires, so a client that crashes and
  never releases cannot pin history forever. Lease expiry is measured on the **cluster HLC**, never a
  node's wall clock, so a leader change can't mis-expire a hold.

The **effective floor** is the single value that reclamation cares about: the **minimum timestamp
among all currently live holds**, or "no floor" when none are live. Releasing a hold raises the floor
only when the *lowest* hold goes away — protecting `T1 < T2` and releasing the `T2` hold does not free
anything between `T1` and `T2`; releasing the `T1` hold does.

Holds are **replicated cluster state**, not per-node memory. They live on the Raft system partition
(the same mechanism the range map uses), so a follower that becomes leader reconstructs the same floor,
and a node that restarts reloads it before serving reads that depend on it. Because only the
system-partition **leader** can commit a hold, acquire/renew/release are automatically **routed to
that leader** — a client may contact any node and the operation is forwarded to the one that can
commit it.

---

## 4. What the floor protects, and where

While a floor is set, reclamation is constrained at **both** places it would otherwise drop history:

- **In-memory trim.** The archive keeps its normal newest 16 revisions **plus one more**: the single
  newest revision at or before the floor — the **floor-boundary revision**. That boundary is the exact
  version an as-of read at the floor needs, so keeping it in memory lets those reads hit memory for the
  boundary without disk I/O. The archive stays bounded at 16 + 1 per protected key regardless of how
  long a branch lives or how much the parent churns.
- **Persistent prune.** The background revision sweep never deletes, per key, the floor-boundary
  revision or anything newer than it — no matter how aggressive the persistent retention settings are.

The deliberate division of labor: **the boundary lives in memory; the deep run of revisions between
the boundary and now lives on disk.** Reads reach that deep run through the disk fallbacks in the next
section. When no hold is live the floor is unset and reclamation behaves exactly as it always has.

---

## 5. Reading history back: as-of reads and disk fallback

An as-of read (`readTimestamp = T`) returns, per key, the newest revision whose commit time is at or
before `T`. Each read shape resolves it the same way: try the in-memory archive first; on a miss (the
key was overwritten past the 16-revision window), fall back to on-disk history via a
"revision at-or-before `T`" lookup. All three read-timestamp-honoring shapes do this:

- **Point read** (`TryGet`) and **point exists** (`TryExists`).
- **Range / index scan** (`GetByRange`).
- **Bucket / prefix scan** (`GetByBucket`).

Two implementation rules keep this safe and fast, and developers changing these paths must preserve
them:

- **Disk lookups run off the actor thread.** Each partition is a single-threaded actor; blocking it on
  per-key disk I/O would stall every other request for that partition. So the as-of disk resolution
  happens in the off-actor read stage, and the on-actor stage only consumes the already-resolved
  result. Never move a `GetKeyValueRevisionAtOrBefore` call onto the actor thread.
- **Pagination is driven by the raw page, not the projected one.** A range scan fetches a page of
  current keys and *then* projects each to its as-of version, dropping keys that didn't exist yet at
  `T`. The "is there another page?" decision and the next cursor must come from the **unprojected**
  page — otherwise a page made entirely of too-new keys projects to empty and the scan wrongly stops
  before later, visible keys.

A read that finds no version at or before `T` correctly returns "does not exist" / omits the key —
that is the right answer when the key didn't exist at `T` or its history below the floor was already
reclaimed.

---

## 6. The public API

Four operations, exposed over gRPC and REST and on the in-process client:

| Operation | Purpose | Returns |
|-----------|---------|---------|
| `AcquireSnapshotHold(holderId, timestamp, leaseMs)` | Acquire or renew a hold protecting revisions at/after `timestamp`. Idempotent by `(holderId, timestamp)` — a repeat returns the same hold id and renews the lease. | `(type, holdId, leaseExpiry)` |
| `RenewSnapshotHold(holdId, leaseMs)` | Extend an existing hold's lease. Fails if it already expired or was released. | `(type, leaseExpiry)` |
| `ReleaseSnapshotHold(holdId)` | Release a hold; the floor rises when the lowest hold is released. | `type` |
| `GetSnapshotFloor()` | Introspection: current effective floor and live hold count. Floor is "zero" when no hold is live. | `(effectiveFloor, liveHolds)` |

`leaseMs` must be **greater than zero** — a zero or negative lease is rejected as invalid input rather
than accepted as an already-expired hold.

**Consumer responsibility.** Kahuna owns only the floor primitive. The client owns the hold lifecycle:
acquire when the long-lived view begins (e.g. a branch is created), **renew on a timer well inside the
lease** while it lives, and release when it ends. If you stop renewing, the hold lapses and its
protection is gone.

---

## 7. Recovery: crashes, restarts, and leader changes

- **A crashed holder is cleaned up automatically.** A background reaper periodically purges holds whose
  lease has expired, so a client that dies without releasing stops pinning history after at most one
  lease interval — no operator action required.
- **Holds survive restart and failover.** Because holds are replicated Raft state (and also written to
  a local snapshot file), a restarting node reloads them before serving dependent reads, and a new
  leader after a failover reports the same floor and live-hold set.

---

## 8. Observability

The subsystem publishes three instruments under the `Kahuna` meter scope:

| Metric | Kind | Meaning |
|--------|------|---------|
| `kahuna.snapshot_floor.live_holds` | gauge | Number of currently live (non-expired) holds. |
| `kahuna.snapshot_floor.effective_floor_ms` | gauge | Physical (millisecond) component of the effective floor, or 0 when no hold is live. |
| `kahuna.snapshot_floor.missing_protected_version_total` | counter | **Must stay 0.** Increments if reclamation ever schedules a floor-protected version for deletion. |

The counter is the **fault signal**. It is wired at both reclamation sites: the in-memory trim (if the
computed removal set would ever include the floor-boundary revision) and the persistent prune (if a
backend reports it deleted a revision at or above the floor boundary — the backends audit their own
deletions independently of their clamp to detect exactly this). In correct operation nothing protected
is ever reclaimed, so the counter is 0. **A non-zero value means floor enforcement has a gap and a
protected version may have been lost — alert on it.** `live_holds` and `effective_floor_ms` are for
capacity and correctness dashboards (how many branches are pinning history, and how far back).

---

## 9. Limits and things to know

- **Memory-only keys past the retention window.** A key that lives only in memory (never flushed to
  disk) and is overwritten more than `RevisionRetention` times has no disk history to fall back to, so
  an as-of read below the window omits it. Persistent-durability data does not hit this (its deep
  history is on disk), and held timestamps keep the boundary revision in memory regardless.
- **A very narrow prune/acquire window.** The background prune samples the effective floor immediately
  before it deletes. A hold acquired in the microsecond-to-low-millisecond gap between that sample and
  the delete completing may not be seen by that one prune batch. In practice this is not reachable for
  the intended usage (holds are taken at recent timestamps, well above the prune horizon), and the
  window was reduced from a much larger one; closing it entirely would require serializing acquire
  against the prune delete, which is deliberately deferred.
- **Hold-registry replication cost.** Each acquire/renew/release replicates the hold registry through
  the system-partition Raft log. Floor *checks* are O(1) (a cached floor is refreshed on each
  mutation), but the replicated *payload* grows with the number of live holds. This is comfortable for
  the expected scale (a bounded number of long-lived branches); if hold counts were to grow very large,
  frequent lease renewals would produce sizable log entries. Prefer coarse-enough lease TTLs that
  renewals are not hot.

---

## 10. Tuning summary

The floor itself has no dedicated global knobs — protection is driven entirely by live holds and their
timestamps. The surrounding retention knobs decide how much history is kept and therefore how much a
hold has to protect:

| Knob | Default | Effect on as-of reads / holds |
|------|---------|-------------------------------|
| `RevisionRetention` | 16 | How many recent revisions per key answer as-of reads from memory before falling back to disk. A live hold additionally keeps one boundary revision. |
| `PersistentRevisionRetentionCount` | 0 (keep all) | How many revisions the persistent sweep keeps per key. The floor clamps this: protected revisions are kept regardless. |
| `PersistentRevisionRetentionAge` | disabled | Age-based persistent pruning; also clamped by the floor. |
| `leaseMs` (per acquire/renew) | — caller-chosen | How long a hold survives without renewal. Renew well inside it; choose it coarse enough that renewals aren't a hot path. |

General guidance:

- To make durable branch reads safe, the client **must** hold a floor at the branch's timestamp and
  keep renewing it — retention knobs alone are best-effort and will eventually reclaim unheld history.
- Aggressive persistent retention is safe to combine with holds: the floor overrides it for exactly the
  versions a hold needs, and nothing more.
- Watch `missing_protected_version_total` — it should be flat at 0.

---

## 11. Mental model in one paragraph

A client pins a point in time by taking a leased, refcounted **hold** at timestamp `T`; the **effective
floor** is the lowest live hold, replicated on the system partition so it survives restart and
failover, and acquire/renew/release are routed to the partition leader. While the floor is set,
reclamation keeps — in memory — the newest 16 revisions plus the one boundary revision at or before the
floor, and — on disk — everything at or after that boundary, refusing to prune it however aggressive
retention is. Every as-of read (point, range, bucket) answers from the in-memory archive when it can
and falls back to on-disk history off the actor thread when it can't, so the protected version is
always reachable. A crashed holder's lease lapses and a reaper reclaims it; a fault-signal counter stays
at 0 unless enforcement ever slips. The result is a store where a long-lived reader can keep seeing data
exactly as it was at `T`, for as long as it keeps its hold alive, without freezing the rest of the store.
