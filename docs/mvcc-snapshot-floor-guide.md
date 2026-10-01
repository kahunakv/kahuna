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
   history to control memory and disk use) must not throw it away while someone still cares about it.
2. **Every read shape must be able to find it** — a point read, a range scan, and a bucket/prefix
   scan at `readTimestamp = T` must all return the as-of version, wherever it happens to live.

The **snapshot floor** provides (1): a client registers a **hold** at timestamp `T`, and while that
hold is live Kahuna refuses to reclaim the revision current at `T` (and everything after it) on
*every* key. The **as-of read fallbacks** provide (2): each read path that honors `readTimestamp`
consults on-disk history when the in-memory copy no longer reaches back far enough.

The rest of this guide covers both halves.

---

## 2. Where a key's history lives

A persistent key that retains revisions has its history spread across three layers, from hottest to coldest:

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
  never releases is eventually removed by the reaper. Lease expiry is measured on the **cluster HLC**, never a
  node's wall clock, so a leader change can't mis-expire a hold.

The reported **effective floor** is the **minimum timestamp among all currently live holds**,
or "no floor" when none are live. It is distinct from the protective floor:
reclamation uses the minimum timestamp of **all registered holds**, including expired holds, until
a replicated release or reaper removal commits. Releasing a hold raises the floor
only when the *lowest registered* hold goes away — protecting `T1 < T2` and releasing the `T2` hold does not free
anything between `T1` and `T2`; releasing the `T1` hold does.

Holds are **replicated cluster state**, not per-node memory. They live on the Raft system partition
(the same mechanism the range map uses), so a follower that becomes leader reconstructs the same floor,
and a node that restarts reloads it before serving reads that depend on it. Because only the
system-partition **leader** can commit a hold, acquire/renew/release are automatically **routed to
that leader** — a client may contact any node and the operation is forwarded to the one that can
commit it.

---

## 4. What the floor protects, and where

While a protective floor is set, reclamation is constrained at **both** places it would otherwise drop history:

- **In-memory trim.** The archive keeps its normal newest 16 revisions **plus one more**: the single
  newest revision at or before the floor — the **floor-boundary revision**. That boundary is the exact
  version an as-of read at the floor needs, so keeping it in memory lets those reads hit memory for the
  boundary without disk I/O. In addition, revisions not yet confirmed flushed are retained even
  beyond the count limit.
  The archive can therefore exceed 16 + 1 while persistence lags; its size depends on the key's
  write rate and flush progress.
- **Persistent prune.** The background revision sweep never deletes, per key, the floor-boundary
  revision or anything newer than it — no matter how aggressive the persistent retention settings are.

The deliberate division of labor: **the boundary lives in memory; the deep run of revisions between
the boundary and now lives on disk.** Reads reach that deep run through the disk fallbacks in the next
section. When the registry is empty the protective floor is unset. An expired but still registered hold
continues protecting history until its removal commits.

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

- **Disk lookups run off the actor thread.** A key-value actor shard is single-threaded; blocking it on
  per-key disk I/O would stall its other keys, potentially across multiple Raft partitions. As-of resolution
  happens in the off-actor read stage, and the on-actor stage only consumes the already-resolved
  result. Never move a `GetKeyValueRevisionAtOrBefore` call onto the actor thread.
- **Pagination is driven by the raw page, not the projected one.** A range scan fetches a page of
  current keys and *then* projects each to its as-of version, dropping keys that didn't exist yet at
  `T`. The "is there another page?" decision and the next cursor must come from the **unprojected**
  page — otherwise a page made entirely of too-new keys projects to empty and the scan wrongly stops
  before later, visible keys.

### Read fences and repeatability

A snapshot read waits when another live writer may commit at or before `T`, including when a
committed intent supplies the candidate value. The serving actor folds `T` into its HLC before
answering; durable commit timestamps are minted above every participant's staged timestamp.
If `T` is more than **5 seconds ahead** of the serving node's HLC, the read is served without this
clock fence and `kahuna.kv.snapshot_clock_fence_skipped_total` increments. Such a future timestamp
can include writes that start after the first read. Use cluster-minted timestamps.

Persisted history may lag committed state. The archive retains unflushed revisions, and a disk
fallback that could omit a queued revision answers `MustRetry` until flush progress makes it safe.
A retained floor-boundary revision is not treated as an authoritative answer for timestamps in a
trimmed gap above it; those reads consult disk. These safeguards do not create history for
`SetNoRevision` writes, which deliberately suppress revision storage. TTL filtering also uses the
current read time, so an as-of timestamp does not freeze expiration.

See [transaction reads and locks](transaction-read-and-lock-semantics-guide.md) for per-key latest-read
pins and their distinction from fixed-timestamp reads. The read-fence counters are
`kahuna.kv.revisions.retained_unflushed_total` and `kahuna.kv.revisions.history_reads_fenced_total`.

A read that finds no version at or before `T` correctly returns "does not exist" / omits the key —
that is the right answer when the key didn't exist at `T` or its history below the floor was already
reclaimed.

---

## 6. The public API

Four operations, exposed over gRPC and REST and on the in-process client:

| Operation | Purpose | Returns |
|-----------|---------|---------|
| `AcquireSnapshotHold(holderId, timestamp, leaseMs)` | Acquire or renew a hold protecting revisions at/after `timestamp`. Idempotent by `(holderId, timestamp)` — a repeat returns the same hold id and renews the lease. | `(type, holdId, leaseExpiry)` |
| `RenewSnapshotHold(holdId, leaseMs)` | Extend an existing hold's lease. Revives an expired hold if it is still registered; returns `DoesNotExist` after release or purge. | `(type, leaseExpiry)` |
| `ReleaseSnapshotHold(holdId)` | Release a hold; the floor rises when the lowest hold is released. | `type` |
| `GetSnapshotFloor()` | Introspection: current effective floor and live hold count. Floor is "zero" when no hold is live. | `(type, effectiveFloor, liveHolds)` |

`leaseMs` must be **greater than zero** — a zero or negative lease is rejected as invalid input rather
than accepted as an already-expired hold.

**Consumer responsibility.** Kahuna owns only the floor primitive. The client owns the hold lifecycle:
acquire when the long-lived view begins (e.g. a branch is created), **renew on a timer well inside the
lease** while it lives, and release when it ends. If you stop renewing, the hold becomes purge-eligible.
Protection ends when that removal commits;
do not rely on the delay between expiry and purge.

---

## 7. Recovery: crashes, restarts, and leader changes

- **A crashed holder is cleaned up automatically.** A background reaper periodically purges holds whose
  lease has expired. A client that dies without releasing stops pinning history when the replicated
  purge commits. Reaper cadence and quorum availability
  can delay removal beyond the lease interval.
- **Holds survive restart and failover.** Because holds are replicated Raft state (and also written to
  a local snapshot file), a restarting node reloads them before serving dependent reads. Floor
  introspection confirms leadership/application catch-up before answering. Loaded holds
  are exempt from expiry purge during `SnapshotHoldStartupGraceWindow` (default 5 minutes),
  allowing renewal after downtime longer than a lease. Renewal of a lapsed hold confirms the
  meta-partition application before proving that the hold is still registered.

---

## 8. Observability

The subsystem publishes these instruments under the `Kahuna` meter scope:

| Metric | Kind | Meaning |
|--------|------|---------|
| `kahuna.snapshot_floor.live_holds` | gauge | Number of currently live (non-expired) holds. |
| `kahuna.snapshot_floor.effective_floor_ms` | gauge | Physical (millisecond) component of the effective floor, or 0 when no hold is live. |
| `kahuna.snapshot_floor.prune_skipped_unconfirmed_total` | counter | Prune cycles skipped because local meta-partition catch-up could not be confirmed; work is retried later. |
| `kahuna.snapshot_floor.missing_protected_version_total` | counter | **Must stay 0.** Increments if reclamation ever schedules a floor-protected version for deletion. |

`missing_protected_version_total` is the **fault signal**. It is wired at both reclamation sites: the in-memory trim (if the
computed removal set would ever include the floor-boundary revision) and the persistent prune (if a
backend reports it deleted a revision at or above the floor boundary — the backends audit their own
deletions independently of their clamp to detect exactly this). In correct operation nothing protected
is ever reclaimed, so the counter is 0. **A non-zero value means floor enforcement has a gap and a
protected version may have been lost — alert on it.** `live_holds` and `effective_floor_ms` are for
capacity and correctness dashboards (how many branches are pinning history, and how far back).

The persistent prune that the floor clamps has its own instruments, because it shares the single
background writer with the flush and must never starve it:

| Metric | Kind | Meaning |
|--------|------|---------|
| `kahuna.persistence.revision_prune.keys_walked_total` | counter | Keys whose revision block the prune (targeted or sweep) scanned. |
| `kahuna.persistence.revision_prune.keys_skipped_total` | counter | Keys answered from the RocksDB prune memo without a scan (nothing deletable yet). On a hot key-set inside its retention window this should dominate `keys_walked_total`. |
| `kahuna.persistence.revision_prune.keys_floor_blocked_total` | counter | Of the skipped keys, those whose deletable rows the snapshot floor (a hold registry entry) protects. Skips with this at zero are retention waiting on the clock or the row count — the configured policy, not a hold, is what keeps history on disk. |
| `kahuna.persistence.revision_prune.revisions_deleted_total` | counter | Revision rows the prune (targeted or sweep) deleted. |
| `kahuna.persistence.revision_prune.budget_exhausted_total` | counter | Flush cycles whose targeted prune stopped on `PersistentRevisionCleanupTimeBudget` with keys still queued. A sustained rate means retention lags the write rate; the flush is unaffected. |
| `kahuna.persistence.revision_prune.cycle_duration` | histogram (ms) | Time the targeted prune took per flush cycle; bounded by the time budget plus one key (the backend checks the budget before every key). |
| `kahuna.persistence.revision_prune.sweep_budget_exhausted_total` | counter | Backend-wide sweep passes that paused on their share of the time budget; the sweep resumes from its cursor next cycle. A large store pauses often and still completes; a sweep that pauses forever without wrapping is the signal to look at. |
| `kahuna.persistence.revision_prune.sweep_pass_duration` | histogram (ms) | Time one sweep pass spent on the writer; bounded by its budget plus one key. |

Read the counters against the policy. With `PersistentRevisionRetentionCount = 0` and
`PersistentRevisionRetentionAge = 1h` (a common long-retention configuration) nothing is deletable for the
first hour, so a 45-minute run shows millions of memo skips, zero deletions, zero floor-blocked keys,
and a store growing at the write rate — the policy at work, not a gate that never opens. The
background writer logs the effective policy once at startup ("Persistent revision retention on this
node …") so the numbers can be read against it; both bounds disabled means history on disk is
unbounded, and that line says so.

The RocksDB sweep visits keys through their `~CURRENT` rows and jumps over the revision rows in
between with the same registry-gated seek the range scans use, so a pass costs O(logical keys) plus
the walks that actually prune — not O(history rows) — and it checks its budget every 1,024 stepped
rows as well as before every key.

---

## 9. Limits and things to know

- **Memory-only keys past the retention window.** A key that lives only in memory (never flushed to
  disk) and is overwritten more than `RevisionRetention` times has no disk history to fall back to, so
  an as-of read below the window omits it. Persistent-durability data does not hit this (its deep
  history is on disk), and held timestamps keep the boundary revision in memory regardless.
- **Acquisition overlapping pruning.** Targeted cleanup and full sweeps sample the protective floor
  under `BeginPrune` and close the delete window with `EndPrune`. A local acquisition whose
  replication/commit overlaps that window returns `MustRetry`, even if its hold was committed.
  Retry with the same `(holderId, timestamp)`; acquisition cannot restore history already deleted.
  Before destructive pruning, each node confirms meta-partition application catch-up. An unconfirmed
  node skips pruning and retains the work for a later cycle. This is not a cluster-wide atomic barrier:
  a remote hold can still commit between that confirmation and the local floor sample.
- **Hold-registry replication cost.** Routine acquire/renew/release mutations replicate keyed
  upsert/remove **deltas** through the system-partition Raft log, rather than the entire registry.
  Floor checks use cached minima; registry copies and local snapshot writes still grow with the
  registered hold count. P0 state transfer carries the complete registry. Prefer lease TTLs that
  keep renewal traffic manageable.

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
| `PersistentRevisionCleanupBatchSize` | 10000 | Revision rows one prune pass (a targeted cycle or a sweep pass) may delete. The time budget bounds the pass; this only caps its tombstones. Keep it well above what one hot key sheds per visit: a walk cut short by this limit has already paid for the whole block and must walk it again next cycle. |
| `PersistentRevisionCleanupTimeBudget` | 250 ms | Wall-clock the revision prune may spend per flush cycle, shared by the targeted prune and the sweep that follows it (the sweep always gets at least a quarter). The RocksDB backend checks it before every key and every 1,024 sweep rows; keys and keyspace it does not reach are resumed next cycle. Keep it well under `DirtyObjectsWriterDelay` (the flush budget): the prune runs on the same writer, so its time comes straight out of flush throughput. |
| `leaseMs` (per acquire/renew) | — caller-chosen | How long a hold survives without renewal. Renew well inside it; choose it coarse enough that renewals aren't a hot path. |

General guidance:

- To make durable branch reads safe, the client **must** hold a floor at the branch's timestamp and
  keep renewing it — retention knobs alone are best-effort and will eventually reclaim unheld history.
- Aggressive persistent retention is safe to combine with holds: the floor overrides it for exactly the
  versions a hold needs, and nothing more.
- Enabling retention on a hot key-set is cheap in steady state: the RocksDB backend memoizes, per key,
  how many history rows it has and how old its oldest non-current row is, and skips the revision walk
  until count or age could actually delete something (the store path advances the memo as it writes;
  a moved floor, a deleted key or a recovery reopen re-arm the walk). Without the memo every flush
  cycle re-walked every hot key's whole revision block, and on a 2,000-row bank workload that walk
  alone outgrew the one-second flush budget within two minutes.
- Watch `missing_protected_version_total` — it should be flat at 0.

---

## 11. Mental model in one paragraph

A client pins a point in time by taking a leased, refcounted **hold** at timestamp `T`; the **effective
floor** is the lowest live hold, replicated on the system partition so it survives restart and
failover, and acquire/renew/release are routed to the partition leader. While the floor is set,
reclamation keeps — in memory — the newest 16 revisions, unflushed revisions, and the boundary revision at or before the
protective floor, and — on disk — everything at or after that boundary, refusing to prune it however aggressive
retention is. Every as-of read (point, range, bucket) answers from the in-memory archive when it can
and falls back to on-disk history off the actor thread when it can't, so the protected version is
reachable subject to retained history and the read fences in §5. A crashed holder's lease lapses and a replicated
reaper removal ends protection; a fault-signal counter stays
at 0 unless enforcement ever slips. A continuously registered hold preserves history for a long-lived reader without stopping writes;
TTL filtering, clock bounds, history suppression and the cross-node acquisition window remain
limitations of that view.
