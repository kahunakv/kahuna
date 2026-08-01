# Kahuna backups and point-in-time recovery guide

This guide explains how Kahuna backs up data, how it restores to an exact moment in the past, and why
those operations are safe. It is written for two audiences:

- **Users** running Kahuna (directly or as the storage engine behind a database such as CamusDB) who
  want to understand what can be backed up, how far back they can recover, and what it costs.
- **Developers** maintaining the code who need the mental model and the invariants that must hold.

No prior knowledge of Kahuna's internals is assumed. Concepts are introduced as they come up, and the
key design decisions are called out with the reasoning behind them.

---

## 1. The big picture

Three features sound different but are really one machine:

- A **full backup** is a complete, restorable copy of the data as of some moment.
- An **incremental backup** is just the *changes* since a previous backup — small and cheap.
- **Point-in-time recovery (PITR)** rebuilds the data as it was at an *exact* chosen moment, not just
  at backup boundaries.

They are one machine because they all combine the same two ingredients:

1. **A base image** — a snapshot of the storage engine at a known point.
2. **An ordered, timestamped log of every change** — Kahuna already keeps this for consensus; it is
   the **Write-Ahead Log (WAL)**.

Given those two, everything falls out: a snapshot is a base image at time `T`; an incremental backup
is the slice of the log since the last backup; and PITR is "load a base image, then replay the log
forward and **stop at exactly `T`**."

> **Decision — reuse the log Kahuna already has, instead of inventing a backup format.** Kahuna writes
> a WAL for consensus regardless of backups. That log already contains every change, in exact order,
> each stamped with a time. Building backups on top of it means PITR is almost free conceptually: no
> separate change-capture machinery, no risk of the backup format disagreeing with the real data. The
> log *is* the change history. This single decision shapes everything below.

---

## 2. The moving parts (Kahuna in 60 seconds)

To follow the rest of this guide, you need a rough map of how a write travels through Kahuna.

```
                    ┌──────────────────────── one partition ────────────────────────┐
   client write     │                                                                │
   ───────────────► │  ┌────────┐   1. append    ┌──────────────┐                    │
                    │  │ actor  │ ─────────────►  │   WAL (log)  │  replicated to     │
                    │  │(leader)│   2. commit     │  ordered +   │  other replicas    │
                    │  └────┬───┘ ◄─────────────  │  timestamped │  for consensus     │
                    │       │      3. apply       └──────┬───────┘                    │
                    │       ▼                            │                            │
                    │  ┌──────────┐                      │ 4. flush (background,      │
                    │  │ in-memory│                      ▼    slightly later)         │
                    │  │  state   │ ───────────►  ┌──────────────┐                    │
                    │  └──────────┘               │   storage    │  RocksDB / SQLite  │
                    │                             │   backend    │  (durable on disk) │
                    │                             └──────────────┘                    │
                    └────────────────────────────────────────────────────────────────┘
```

What each piece is:

> **Concept — partition (shard).** Kahuna splits all keys into many independent **partitions**. Each
> is its own little replicated database with its own log. This is how Kahuna scales horizontally —
> and it is why "back up the cluster" means "coordinate across many partitions" (see §8).

> **Concept — actor.** Each partition is owned by a single-threaded worker called an **actor**. It
> handles that partition's requests one at a time. Single-threaded means no locks for its own data,
> but also that any slow operation stalls the partition — so backup work is designed to *read* the
> log in the background rather than block the actor.

> **Concept — consensus and the leader.** For durability, a change isn't "real" until a majority of a
> partition's replicas agree on it. One replica is the **leader**; it appends the change to the WAL
> and replicates it. Once a majority has it, the change is **committed** — permanent and ordered. A
> backup only ever cares about *committed* entries.

> **Concept — the background writer.** Applying a change updates fast in-memory state immediately, but
> writing it down into the durable storage engine (RocksDB/SQLite) happens a little later, in
> batches, via a background writer. **This lag — committed in the log but not yet flushed to disk —
> is small but real, and it is the reason full backups need the careful ordering described in §5.**

So the two artifacts a backup draws from are the **WAL** (the ordered change history) and the
**storage backend** (the on-disk state). Hold that picture; the rest of the guide is just different
ways of combining them.

> **Concept — Hybrid Logical Clock (HLC).** Every committed change carries an HLC timestamp — a clock
> that blends real wall-clock time with a logical counter so that, across the whole cluster, any two
> changes can be put in a consistent order. "Recover to time `T`" means "recover to a specific HLC."

> **Decision — make time the recovery axis, not log position.** A log position (index) is local to one
> partition and meaningless across the cluster. An HLC is comparable everywhere. By keying recovery on
> HLC, "restore everything to `T`" is one well-defined cut across all partitions at once — the
> foundation for both PITR and coordinated cluster snapshots.

---

## 3. The retention window: how far back you can go

PITR is not unlimited. Keeping every change forever would grow without bound, so Kahuna keeps a
**sliding window** of recent history — by default **1 hour**, configurable up to **6 hours**.

The window slides: at any moment you can recover to any point between "now" and "now minus the
window." Anything older than the window is allowed to be cleaned up, which keeps storage bounded to
roughly *window length × write rate* — a number you can plan capacity around, not an open-ended
liability.

```
   past  ◄─────────────────────── time ───────────────────────►  now
                 │◄──────────── PitrWindow (1–6h) ─────────────►│
        ─────────┼──────────────────────────────────────────────┼──
        trimmed  │  recoverable: pick any T in this range         ▲
       (gone)    ▲                                               you can
              retention                                          restore
                floor                                            to "now"
```

Two settings control this:

- **`PitrWindow`** (default 1 hour, max 6 hours) — how far back you can recover.
- **`BaseSnapshotInterval`** (default 30 minutes) — the cadence at which the WAL-retention floor is
  recomputed, and a safety margin added below the window when computing that floor (the protected
  boundary is `now − PitrWindow − BaseSnapshotInterval`). It does **not** schedule backups — Kahuna
  takes no base image on its own; full and incremental backups are operator-triggered (§5–§6). Keep it
  no larger than the window so the retained log always reaches back to the oldest recoverable point.

> **Concept — compaction and the retention floor.** Normally the WAL is *compacted* (old entries
> trimmed) once their effects are safely on disk — otherwise the log grows forever. But PITR needs the
> log kept *longer*, back to the edge of the window. Kahuna continuously computes a **floor**: the
> oldest log position that must survive, at roughly `now − PitrWindow − BaseSnapshotInterval`. The
> consensus layer is told never to compact below that floor.

> **Decision — bound recovery by time, and let one floor reconcile two opposite needs.** Kahuna is
> tuned to *forget* aggressively (small memory, small log). PITR needs it to *remember*. Rather than
> bolt on a second retention system, a single sliding floor reconciles them: everything below the
> floor is forgotten as before; everything above it is kept for recovery. Choosing a *bounded* window
> (not "forever") is the deliberate trade that keeps the cost a predictable, plannable number.

A practical consequence: the floor lives in memory and is re-established shortly after a node
restarts. There is a brief window right after a restart where, if cleanup runs before the floor is
re-asserted, the recoverable history on that node can be slightly shorter. In normal operation the
floor is refreshed far more often than cleanup runs, so this is a minor edge, not a routine concern.

---

## 4. Two ways to build a backup: base image vs. delta

Before the mechanics, the shape of the whole system:

```
   full backup  =  [ base image (whole storage engine) ]  +  manifest
   incremental  =  [ log slice: changes since parent ]     +  manifest  ─── points to parent
```

> **Decision — base image + deltas, not "a full copy every time."** A full copy of a large dataset is
> expensive and slow. Instead an operator takes an occasional **base image** (a full backup) and then
> captures cheap **deltas** (incrementals — log slices) between them. Restoring means "load the nearest
> base, then replay the few deltas on top." This is the classic trade that makes frequent backups
> affordable and restores fast — provided the operator (or their scheduler) takes a fresh full often
> enough that a base image is never too far behind. `BaseSnapshotInterval` does not do this for you; it
> only bounds how long the WAL is retained so an incremental can still be cut against the last base.

The next two sections are just these two shapes in detail.

---

## 5. Full backups

A full backup has two parts written together:

1. **A base image** — a consistent, crash-safe snapshot of the storage engine. RocksDB produces this
   almost instantly (it hard-links its files); SQLite copies the database; the in-memory engine
   serializes its state. The snapshot is taken into a temporary location and atomically moved into
   place, so an interrupted backup never leaves a half-written image that looks valid.
2. **A manifest** — a small JSON record describing the backup: its identity, the per-partition range
   of log positions it covers, checksums, and (for coordinated backups, see §8) the cluster
   timestamp.

> **Concept — checkpoint.** A "checkpoint" is a point-in-time snapshot of the storage engine that can
> be opened independently of the live database. It is the base image a restore starts from.

### The flow, and why the order is non-negotiable

```
   1. read M  = last committed log position (per partition)   ◄── do this FIRST
   2. barrier = wait until every captured partition has APPLIED and enqueued its
                committed writes up to M's timestamp                ── else fail closed
   3. flush   = drain pending writes to the storage backend   ── now disk holds everything ≤ M
   4. snapshot the storage backend  →  base image
   5. write manifest (covers up to M) + checksums  →  catalog
```

Recall from §2 that a change can be *committed in the log* but *not yet flushed to disk*. The base
image is a snapshot of the disk. So the order matters:

- **Read `M` (the last committed position) first.**
- **Wait for the apply barrier.** Committing an entry in the log and *applying* it to in-memory
  state (which is what the flush later drains) are two steps, and the second lags the first. The
  backup waits until every captured partition has applied — and enqueued for persistence — its
  committed writes up to `M` before it flushes.
- **Then flush**, which pushes everything applied-so-far onto disk — a superset of `M`.
- **Then snapshot.** The snapshot is now guaranteed to contain everything up to `M`.

> **Decision — capture the position before flushing, never after.** If you snapshotted first and then
> claimed it covered the latest committed position, a change committing in the gap would be *named in
> the manifest but absent from the image*. And because a full backup carries **no log slice of its
> own**, there would be nothing to replay it back from — it would be silently lost on restore. Reading
> `M` before flushing closes that gap: the image always contains at least what the manifest promises.
> (If it contains a little *more* than `M`, that's harmless — replay is idempotent, see §7.)

> **Decision — an applied-index barrier before the flush, keyed on HLC, that fails closed.** A flush
> only drains what has been *applied* to in-memory state. If a write is committed but not yet applied
> when the flush runs, the flush can't drain it, the snapshot omits it, and the manifest still names
> it — the same silent-loss bug, one step earlier in the pipeline. So before flushing, the backup
> waits until each captured partition's applied-and-enqueued progress reaches that partition's
> covered timestamp. If a partition doesn't catch up within the barrier timeout
> (`applyBarrierTimeoutMs`, default 30 s), the backup **fails closed** with
> `ExactCheckpointUnavailable` — no image, no catalog entry — rather than publish a checkpoint that
> is missing committed writes. The barrier is keyed on **HLC**, not log index, on purpose: the
> leader's commit-completion path carries only the change's HLC, not a WAL index, so an HLC barrier
> keeps this a self-contained Kahuna guarantee with no change to the consensus layer.

> **Decision — a flush that cannot durably persist must fault, not report success.** The background
> writer's drain now reports whether it actually persisted everything. If it can't (a backend I/O
> failure that survives its retries), it raises an error instead of returning quietly, and the backup
> aborts with nothing published. Combined with the apply barrier, a full backup now has one honest
> outcome contract: **either the base image provably contains every committed write up to `M`, or the
> backup fails and writes no artifact and no catalog entry** — it never publishes a checkpoint it
> can't stand behind.

---

## 6. Incremental backups and the chain

An incremental backup doesn't re-snapshot anything. It records the **slice of the WAL** committed
since its parent backup — per partition, from "one past where the parent ended" up to now — as a set
of segment files, plus a manifest that links back to its parent.

```
   full ───────► incremental ───────► incremental ───────► incremental
   (base image)   (log 11..40)         (log 41..78)         (log 79..120)
        ▲              │                     │                     │
        └──────────────┴──── each "ToIndex" + 1 == next "FromIndex" (no gaps) ──┘
```

Backups therefore form a **chain**: a full backup at the root, then incrementals each pointing at the
previous one. Before any restore, Kahuna walks the chain and validates it: it must start with a full
backup, every later link must be an incremental, parent links must be unbroken, and the log ranges
must be contiguous with no gaps. A broken or gapped chain is rejected with a clear error rather than
silently restoring partial data. (Cycles in the links are detected too, so a corrupt catalog can't
send the validator into a loop.)

> **Concept — the catalog.** The catalog is the index of all backups in a storage location. It stores
> and retrieves manifests and resolves a chain from any backup back to its root.

> **Decision — validate the whole chain before trusting any of it.** A restore is only as good as the
> weakest link in its chain. Rather than discover a gap halfway through replaying, Kahuna proves the
> chain is complete and contiguous *first*. The principle throughout PITR: **refuse loudly rather than
> reconstruct a state that never existed.**

If an incremental's starting point has already fallen below the retention floor (its parent is too
old to still be in the WAL), Kahuna refuses it and tells you a new full backup is required — rather
than producing an incremental with a hole in it.

---

## 7. Point-in-time recovery

To restore to an exact time `T`:

```
   1. validate T is inside the window (reject if older than now−PitrWindow, or in the future)
   2. load the base image (root full backup)        →  state as of the base
   3. replay log slices in order, applying while Time ≤ T,
      STOP at the first change with Time > T          →  state as of exactly T
```

> **Concept — the stop-predicate.** Replay walks the log slices in order and applies each change only
> while its timestamp is at or before `T`. The instant it sees a change past `T`, it stops. Because
> the log is ordered by time, everything after that is also past `T`, so the cut is clean.

Two properties make this safe to run, and safe to *re-run*:

- **Idempotent.** Applying a change is an upsert keyed by key and revision, so replaying the same
  slice twice produces the same result. If a restore is interrupted, re-running it from the start is
  harmless.
- **Never torn by an in-flight transaction.** Backups capture only *committed* changes. A transaction
  that had started but not committed by `T` simply isn't in the data — the affected keys keep their
  last committed value. There is no half-finished transaction to clean up.

> **Concept — write intent.** While a transaction is preparing, it holds a *write intent* on the keys
> it will change — a marker saying "a commit may be coming." Intents are not committed data, so they
> are never in a backup. This is why an uncommitted transaction is automatically absent from a
> restore.

> **Decision — make replay idempotent so restore is restartable.** Restores can be interrupted (a
> crash, a cancelled job). If applying a change twice could corrupt state, every interruption would
> need careful cleanup. By making each apply an upsert keyed by `(key, revision)`, "just run it again
> from the start" is always correct. Idempotency is what turns restore from a delicate operation into
> a routine one.

---

## 8. Coordinated snapshots across the whole cluster

Kahuna spreads data across many partitions, each with its own log. A backup of a single partition is
straightforward. A backup of the *whole cluster* needs every partition to stop at the **same logical
moment**, so the result is a consistent cut — not partition 1 as of 12:00:05 and partition 2 as of
12:00:09.

```
                       coordinator picks one safe T
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                    ▼
        partition 1         partition 2          partition 3
        cap at Time ≤ T     cap at Time ≤ T      cap at Time ≤ T
              │                   │                    │
              └────────── one consistent cut at T ─────┘
```

Because every change carries a cluster-wide HLC, the coordinator can pick one timestamp `T` and tell
every partition "cap your coverage at `T`." Each partition independently includes everything with
`Time ≤ T`.

### Choosing a safe `T`

There is a subtlety worth understanding. A transaction that spans two partitions commits on each
partition with that partition's *own* local timestamp, so the two halves can land at slightly
different HLCs. If `T` were chosen to fall *between* them, the cut would include one half and exclude
the other — a torn transaction.

```
   cross-shard transaction:   shard A commits at t=240   shard B commits at t=260
                                          │                       │
   unsafe T = 250  ───────────────────────┼─────── T ─────────────┼──  A in, B out  → TORN
   safe   T = 230  ──── T ─────────────────┼───────────────────────┼──  both out     → OK
   safe   T = 270  ────────────────────────┼───────────────────────┼─── T ──  both in → OK
```

To avoid the torn case, the coordinator picks `T` **strictly below the earliest in-flight (preparing)
transaction in the cluster**. Any transaction that is mid-commit will land entirely above `T` and be
excluded as a whole; everything already settled below `T` is included. When the cluster is idle (no
transactions in flight), `T` is simply the latest committed point — which includes everything and so
can't tear anything.

> **What this guarantees, and what it doesn't.** Choosing `T` below all in-flight work prevents
> cutting a transaction *that is actively committing*. It does not, by itself, protect against a
> transaction that committed earlier whose two halves happened to land on opposite sides of `T`. In
> practice the coordinator chooses `T` to avoid the active-commit case; an unconditional guarantee
> would require stamping every participant of a transaction with one shared commit timestamp, which
> is a larger change. For most operational backups — taken at a quiet point or with the coordinator
> picking a safe `T` — the cut is consistent.

> **Decision — accept a "choose a safe T" rule rather than re-architect commits now.** The fully
> general fix (one shared commit timestamp across all participants of a transaction) is a deep change
> to how transactions commit. The pragmatic choice is to keep per-shard commit timestamps and instead
> have the coordinator *pick `T` to dodge the dangerous zone*. It covers the common case cheaply, and
> the limitation is documented honestly rather than hidden behind an over-promise.

---

## 9. Restoring a node vs. adding it to the cluster

A common expectation is "restore a backup onto a fresh machine and it joins the cluster." It is worth
being precise: **restore produces *data*; joining a cluster is a separate step.**

```
   restore  ──►  a node holding the data as of T  ──►  to the cluster it looks like a
                                                        member that is simply "behind"
                                                              │
                                            admit to membership + let consensus catch it up
                                                              ▼
                                                     a working cluster member
```

A restored node holds the data as of `T`, but to a running cluster it looks like a member that is
simply *behind*. Making it a participant still requires admitting it to cluster membership, after
which the normal consensus catch-up brings it current. Restoring from a recent backup is a useful way
to *seed* a node that was down for a while — so the cluster only has to ship the small remainder
instead of the entire dataset — but the seed only helps when the restore point is still inside the
retention window (otherwise the cluster sends a full copy anyway). Whole-cluster disaster recovery —
restoring every node to one coordinated `T` and bringing the cluster back up — is the case where
restore alone reconstitutes a running system.

> **Decision — keep "restore data" and "join cluster" as separate, composable steps.** Conflating them
> would tie the backup format to cluster identity and membership, which change independently of data.
> Keeping them separate means a backup is portable (restore it anywhere), and the seed-then-catch-up
> path is an *optimization* layered on the normal join, not a replacement for it.

---

## 10. Tuning and operations

| Setting | Default | What it controls |
|---|---|---|
| `PitrWindow` (`--pitr-window`, seconds) | 1 hour | How far back you can recover. Larger = more recovery range, more retained WAL. Max 6 hours. |
| `BaseSnapshotInterval` (`--base-snapshot-interval`, seconds) | 30 minutes | WAL-retention-floor tick cadence and the safety margin below the window (`floor = now − PitrWindow − BaseSnapshotInterval`). Does **not** schedule backups. Must be ≤ `PitrWindow`. |
| `BackupRetentionMaxChains` (`--backup-retention-max-chains`) | 0 (off) | Keep at most this many most-recent backup **chains**; older chains are deleted whole. 0 = unbounded. |
| `BackupRetentionMaxAge` (`--backup-retention-max-age`, seconds) | 0 (off) | Delete any chain whose *newest* backup is older than this. 0 = unbounded. |
| `BackupRetentionMaxBytes` (`--backup-retention-max-bytes`) | 0 (off) | Keep the most-recent chains whose combined artifact bytes stay within this budget; the single newest chain is always kept. 0 = unbounded. |
| `BackupGcInterval` (`--backup-gc-interval`, seconds) | 1 hour | Cadence of the background GC pass (orphan sweep + retention), and a startup sweep on its first tick. 0 disables the periodic pass (GC then runs only inline after each backup). |
| `BackupRestoreThrottleBytesPerSec` (`--backup-restore-throttle-mbps`, MB/s) | 0 (unlimited) | Throughput budget for a restore's bulk checkpoint copy, so a restore does not saturate the disk and starve foreground traffic. 0 = unlimited. |

Rules of thumb:

- **Storage cost** is roughly `PitrWindow × write throughput` of retained WAL, plus the base images
  overlapping the window. Pick the window from how far back you realistically need to recover.
- **Restore speed** depends on how much log must be replayed after the base image — so taking a fresh
  full backup more often makes restores faster at the cost of more snapshot overhead. This cadence is
  set by whatever schedules your backups (an operator/cron calling `TakeFullBackup`), **not** by
  `BaseSnapshotInterval` — that setting only governs how long WAL is retained so an incremental can
  still be cut against the last base.
- **A `T` outside the window is rejected.** If you need to recover further back than the window, you
  need an external archive of older full backups; the live system intentionally does not keep them.
- **Restore-target coverage is exact, not wall-clock-based.** A resolved chain reports its recoverable
  window `[MinRecoverablePhysicalMs, MaxRecoverablePhysicalMs]` on the head (Full) entry of
  `GetBackupChain`, and restore rejects a target outside it with a `TargetOutsideCoverage` outcome —
  independent of how much time has passed since the backup was taken.

**Persisted-state contract (what a PITR image contains):**

- **Key-values with history** (the default) are captured *exactly* as-of the cut — the newest revision
  with `LastModified ≤ cut`, rolling back anything newer.
- **Keys written with `SetNoRevision`** keep no history, so a value written after the cut cannot be
  rolled back and an overwrite is indistinguishable from a brand-new key. A backup **fails closed**
  (`ExactCheckpointUnavailable`) if any such key was modified after the cut, rather than silently
  dropping or over-including it. Reserve `SetNoRevision` for keys you don't need point-in-time recovery
  on, or expect backups to fail while such keys are being written.
- **Locks are not part of a PITR image.** They are volatile lease/coordination state with no history;
  including a snapshot would leak stale or post-cut locks. A restored/bootstrapped node starts with no
  persisted locks and re-establishes them at runtime (from the cluster / re-acquisition).

**The pruned-history floor (why an as-of cut below a point can be refused):**

Exact as-of reconstruction (§10, first bullet) depends on the *older* revisions of a key still being
present — to roll a key back to time `T`, the revision that was current at `T` must not have been
trimmed. But MVCC retention *does* trim old revisions to bound storage. So each backend that prunes
tracks a **pruned-history floor** `W`: the highest timestamp below which some key's boundary revision
may already be gone. A full backup whose cut falls **below `W` fails closed**
(`ExactCheckpointUnavailable`) rather than reconstruct a state it can't prove is exact. At or above
`W`, every key's boundary still survives, so the cut is exact.

> **Decision — persist the floor write-ahead, and fail closed if it is lost or unreadable.** The floor
> is only useful if it never *under*-reports what was pruned. Two failure modes are closed off
> explicitly:
> - **It is written before the deletes it accounts for.** Each prune persists the advanced floor
>   (in the backend's own metadata, per shard) *ahead of* deleting the revisions that advance it, so a
>   crash between the two steps leaves the floor covering data that is still present — conservative,
>   never optimistic. The floor is monotonic and durable: pruned history does not come back, so the
>   floor only ever moves up and survives restart.
> - **A missing or corrupt floor is treated as "everything might be pruned," not "nothing was."** If a
>   backend that may have pruned can't read back a trustworthy floor, it reports a *fail-closed* floor
>   that refuses every cut until the metadata is repaired — instead of defaulting to zero, which would
>   silently declare all history intact and let an inexact backup through. (Backends that never prune,
>   such as the in-memory engine, keep the floor at zero legitimately.)

> Concretely, a prune advances `W` to the **oldest surviving** revision timestamp across the keys it
> trimmed — including the case where a key's *last* history row is removed, which must still push the
> floor up rather than leave it at zero.

### Reclaiming backup disk: retention and the orphan sweep

Backups accumulate. Two independent mechanisms keep the backup directory bounded, and it is worth
being precise about which does what:

- **The orphan sweep** reclaims artifacts that no valid backup accounts for — an artifact directory a
  crashed or interrupted backup left behind with no manifest, and staging/temporary remnants
  (`.tmp_`/`.staging_`/`.quarantine_`/`.merge_`) of an interrupted publish, delete, or restore. It
  **never touches a valid backup**, so it always runs, regardless of any retention configuration.
- **Retention** deletes *valid* backups once they fall outside the configured bounds. It is **off by
  default** — Kahuna never deletes a backup you took unless you opt in with a `BackupRetention*`
  setting.

> **Decision — sweep orphans always, delete real backups only on explicit opt-in.** Reclaiming a
> crash-orphaned directory is unambiguously safe (nothing valid points at it), so it needs no
> configuration. Deleting a *real* backup is destructive and irreversible, so it happens only when an
> operator sets an explicit bound — a data store should never quietly discard the recovery points you
> asked it to take.

Retention operates on **whole chains**, never individual backups. Recall (§6) that a chain is a Full
root plus the incrementals built on it; deleting a Full out from under a retained incremental would
strand it. So retention keeps the most-recent chains that fit within the enabled bounds
(`MaxChains` / `MaxAge` / `MaxTotalBytes`, evaluated newest-first) and deletes the rest **root and all
incrementals together**, always leaving at least the single newest chain even if it alone exceeds a
byte budget.

> **Decision — chain-granular retention, and keep a retained leaf's whole ancestry.** A retained
> incremental is only restorable if its Full root and every intermediate parent still exist, so the
> unit of deletion has to be the chain, not the backup. Keeping a chain therefore *pins* its entire
> parent closure — a Full shared by a still-retained branch is never deleted — and each doomed chain is
> removed **descendants-first**, so no surviving backup ever briefly points at a deleted parent.

**Crash safety.** A delete removes the **manifest first**, then the artifacts. If the process dies
between the two steps, the worst case is an artifact directory with no manifest — an orphan the sweep
reclaims on its next pass — never a manifest that resolves to missing artifacts. Symlinks are never
followed: if an artifact directory is itself a reparse point, only the link is removed, so a
swapped-in symlink can't redirect a delete outside the backup directory.

**When GC runs.** A GC pass (sweep, then retention if configured) runs **inline after every backup**,
and on a **periodic tick** (`BackupGcInterval`, default 1 hour) whose first firing shortly after
startup doubles as a **startup sweep** — so crash-orphaned artifacts and age-based expiry are reclaimed
even on a node that has stopped taking backups. GC is serialized against backup creation on each node,
so a sweep never races a backup that is midway through writing its directory. You can also trigger a
pass on demand, or preview one without deleting anything (see §11g).

> **Note — retention is per-node and local.** Each node runs GC against its own backup directory. In a
> multi-node cluster there is not yet a single cluster-wide retention owner; that coordination is
> future work, tracked with the coordinated-backup ownership contract.

**Observability.** Each pass emits counters (OpenTelemetry names; Prometheus exporters translate dots
to underscores):

- `kahuna.backup.gc.runs` — passes completed.
- `kahuna.backup.gc.orphans_reclaimed` — orphaned/leftover artifacts reclaimed.
- `kahuna.backup.gc.retention_deletions` — backups deleted by retention.
- `kahuna.backup.gc.bytes_reclaimed` — artifact bytes freed by retention.

---

## 11. Triggering backups and inspecting the catalog

### 11a. Configuration

Add `--pitr-backup-dir <path>` to the server command line (or set `BackupDir` in the JSON config) to
enable backups.  The same directory is used for catalog manifests and artifact subdirectories.

Set `--pitr-backup-cluster-id <id>` (JSON `BackupClusterId`) to the **same value on every node** of a
cluster. It is stamped into every manifest and gates chain resolution: a chain may not span manifests
carrying different cluster ids, so a foreign cluster's artifacts can never be chained or restored here.
Leaving it empty disables that guard (a null id is treated as "unknown" and skipped).

### 11a-i. Cluster backups and catalog placement

A **coordinated** backup (`/v1/backups/coordinated`) is the cluster-wide product: it is accepted only
on the node that leads the meta partition (the *backup coordinator*). A request to any other node is
rejected with outcome `NotBackupCoordinator` (HTTP 503 / gRPC `Unavailable`) — retry against the
current leader. Because leadership can move, **the coordinator changes over time**, so a coordinated
backup is written to whichever node was coordinator at the time.

> **The catalog is whatever is at `BackupDir`.** `ListBackups`, chain resolution, and parent lookup
> read the local `BackupDir` directly. For these to be **node-independent** — the same answer no matter
> which node you ask — every node's `BackupDir` must point at **shared storage** (the same directory on
> a shared/replicated filesystem or object store). With shared storage the catalog is one logical
> catalog by construction. With **node-local** directories each node holds only the backups it wrote
> while it was coordinator, so any single node's listing is a *partial* view of the cluster's backups —
> there is no server-side routing that reconstructs a whole-cluster catalog from scattered local disks.

To make a partial catalog visible, every listing entry now carries the identity of the backup it
describes: `clusterId` (the configured cluster id) and `coordinatorNode` (the node that produced it).
On a shared catalog every node returns the same set with a mix of `coordinatorNode` values; if a
node's listing shows only its own `coordinatorNode`, its `BackupDir` is node-local and the view is
partial. (The plain `/v1/backups/full` and `/v1/backups/incremental` endpoints remain per-node,
node-local operations intended for single-node or diagnostic use.)

### 11a-ii. Confidentiality and authenticity

A node-wide backup contains **all** tenant data — including anything an application stored as a secret
(e.g. password hashes) — as cleartext, because it is a physical image of the storage engine plus WAL
segments. Protect it accordingly.

- **Restrictive permissions (automatic).** Backup directories are created `0700` and every manifest and
  artifact file `0600` on POSIX, so other users on the host cannot read the data. On startup the server
  **refuses** a `BackupDir` that is a symlink or group/world-writable (outcome `InsecureRoot`) rather than
  writing tenant data somewhere another user could read or tamper with it. On Windows, NTFS ACL
  inheritance from an access-controlled parent is relied upon.
- **Authenticated manifests (opt-in).** By default a manifest's per-file digests are plain SHA-256 stored
  in the (plaintext) manifest — good against accidental corruption, but anyone who can write `BackupDir`
  could rewrite a file *and* its recorded digest. Set `--pitr-backup-mac-key-file <path>` (JSON
  `BackupMacKeyFile`) to a file holding a secret key, **identical on every node and kept outside the
  backup directory**, readable only by the server user. Manifests are then signed with HMAC-SHA-256 over
  their identity, coverage, and digest map, and the tag is verified before restore — so a tampered
  digest/coverage/identity, a swapped file, or a stripped tag fails authentication. Note: enabling the key
  means backups taken **before** it was configured are unsigned and can no longer be restored until
  re-taken; a configured-but-missing/empty key file fails startup rather than silently disabling
  authentication.
- **Encryption at rest (not provided — your responsibility).** Kahuna does not yet encrypt backup
  artifacts. Store `BackupDir` on an **encrypted, access-controlled volume** (or an object store with
  server-side encryption). Transport the artifacts only over secure channels. A dedicated backup-cipher
  seam is planned as future work.
- **Sanitized errors with correlation.** Backup/restore failures return a stable, path-free message with
  an `(operation <id>)` tag; the full detail (paths, backend exception) is logged server-side under the
  same id. Quote the id when diagnosing — no absolute paths or raw backend text are leaked to callers.

### 11b. REST API

All endpoints return or consume JSON.  Responses use camelCase field names.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/backups/full` | Take a full backup now. |
| `POST` | `/v1/backups/incremental` | Take an incremental backup. Body: `{"parentBackupId":"<guid>"}`. |
| `POST` | `/v1/backups/coordinated` | Take a cluster-wide coordinated full backup. |
| `GET`  | `/v1/backups` | List all backups in the local catalog. |
| `GET`  | `/v1/backups/{id}/chain` | Resolve and validate the chain ending at `id`. |
| `POST` | `/v1/backups/validate-chain` | Validate a chain. Body: `{"leafBackupId":"<guid>","targetDir":"","targetTimeMs":0}`. |
| `POST` | `/v1/restore` | Offline restore: copies Full checkpoint to `targetDir` and replays WAL to `targetTimeMs`. Body: `{"leafBackupId":"<guid>","targetDir":"/data/restored","targetTimeMs":0}`. |
| `POST` | `/v1/backups/gc` | Reclaim backup disk: sweep orphaned/leftover artifacts and enforce retention. `?dryRun=true` returns the inventory of what *would* be reclaimed (with reasons and bytes) without deleting anything; default `false` applies it. |

All endpoints return `503` when `--pitr-backup-dir` is not set on the target node.

### 11c. gRPC API

A `Backups` service mirrors every REST endpoint.  RPC names match the action:
`TakeFullBackup`, `TakeIncrementalBackup`, `TakeCoordinatedBackup`, `ListBackups`, `GetBackupChain`,
`ValidateChain`, `Restore`, `RunBackupGarbageCollection`.  See
`Kahuna.Shared/Communication/Grpc/Protos/backups.proto` for message definitions.

### 11d. `KahunaClient` methods

```csharp
KahunaClient client = new(urls);
KahunaBackupInfo full   = await client.TakeFullBackupAsync();
KahunaBackupInfo incr   = await client.TakeIncrementalBackupAsync(full.BackupId);
KahunaBackupInfo coord  = await client.TakeCoordinatedBackupAsync();
List<KahunaBackupInfo> all   = await client.ListBackupsAsync();
List<KahunaBackupInfo> chain = await client.GetBackupChainAsync(incr.BackupId);

// Offline restore: copies Full checkpoint to /data/restored and replays WAL
KahunaRestoreResponse result = await client.RestoreAsync(
    leafBackupId: incr.BackupId,
    targetDir:    "/data/restored",
    targetTimeMs: 0);  // 0 = chain max; or Unix ms for a specific T
// Then: start a new node with --storage-path=/data/restored

// Reclaim backup disk (orphan sweep + retention). dryRun previews without deleting.
KahunaBackupGcResult preview = await client.RunBackupGarbageCollectionAsync(dryRun: true);
KahunaBackupGcResult done    = await client.RunBackupGarbageCollectionAsync(dryRun: false);
// done.RetentionDeletions / done.OrphanReclamations / done.BytesReclaimed
```

### 11e. `kahuna.control` CLI verbs

```
# Take a full backup
kahuna.control --backup-full

# Take a coordinated backup (recommended for production)
kahuna.control --backup-coordinated

# Take an incremental backup on top of a previous one
kahuna.control --backup-incremental --parent-backup-id <guid>

# List all backups
kahuna.control --list-backups

# Resolve and validate a chain
kahuna.control --backup-chain <leaf-guid>

# Reclaim backup disk (orphan sweep + retention); add --backup-gc-dry-run to preview without deleting
kahuna.control --backup-gc
kahuna.control --backup-gc --backup-gc-dry-run

# Offline restore to a target directory (0 = chain max; set --target-time-ms for a specific T)
kahuna.control --restore <leaf-guid> --target-dir /data/restored
kahuna.control --restore <leaf-guid> --target-dir /data/restored --target-time-ms 1750000000000

# Output as JSON
kahuna.control --list-backups --format json
```

Interactive console verbs (type at the `>>` prompt):

```
backup full
backup coordinated
list backups
```

### 11f. Online versus offline operations (v1 boundary)

In v1 the following operations are **online** — they run while the node is serving traffic, without
taking the node offline:

- Trigger a full, incremental, or coordinated backup.
- List backups and inspect the catalog.
- Resolve and validate a chain.
- Trigger (or dry-run) a garbage-collection pass — orphan sweep + retention.

> **Online is not free.** "Online" means these run without stopping the node — not that they are
> invisible to client latency. Taking the base image consumes real I/O and cache bandwidth on the
> backup volume, and for the **SQLite** backend the `VACUUM INTO` that copies a shard holds that
> shard's writer lock for the whole copy, so writes to that shard stall until it finishes (reads and
> other shards are unaffected). RocksDB's checkpoint is a near-instant hard-link and does not hold a
> writer lock, but still competes for disk bandwidth. Restore replays segments with memory bounded to
> one write batch (segments stream one record at a time), but it too consumes I/O. Prefer taking
> backups off-peak, or against a follower, on latency-sensitive SQLite deployments; benchmark the
> impact for your workload before advertising backups as latency-transparent.
>
> A repeatable backend-level benchmark ships in the test suite
> (`BenchmarkOnlineBackupImpact`, env-gated so CI skips it — run it with
> `KAHUNA_BENCH=1 dotnet test --filter FullyQualifiedName~BenchmarkOnlineBackupImpact`). It measures
> foreground write p50/p95/p99 while a checkpoint runs under concurrent load. Indicative shape on a
> dev laptop (40k×1 KB seed, 4 writers — measure on your own hardware): **SQLite** — aggregate
> percentiles barely move, but a write landing on the shard being vacuumed stalls for that shard's
> `VACUUM INTO` (hundreds of milliseconds worst-case), so the impact is a tail-latency spike, not a
> throughput loss; **RocksDB** — the hard-link checkpoint finishes in well under a second and inflates
> p95/p99 only modestly (≈1.5–2×) from I/O contention, with no multi-hundred-millisecond stall.

The following is an **offline operation** (runs via the REST/gRPC/client/CLI surface but writes to
the local filesystem of the node receiving the request):

- **`POST /v1/restore` / `client.RestoreAsync` / `--restore`** — copies the Full backup's checkpoint
  to `targetDir`, replays incremental WAL segments up to the given time `T`, then returns.  The
  operator then starts a **fresh** node with `--storage-path=<targetDir>` to use the restored image.
  The restored node joins the cluster and catches up via normal Raft AppendEntries.

> **Decision — confine every restore target under a server-owned restore root, comparing paths
> case-sensitively.** A restore writes files to a caller-supplied `targetDir`; left unchecked, a
> network caller could aim it anywhere on the node's filesystem. So a node only accepts restore over
> the network when it is configured with a **restore root**, and it rejects any `targetDir` that does
> not resolve to a path inside that root. The containment check is **ordinal (case-sensitive)**: on a
> case-sensitive filesystem `/data/restore` and `/data/Restore` are genuinely different directories,
> so a case-insensitive comparison would accept a target that actually lands *outside* the root. The
> check normalizes both paths and confirms the target equals the root or sits beneath `root +
> separator`; anything else is refused before any file is staged. When in doubt it **fails closed** —
> rejecting a legitimate-but-unconfined target is safe; accepting an escaping one is not.

The following is **out of scope for v1 and not supported**:

- **Hot in-place restore** of a running node.  Shutdown the node, use the offline restore above to
  populate a `targetDir`, then restart with `--storage-path=<targetDir>`.

---

## 12. What to expect at scale

- Backups read the WAL a page at a time and write segment files atomically. They do not stop a
  partition, but they are not latency-free: the base image consumes I/O/cache bandwidth, and the
  SQLite `VACUUM INTO` copy holds the affected shard's writer lock for its duration (see §11f).
- **Restore streams; its memory is bounded by one write batch, not the segment size.** Segments are
  stored as JSON Lines (one record per line) and replayed one record at a time, so restoring a
  multi-gigabyte incremental does not load the whole segment into memory. Segment *verification* streams
  the same way. (Backups written before this format — a single JSON array — are still read, via a
  whole-file parse, for compatibility.)
- Incremental backups are proportional to the *changes* since the last one, not the dataset size, so
  frequent incrementals stay cheap.
- The retention floor advances on a slow tick (tied to the snapshot interval), so its overhead is
  negligible.
- Backup artifacts carry SHA-256 checksums, and chains are validated before restore, so corruption is
  detected rather than silently restored.

Backup and restore emit metrics (OpenTelemetry names; Prometheus exporters translate dots to
underscores) so you can watch throughput, latency, and failures: `kahuna.backup.operations` /
`kahuna.backup.failures` / `kahuna.backup.bytes` / `kahuna.backup.duration_ms`, and
`kahuna.restore.operations` / `kahuna.restore.failures` / `kahuna.restore.bytes` /
`kahuna.restore.entries_applied` / `kahuna.restore.duration_ms` (plus the `kahuna.backup.gc.*` counters
in §10). Restore's checkpoint copy honors the `--backup-restore-throttle-mbps` budget above.

Artifact verification hardens several ways an artifact directory could lie about its contents, all
checked before any file is trusted for copy or replay:

- **Every declared artifact must match its recorded size *and* checksum**, and the size and checksum
  key-sets must correspond exactly — a size with no matching checksum (or vice-versa) is a corrupt
  manifest, not a partial match to tolerate.
- **Symlinks / reparse points are rejected — including at the artifact root itself,** not only on the
  path between a file and its root. A symlinked per-backup directory would otherwise pass every child
  check while redirecting reads, copies, and replay to a tree *outside* the configured backup
  directory; the staging checkpoint directory is checked the same way before its files are opened.
- **Manifest schema is validated before any filesystem work** — type/parent/base-cut consistency, no
  duplicate partition ranges, valid index/HLC bounds, and the artifact-name set the backup type
  requires — so a structurally invalid manifest fails fast instead of part-way through a restore.
- **Bootstrap seeds the WAL one partition at a time and checks each write is durable.** The backend
  restore it follows is idempotent and each checkpoint write overwrites by key, so a partial failure
  is reported precisely and the bootstrap is safe to re-run — rather than being reported as a success
  while the WAL trails the state already restored into the backend.

---

## 13. The mental model in one paragraph

Kahuna keeps an ordered, timestamped log of every change and a periodic base image of the storage
engine. A **full backup** is a base image plus a manifest of what it covers; an **incremental** is
the log slice since the last backup; a **restore** loads a base image and replays the log, stopping
at exactly the timestamp you ask for. A **sliding window** (1–6 hours) bounds how far back you can go
and how much log is kept, by holding a retention floor that prevents the log from being trimmed too
soon. Across the cluster, one chosen timestamp gives every partition a consistent cut. Uncommitted
transactions are never in a backup, replays are idempotent, and every chain is validated before it is
trusted. The whole subsystem is built to **fail closed**: a full backup waits until its committed
writes are provably on their way to disk and refuses to publish otherwise; a cut below the
pruned-history floor is refused rather than reconstructed inexactly; a restore is confined under its
server-owned root; and a corrupt or symlinked artifact is rejected before it is trusted — so a backup
or restore either reproduces a real past state or refuses, but never silently invents one.
