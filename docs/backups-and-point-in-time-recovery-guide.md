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
- **`BaseSnapshotInterval`** (default 30 minutes) — how often a fresh base image is taken. It must be
  no larger than the window, so a base image always exists within reach of the oldest recoverable
  point.

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
> expensive and slow. Instead Kahuna takes an occasional **base image** and then captures cheap
> **deltas** (log slices) between them. Restoring means "load the nearest base, then replay the few
> deltas on top." This is the classic trade that makes frequent backups affordable and restores fast,
> as long as a base image is never too far behind (which `BaseSnapshotInterval` guarantees).

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
   2. flush   = drain pending writes to the storage backend   ── now disk holds everything ≤ M
   3. snapshot the storage backend  →  base image
   4. write manifest (covers up to M) + checksums  →  catalog
```

Recall from §2 that a change can be *committed in the log* but *not yet flushed to disk*. The base
image is a snapshot of the disk. So the order matters:

- **Read `M` (the last committed position) first.**
- **Then flush**, which pushes everything committed-so-far onto disk — a superset of `M`.
- **Then snapshot.** The snapshot is now guaranteed to contain everything up to `M`.

> **Decision — capture the position before flushing, never after.** If you snapshotted first and then
> claimed it covered the latest committed position, a change committing in the gap would be *named in
> the manifest but absent from the image*. And because a full backup carries **no log slice of its
> own**, there would be nothing to replay it back from — it would be silently lost on restore. Reading
> `M` before flushing closes that gap: the image always contains at least what the manifest promises.
> (If it contains a little *more* than `M`, that's harmless — replay is idempotent, see §7.)

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
| `BaseSnapshotInterval` (`--base-snapshot-interval`, seconds) | 30 minutes | How often a fresh base image is taken. Smaller = faster restores (less log to replay), more snapshot overhead. Must be ≤ `PitrWindow`. |

Rules of thumb:

- **Storage cost** is roughly `PitrWindow × write throughput` of retained WAL, plus the base images
  overlapping the window. Pick the window from how far back you realistically need to recover.
- **Restore speed** depends on how much log must be replayed after the base image — so a shorter
  `BaseSnapshotInterval` makes restores faster at the cost of taking snapshots more often.
- **A `T` outside the window is rejected.** If you need to recover further back than the window, you
  need an external archive of older full backups; the live system intentionally does not keep them.

---

## 11. What to expect at scale

- Backups read the WAL a page at a time and write segment files atomically; they do not block the
  partitions serving live traffic.
- Incremental backups are proportional to the *changes* since the last one, not the dataset size, so
  frequent incrementals stay cheap.
- The retention floor advances on a slow tick (tied to the snapshot interval), so its overhead is
  negligible.
- Backup artifacts carry SHA-256 checksums, and chains are validated before restore, so corruption is
  detected rather than silently restored.

---

## 12. The mental model in one paragraph

Kahuna keeps an ordered, timestamped log of every change and a periodic base image of the storage
engine. A **full backup** is a base image plus a manifest of what it covers; an **incremental** is
the log slice since the last backup; a **restore** loads a base image and replays the log, stopping
at exactly the timestamp you ask for. A **sliding window** (1–6 hours) bounds how far back you can go
and how much log is kept, by holding a retention floor that prevents the log from being trimmed too
soon. Across the cluster, one chosen timestamp gives every partition a consistent cut. Uncommitted
transactions are never in a backup, replays are idempotent, and every chain is validated before it is
trusted — so a restore either reproduces a real past state or refuses, but never silently invents
one.
