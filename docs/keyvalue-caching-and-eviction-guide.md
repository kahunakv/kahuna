# Kahuna Key/Value caching and eviction guide

This guide explains how Kahuna keeps key/value data in memory, how it decides what to keep and what
to throw away, and why those choices are safe. It is written for two audiences:

- **Users** running Kahuna (directly or as the storage engine behind a database such as CamusDB) who
  want to understand memory behavior, tuning knobs, and what to expect at scale.
- **Developers** maintaining the code who need the mental model and the invariants that must hold.

No prior knowledge of Kahuna's internals is assumed. Concepts are introduced as they come up.

---

## 1. The big picture

Kahuna is a distributed key/value store. Keys are spread across many independent **partitions**, and
each partition is owned by one **actor** — a single-threaded worker that processes requests for its
keys one at a time. Because an actor is single-threaded, it never needs locks for its own data, but
it also means any slow operation inside an actor stalls every other request for that partition. This
is the single most important fact for understanding the eviction design: **all the bookkeeping below
is built to keep per-request work small and predictable.**

Each actor holds its keys in an **in-memory cache**. Depending on the key's durability, that cache is
either a fast copy of data that also lives on disk, or the only place the data exists:

- **Persistent keys** are written through to a durable backend (RocksDB or SQLite) and also
  replicated through the Raft consensus log. The in-memory copy is a *cache*: if it's dropped, the
  next read transparently re-loads it from disk. Evicting a persistent key frees memory and loses
  nothing.
- **Ephemeral keys** live only in memory. They are meant for short-lived, best-effort data (think
  leases or transient coordination state). Evicting an ephemeral key *does* lose it — that is by
  design, and callers choosing `Ephemeral` accept it.

> **Concept — durability.** "Durable" means *survives a crash*. Persistent keys are durable because
> they are on disk and in the replicated log. Ephemeral keys are not durable; they trade safety for
> speed and simplicity.

Because memory is finite, each actor has a **budget**. When it exceeds the budget, a background
**collector** reclaims memory by removing entries. The rest of this guide is about how the budget
works, how the collector decides what to remove, and the safety rules that make removal correct.

---

## 2. The in-memory store

Inside each actor, keys are kept in a **B-tree** ordered by key. A B-tree is a balanced, sorted
structure that keeps related keys near each other on disk-page-sized nodes, which makes both
single-key lookups and ordered range scans efficient.

> **Concept — why sorted order matters.** A database asking Kahuna to "read all rows of a table" is
> really asking to "read every key with this prefix, in order." A sorted store answers that by
> walking a contiguous slice instead of hunting all over memory.

Every stored value is a `KeyValueEntry`. Beyond the obvious fields (value bytes, revision number,
expiration time) it carries some bookkeeping the eviction system relies on, introduced in the
sections below.

---

## 3. Budgets: how much memory an actor may use

An actor is "over budget" when either of two limits is exceeded:

| Limit | Default | Meaning |
|-------|---------|---------|
| `MaxEntriesPerActor` | 50,000 | Maximum number of cached keys. |
| `MaxBytesPerActor` | 256 MiB | Approximate maximum bytes of cached data. |

The byte figure is an *estimate* — object headers and dictionary overhead are approximated rather
than measured exactly. It also folds in the overhead of the helper structures (described next) so
that their growth is visible to the budget and can trigger reclamation.

When you run many partitions, the *total* memory ceiling is roughly `partitions × MaxBytesPerActor`.
This is also how Kahuna scales to large datasets: a big table is split across many partitions, so no
single actor ever holds the whole thing. Cold keys are evicted and re-read from disk on demand; hot
keys stay cached. This is the same "cache hot data, fall back to disk" model used by large-scale
databases.

---

## 4. The collector: when and how reclamation happens

Reclamation is done by the **collector**, which runs inside the actor (so it competes with normal
requests for that single thread). Two rules keep it cheap:

1. **It only runs when over budget.** A collect pass is considered roughly every few hundred
   operations, but it does real work only if the actor is above its limits.
2. **It does bounded work per pass.** At most `CollectBatchMax` (default 1,000) entries are evicted
   in one cycle. If that isn't enough to get back under budget, it schedules a follow-up rather than
   running long.

The crucial property is that **the collector never scans the whole store.** A naive design would
walk every cached key each cycle looking for things to remove; with hundreds of thousands of keys,
that walk would freeze the partition repeatedly. Instead, the collector consults three purpose-built
structures that are kept up to date as keys change, and each reclamation step costs time proportional
to *what it reclaims*, not to how much is cached.

### 4.1 Tombstone queue — reclaiming deleted keys

When a key is deleted, its entry isn't removed immediately; it becomes a **tombstone** (a marker in
the `Deleted`/`Undefined` state). Tombstones exist briefly so that concurrent readers and the
replication machinery see a consistent "this key is gone" answer.

> **Concept — tombstone.** A gravestone for a deleted key: the data is gone, but a marker remains so
> the system can distinguish "deleted" from "never existed" until everyone has caught up.

Each new tombstone's key is pushed onto a simple **first-in-first-out queue**. The collector drains
this queue to find deletable entries directly, instead of searching for them. Before removing one it
re-checks the entry is still a tombstone — if the key was re-created in the meantime, the stale queue
entry is harmlessly skipped.

### 4.2 Expiry heap — reclaiming entries whose time is up

Keys can be given a **time-to-live (TTL)**: an expiration timestamp after which they should vanish.
To reclaim them without scanning, every key with a TTL is tracked in a **min-heap** ordered by
expiration time.

> **Concept — min-heap.** A structure that always hands you the smallest item first. Ordered by
> expiration time, it always offers the *soonest-to-expire* key next.

The collector pops from the heap while the earliest entry is already past its deadline, and **stops
at the first key that hasn't expired yet** — because nothing later in the heap can be more overdue.
This makes expiry reclamation cost time proportional to the number of *expired* keys, not the total.

Because an entry's TTL can be extended (pushing its deadline later), the heap can contain stale
entries for a key. That's fine: when the collector pops one, it re-checks the live entry's actual
expiration and discards the pop if it no longer matches.

### 4.3 LRU list — reclaiming cold keys to stay under budget

After tombstones and expired keys are reclaimed, the actor may still be over budget simply because it
is caching more *live* keys than the limit allows. Now it must evict some genuinely valid data — and
it should evict the data least likely to be needed again. The standard heuristic is **least-recently-
used (LRU)**: throw out whatever hasn't been touched for the longest time.

> **Concept — LRU.** "Least Recently Used." If you must drop something from a cache, drop what you
> haven't touched in the longest time, betting that recently used items will be used again soon.

Kahuna implements LRU with an **intrusive doubly-linked list** woven through the entries themselves:

- Every cached entry has two extra links, to the entry "colder" than it and the one "hotter" than it.
- The list has two ends: the **head** is the coldest entry; the **tail** is the hottest.
- Whenever a key is read or written, it is moved to the tail (it just became the hottest).
- To evict, the collector walks from the head (coldest) and removes entries until back under budget.

> **Concept — intrusive list & why it's fast.** "Intrusive" means the link pointers live inside the
> data objects, not in a separate container. Moving an entry to the hot end or removing the coldest
> one is a couple of pointer updates — constant time, no searching, no sorting. This is what lets the
> cache stay accurate *and* cheap even with hundreds of thousands of keys.

An earlier approach sampled a handful of random keys and evicted the oldest of the sample. That is
cheaper to bolt on but inaccurate — it can evict a key that's about to be used again. The intrusive
list gives true recency ordering at the same constant per-access cost.

---

## 5. The two safety rules

The collector may *consider* any entry, but it will **never** evict one that violates either rule
below. These are what make "drop data to save memory" safe rather than dangerous.

### 5.1 Never evict an entry with an in-flight intent

While a key is being written or replicated, its entry carries an **intent** — a `WriteIntent` (a
transaction is modifying it) or a `ReplicationIntent` (a committed change is still propagating). An
entry with a live intent is in the middle of an operation, so evicting it would corrupt that
operation. The collector skips such entries entirely.

> **Concept — write intent.** A "reserved" sign on a key: a transaction has claimed it and may be
> about to change it. Other operations must respect the claim, and the cache must not drop the entry
> out from under it.

### 5.2 Never evict an entry whose latest change isn't on disk yet

This is the subtle one, and the most important for correctness.

When a persistent key is committed, the change is applied to the in-memory entry **immediately**, but
the write to the durable backend happens slightly later, in batches, on a background timer. For a
short window the in-memory copy is *newer than disk*.

Now recall that evicting a persistent key is supposed to be safe because the next read re-loads it
from disk. But during that window, disk is stale. If the collector evicted the entry right then, the
next read would fall back to disk and return the **old** value (or "not found" for a brand-new key).
That's a correctness violation — a reader would not see a write that already succeeded.

To prevent this, each entry records the highest revision known to be safely on disk
(`FlushedRevision`). An entry is considered **dirty** while its current revision is ahead of that and
it was modified recently enough that the background flush may not have completed. The collector never
evicts a dirty entry. Once the flush window has comfortably passed, the entry becomes eligible again.

> **Concept — dirty vs. clean.** A *dirty* cache entry holds changes not yet saved to durable
> storage; a *clean* entry's contents are already safely persisted. You can drop a clean entry freely
> (you can always reload it); dropping a dirty one would lose or hide a change.

The same rule protects deletes: a delete that hasn't reached disk yet must not be evicted, or a read
could resurrect the old value from the stale on-disk copy.

> **Note on the safety window.** Kahuna currently judges "recently modified" with a time window
> (comfortably larger than the background-flush interval) rather than a precise per-write
> confirmation. This keeps the actor decoupled from the background writer. The trade-off: an entry is
> pinned in memory a little longer than strictly necessary, and a pathologically slow flush is
> covered by the window's generous floor.

---

## 6. Keeping per-entry metadata bounded

Two kinds of per-entry history could otherwise grow without limit. Both are trimmed *inline*, exactly
where they grow, so the collector never has to sweep the whole store to bound them.

- **Revision history.** Each entry keeps a small number of recent prior versions so that
  point-in-time ("as of") reads can be answered from memory. This history is trimmed back to
  `RevisionRetention` (default 16) every time a new revision is archived.
- **Transaction snapshots (MVCC).** While transactions are in flight, an entry may hold per-
  transaction snapshots used for isolation and conflict detection. When a transaction resolves
  (commits, rolls back, or releases its lock), its own snapshot is removed and any expired snapshots
  left by other transactions are cleaned up at the same time.

> **Concept — MVCC.** "Multi-Version Concurrency Control." Instead of blocking, the store keeps
> multiple versions of a value so each transaction sees a consistent snapshot. Those extra versions
> must be cleaned up once no transaction needs them.

> **Maintainer invariant.** Because the collector no longer trims metadata, these inline trims are
> the *only* enforcement. Any new code path that archives a revision or resolves a transaction must
> perform the corresponding trim, or that history can grow unbounded.

---

## 7. Reading data back: scans and disk fallback

Two read shapes interact with the cache differently, and the distinction matters at scale.

- **Single-key reads** check the cache first; on a miss for a persistent key they load from the
  backend and (usually) re-cache the entry.
- **Range scans** ("read everything with this prefix, in order") come in two forms:
  - **Paginated range scan** streams results one bounded page at a time using an opaque **cursor**,
    merging the in-memory and on-disk views as it goes. It carries a consistent point-in-time
    snapshot across pages and has no fixed size ceiling — you page until there's no more. This is the
    path to use for large or full-table iteration, and it never materializes the whole result in
    memory at once.

    > **Concept — cursor.** A bookmark. Instead of returning a million rows in one giant response,
    > the server returns a page plus a cursor that says "resume here," so memory use stays flat no
    > matter how big the result is.

  - **Non-paginated prefix/bucket scans** return the whole matching set in one call and are therefore
    capped (currently 4,096 entries). They're for small, known-bounded prefixes; for anything that
    might be large, use the paginated scan.

A subtle but important interaction: a range scan may encounter keys that live only on disk (they were
evicted). It reads those without forcing them back into the cache, so a big scan doesn't blow the
budget or disturb the recency ordering of the keys that are actually hot.

---

## 8. Observability

When the collector evicts anything, it emits a single summary line reporting how many entries were
reclaimed and the breakdown by source — **tombstone**, **expired**, and **lru** — along with the
resulting store size, byte estimate, elapsed time, and whether a follow-up pass is queued (the
*backlog* flag, meaning it hit the per-cycle cap while still over budget). If you see frequent
backlog or large `lru` counts, the actor is under sustained memory pressure and the budget may be
too small for the workload.

---

## 9. Tuning summary

| Knob | Default | Raise it to… | Lower it to… |
|------|---------|--------------|--------------|
| `MaxEntriesPerActor` | 50,000 | cache more keys per partition (more memory, fewer disk reads) | cap memory harder |
| `MaxBytesPerActor` | 256 MiB | cache more/larger values per partition | cap memory harder |
| `CollectBatchMax` | 1,000 | reclaim faster under bursty pressure (longer pauses) | keep collect pauses shorter |
| `RevisionRetention` | 16 | answer more "as of" reads from memory | shrink per-entry history |
| `DirtyObjectsWriterDelay` | 1,000 ms (embedded) | batch disk writes more aggressively | flush sooner (smaller dirty window) |

General guidance:

- If reads frequently miss the cache and hit disk, the budget is too small for the working set —
  raise `MaxEntriesPerActor` / `MaxBytesPerActor`, or add partitions so each holds less.
- If collect pauses are noticeable, lower `CollectBatchMax` so each pass does less at once.
- Remember every limit is **per actor/partition**: total memory scales with the number of partitions.

---

## 10. Mental model in one paragraph

Each partition is a single-threaded actor with a sorted in-memory cache bounded by a per-actor budget.
Deleted keys, expired keys, and cold keys are reclaimed by a collector that consults a tombstone
queue, an expiry heap, and an intrusive LRU list — so it only ever touches the entries it actually
reclaims, never the whole store. Two rules keep reclamation safe: never drop an entry that's mid-
operation (has an intent), and never drop one whose latest change might not be on disk yet (is dirty).
Persistent data dropped from the cache is harmless because reads fall back to disk; ephemeral data is
memory-only by design. Per-entry history (revisions and transaction snapshots) is trimmed where it
grows, not by scanning. The result is a cache that stays accurate and bounded while keeping every
per-request operation cheap and predictable.
