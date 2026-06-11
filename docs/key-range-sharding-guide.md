# Key-Range Sharding — Developer Guide

> **Audience:** developers who will maintain or extend Kahuna's key-range sharding system.
> This is a from-scratch explanation — it assumes you know what Kahuna is (a distributed
> key-value / lock / sequence store built on the Kommander Raft library) but does **not**
> assume you know anything about how ranges, splits, or descriptors work.

---

## 1. What problem does this solve?

### The starting point

Kahuna stores every key-value pair inside a **partition**. A partition is one Raft group —
a small cluster of replicas that agree, via consensus, on an ordered log of writes. Each
partition has a leader; writes go through the leader and replicate to followers.

Originally, a key was assigned to a partition by **hashing** it:

```
partition = hash(key) % numberOfPartitions
```

Hashing spreads keys evenly, which is great for balancing load. But it destroys **locality**:
two keys that look adjacent to a human — `users/0001` and `users/0002` — hash to unrelated
numbers and land on different partitions. There is no way to say "all the `users/*` keys live
together."

### Why locality matters

Two important things become impossible without locality:

1. **Ordered range scans.** A query like "give me every key between `users/0001` and
   `users/0500` in order" has to touch *every* partition, because the keys are scattered. You
   can't serve it from one place.

2. **Range-scoped locking.** Suppose you want to lock a slice of keys so a transaction can read
   them consistently. Under hashing, a "lock everything starting with `users/`" lock has to
   block the *entire* key space on its partition — because that partition holds an arbitrary
   random subset of *all* prefixes, there's no clean boundary. One transaction locking a table
   stalls every other transaction touching that partition.

This is the exact wall that systems like **Google Spanner** and **CockroachDB** hit, and they
solved it the same way: **key-range sharding**.

### The idea: shard by key range, not by hash

Instead of `hash(key)`, split the key space into **contiguous ranges**, ordered by the raw key:

```
Range A:  [ -∞   , users/0250 )   →  partition 5
Range B:  [users/0250, users/0700 )  →  partition 6
Range C:  [users/0700,  +∞   )   →  partition 7
```

Now `users/0001` … `users/0249` all live on partition 5, in order. A range scan touches only
the ranges it overlaps. A lock on `[users/0100, users/0200)` contends only with other
operations in that one slice — disjoint ranges never block each other.

The cost is that you give up automatic load spreading: if everyone writes sequentially to the
top of the key space (`users/0998`, `users/0999`, `users/1000`…), they all hit the *same*
range until it grows large enough to **split**. That's the tradeoff Spanner/CRDB accept, and
the system handles it by splitting hot/large ranges automatically.

### Both models coexist

Kahuna did not throw away hashing. Routing mode is a **per-key-space property**:

- **Hash mode** (the default) — keys spread across partitions, no locality. Good for caches,
  system data, anything that just wants throughput.
- **Key-range mode** (opt-in per prefix) — contiguous keys co-locate, enabling ordered scans
  and range locks.

A "key space" is just a key prefix (everything before the last `/`). Each key space is
unambiguously one mode or the other.

---

## 2. The core data structure: range descriptors

Everything starts with the **range descriptor** — the record that says "this slice of keys
lives on that partition."

```csharp
RangeDescriptor {
    string  KeySpace;      // the prefix this range belongs to, e.g. "users"
    string? StartKey;      // inclusive lower bound (null = -infinity within the key space)
    string? EndKey;        // exclusive upper bound (null = +infinity within the key space)
    int     PartitionId;   // the Raft group serving [StartKey, EndKey)
    long    Generation;    // version counter, bumped on every split/merge (the "fence")
}
```

Files: `Kahuna.Core/KeyValues/Ranges/RangeDescriptor.cs`, `RangeMap.cs`.

A few rules you must keep in your head — they are load-bearing:

- **Half-open intervals.** `StartKey` is *included*, `EndKey` is *excluded*: `[StartKey, EndKey)`.
  This is what makes adjacent ranges tile perfectly with no gap and no overlap. The end of one
  range is exactly the start of the next.

- **Ordinal comparison, always.** Every key comparison uses `StringComparison.Ordinal` /
  `string.CompareOrdinal` — never culture-aware. This is non-negotiable: culture-aware sorting
  would put `"r/9"` *before* `"r/10"` in some locales, silently corrupting the ordering the
  whole system depends on. If you add a new comparison path anywhere near ranges, it must be
  ordinal.

- **The two invariants** (call them "no gap / no overlap"): at every instant, every key in a
  key space is owned by **exactly one** range. `RangeMap.Validate()` checks this, and the test
  suite asserts it constantly.

### The RangeMap

`RangeMap` is the in-memory collection of descriptors for a key space, kept sorted by
`StartKey`. The key operations:

- `Find(keySpace, key)` — binary search for the single descriptor whose `[StartKey, EndKey)`
  contains `key`. This is the routing lookup.
- `FindIntersecting(...)` — all descriptors overlapping a `[start, end)` interval (used by
  range scans and range locks).
- `Validate()` — asserts no-gap/no-overlap.

```
Find("users", "users/0300"):

  [-∞, users/0250)  [users/0250, users/0700)  [users/0700, +∞)
                          ^^^^^^^^^^^^^^^^
                          users/0300 lands here  → partition 6
```

---

## 3. Where descriptors live: the meta partition

The descriptor map is the source of truth for routing. If it's wrong or inconsistent, keys go
to the wrong place. So it must itself be **consistent and replicated** — exactly the guarantee
a Raft group gives.

Kahuna stores all descriptors on a dedicated **meta partition**: **partition 1**.

```
Partition 0  →  Kommander's reserved system partition. Hands-off; it bypasses Kahuna's
                callbacks entirely. Never used for data.

Partition 1  →  the META partition. Holds the replicated RangeMap (the descriptors).
                Also participates in the hash pool, so hash-mode data may share it.

Partition 2+ →  DATA partitions. Key-range data always lands here (never on P1).
```

Why partition 1 and not partition 0? Partition 0 is Kommander-internal: it only accepts a
special system log type and routes its committed entries to Kommander's own coordinator,
*never* to Kahuna's `OnLogRestored` / `OnReplicationReceived` callbacks. We need the
descriptor writes to reach Kahuna's apply logic, so we use an ordinary replicated partition —
partition 1 — which always exists (`InitialPartitions > 0` is enforced) and is well-known.

### RangeMapStore — the single writer

`RangeMapStore.cs` owns the descriptor map. Its rules:

- **One writer.** All descriptor mutations funnel through `MutateAsync`, which only commits via
  a replicated partition-1 log entry, and only on the partition-1 leader. Serializing every
  change on one Raft log is what makes the no-gap/no-overlap invariant easy to hold — there is
  never a torn or racing update.

- **A dedicated log type.** Descriptor entries use `ReplicationTypes.RangeMap`, distinct from
  the normal `ReplicationTypes.KeyValues`. When a log entry arrives on partition 1 with the
  `RangeMap` type, it rebuilds the in-memory map instead of going through the KV path.

- **Durable checkpoint.** Here's a subtle trap to be aware of. The descriptor map's only
  durable home is partition 1's write-ahead log. But Kommander **compacts** (trims) committed
  log entries older than the last checkpoint — and a checkpoint marker carries *no state
  snapshot*. So naively checkpointing partition 1 would delete the very entries that hold the
  map, and a restart would rebuild an **empty map** → silent data loss.

  The fix: before every checkpoint, `RangeMapStore` writes a full snapshot of the map to a file
  (`{StoragePath}/rangemap_{revision}.snapshot`, atomic temp-write + rename). On startup it
  loads this snapshot *first*, then replays whatever WAL tail survived. Because every entry is a
  full idempotent snapshot, replay order doesn't matter. Only then is it safe to fire the
  checkpoint and let Kommander compact.

  **If you touch `RangeMapStore` persistence, preserve this ordering: snapshot to disk → then
  checkpoint. Reversing it loses the map.**

---

## 4. Routing: turning a key into a partition

```
                         ┌─────────────────────────────────────────┐
   key  ─────────────►   │  RangeRouting.Locate(registry, map,     │
                         │                 dataPartitionRouter, key)│
                         └───────────────┬─────────────────────────┘
                                         │
                  ┌──────────────────────┴───────────────────────┐
                  │  KeySpaceRegistry: what mode is this space?   │
                  └──────────────────────┬───────────────────────┘
                                         │
             ┌───────────── Hash ────────┴──────── KeyRange ──────────────┐
             ▼                                                            ▼
   DataPartitionRouter.Locate(key)                          RangeMap.Find(keySpace, key)
   hash over user partitions [1, N]                         → (PartitionId, Generation)
   → PartitionId, Generation = 0                            generation is the FENCE
```

All routing funnels through one static method, `RangeRouting.Locate(...)`
(`Ranges/RangeRouting.cs`). This single funnel is deliberate: it guarantees the two places
that route a key (see below) can never drift apart and disagree.

### KeySpaceRegistry

`KeySpaceRegistry.cs` answers "what routing mode is this key space?" It extracts the key space
as the prefix before the last `/` (matching how Kommander computes its hash boundary), and
returns `Hash` or `KeyRange`. Default is `Hash`. A space registered as `KeyRange` resolves via
the descriptor map; everything else hashes.

### DataPartitionRouter (the hash path)

`DataPartitionRouter.cs` is Kahuna's *own* hash assignment. Crucially, Kahuna owns this — it no
longer delegates to Kommander's `GetPartitionKey`. It hashes a key over the user-partition pool
`[1, InitialPartitions]` using `HashUtils.InversePrefixedHash(key, '/')`. Note partition 1 *is*
in the pool — hash data and the meta map coexist on it via distinct log types. Hash-mode
descriptors return `Generation = 0` (no fence; hash spaces never split).

### The two routing call sites (a maintenance hazard you must respect)

There are **two** independent places that derive a partition from a key. Both must agree, or a
split will misroute:

1. **`KeyValueLocator`** — routes a request to the right **leader node** (where do I send this
   operation?).
2. **`KeyValueProposalActor`** — once on the leader, re-derives the partition to **replicate
   into** (which Raft log do I append to?).

If only one of these consulted the descriptor map and the other still hashed, a write could be
routed to the correct leader but replicated into the wrong (stale) partition's log. That's why
both go through `RangeRouting.Locate`, and why the local consistent-hash **worker actor**
router (`KeyValuesManager.cs`, `ConsistentHashActor<KeyValueActor,…>`) is explicitly **out of
scope** — that one only shards work across local in-process actors for concurrency and has
nothing to do with which partition owns the data. Don't confuse the two.

---

## 5. The generation fence: making splits safe for clients

Here's the central correctness mechanism. When a range moves (split or merge), any request that
was routed using the *old* map must not silently apply to the wrong place. The **generation**
field is how we catch that.

```
Client routes a write to key K:
    map says K → partition 6, generation 42
    client sends the write to partition 6, STAMPED with generation 42

Meanwhile, a split happens. The descriptor for K is now generation 43 on partition 7.

Partition 6 receives the write stamped generation 42:
    "My current descriptor is generation 43 (or I don't own K anymore)."
    → REJECT with MustRetry

Client sees MustRetry:
    re-resolves LocateRange → K now → partition 7, generation 43
    retries → lands on partition 7 → applied exactly once.
```

The fence is checked on the leader, just before replicating a key-range write, by
`RangeRouting.TryFenceKeyRange`. The proposal actor re-resolves the *current* descriptor and
rejects with `MustRetry` if there's no covering descriptor or the routed generation no longer
matches.

**Important architectural note:** this fence is a **Kahuna-level** mechanism built on the
descriptor map's `Generation` field. It is **not** Kommander's `expectedGeneration` parameter.
Kommander does have a partition-generation concept, but it has no API to bump it at a key-range
cutover (it only bumps it inside its own hash-split/merge coordinator). So Kahuna runs its own
fence entirely. Don't try to wire the descriptor generation into Kommander's `ReplicateLogs`
generation — they are decoupled on purpose.

The routed generation threads through the request → proposal pipeline. On a fenced rejection,
the handler resolves the client's promise as `MustRetry` (via a `FenceRetry` flag) and the
client re-resolves and retries. The batch (`TrySetMany`) path threads a per-item
`RoutedGeneration` the same way — the single-key path had it first, and the batch path threads it
too so a batched multi-key set during a split can't slip through unfenced.

---

## 6. The split lifecycle (the hard part)

Splitting range `R = [S, E)` on partition `P` at split key `K` (where `S < K < E`) into
`[S, K) @ P` and `[K, E) @ P'`. Driven by `RangeSplitter.cs`.

```
   BEFORE:        [ S ─────────────── E )  @ P
                         split at K

   AFTER:         [ S ──── K )  @ P      [ K ──── E )  @ P'
                  (old gen)               (new partition, new gen)
```

Step by step:

1. **Choose K.** Size-based (range exceeds a target key count) or load-based. K is sampled —
   for an even split, the median key of the range. There's a special case for
   **monotonic-append** workloads (sequential inserts at the top, like time-ordered IDs): split
   the **hot tail** rather than the median, because the median split would immediately need to
   split again. A `min-range-size` guard prevents splitting into a tiny/empty half (avoids
   thrashing). See `RangeSplitPolicy.cs`.

2. **Create the target partition P'.** This needs `IRaft.CreatePartitionAsync`. **Gotcha:**
   that call requires the caller to be the **system-partition (0) leader**, while the cutover
   (step 5) requires the **meta-partition (1) leader** — and those are often *different nodes*.
   So `SplitAsync` takes a **pre-created** `newPartitionId`: the caller creates P' on the P0
   leader first, then calls `SplitAsync` on the P1 leader. The test helper `SplitViaLeaders`
   shows this two-step dance. In production, the auto-splitter needs one node leading both P0
   and P1 (see §8).

3. **Transfer the data** for `[K, E)` from P to P'. Done in two passes by
   `KvStateMachineTransfer.cs`: a **bulk export** at a fixed MVCC snapshot timestamp, then a
   **catch-up export** under the quiesce lock that captures any writes in between. Import is
   idempotent (re-importing the same keys overwrites harmlessly), so the overlap is fine.
   The wire format is length-delimited `RangeSnapshotPage`s (256 entries each) with an FNV-1a
   checksum verified on import. Import buffers the whole stream and applies only after a clean
   checksum, so a crash mid-read is a no-op.

4. **Quiesce** the range during the catch-up→cutover window so no write is lost. Two layers:
   - The **exclusive range lock** blocks concurrent **2PC** commits on `[K, E)`.
   - `RangeQuiesceStore` blocks **direct (non-2PC)** writes: the splitter marks
     `[K, E)` quiesced right after taking the range lock; the locator checks `IsQuiesced`
     pre-route and bounces direct writes to a quiescing range with `MustRetry`.

5. **Atomic cutover.** One `MutateAsync` meta-transaction on partition 1: bump generations,
   write the two new descriptors (`[S,K)@P` and `[K,E)@P'`), remove the old one. Because
   `MutateAsync` is the single serialized writer, overlap is impossible. After commit, routing
   sends `[K, E)` traffic to P'. In-flight requests against P for `[K, E)` fail the generation
   fence and retry onto P'. The quiesce is released in the cutover `finally`.

### Two things to know about the current implementation

- **The persistence backend is node-global, keyed by the full key string** — *not* physically
  partitioned. This is the single most important thing to understand about the current state.
  It means the "transfer" in step 3 is largely **vestigial**: the data is already present on
  every node (it arrived via Raft replication of the original writes). "Import into P'" just
  means writing the keys into the same shared local store. Splits today move **routing**, not
  physical bytes.

- Because of that, after cutover the source `P` does **not** delete `[K, E)` — it **orphan-
  retains** those rows. They become unreachable through routing (everything for `[K,E)` now
  resolves to P') but they're not deleted, because a real delete from the shared store would
  also remove P''s copy. Orphan retention is harmless-but-wasteful, and it's also what makes
  certain mid-scan-split cases read correctly (a stale descriptor query still finds the data).

These two facts are why **partition-scoped storage** (see §10) is the load-bearing piece of future
work.

---

## 7. Merge, scans, buckets, and range locks

With split in place, the rest of the system is about *using* and *maintaining* the ranges.

### Merge

`RangeMerger.cs` is the inverse of split: when two adjacent ranges `[A,B)@P1` and `[B,C)@P2` are
both under the minimum size, coalesce them. It bulk-exports `[B,C)` to the survivor P1, does an
atomic `MutateAsync` cutover replacing the two descriptors with one `[A,C)@P1` at a bumped
generation, and returns the retired partition id so the caller can `RemovePartitionAsync` it.
The generation fence covers stale routing exactly as in split.

### Multi-range ordered scans (`LocateAndGetByRange`)

Once a key space is split, an ordered scan of `[startKey, endKey)` may cross several ranges. The
locator resolves the **ordered set of descriptors** the scan spans, pages within each, and
**stitches** results in key order. The cursor (`KeyValueRangeCursor`) encodes
`(rangeGeneration, lastKey)`: if a split happens mid-scan, the generation miss triggers a
re-resolve and the scan resumes from `lastKey`. Because the cursor is **key-based**, it
survives splits naturally — the boundaries are keys, not offsets.

### Multi-range bucket reads (`LocateAndGetByBucket`)

A "bucket" is a prefix scan (everything starting with `P`). Treat the prefix as the key
interval `[P, P⁺)` where `P⁺` is P's ordinal upper bound, then:

- Ask the map for **only** the descriptors that *intersect* `[P, P⁺)` — not a blind broadcast
  to all nodes, not all ranges.
- **Single-range fast path:** if the prefix lives in one range, short-circuit to the old
  single-leader path — no coordination cost when there's nothing to coordinate.
- Otherwise fan out **in parallel** (bounded by a `SemaphoreSlim(8)`), each partition clipping
  its scan to `[max(StartKey, P), min(EndKey, P⁺))`, and merge the key-sorted streams.

Hash-mode and `{db}/meta` buckets keep the simple single-leader path untouched — they don't
split, so there's nothing to union.

> Note: same-leader **coalescing** (grouping descriptors on one leader into a single RPC) is
> *not* implemented at the locator level — it fires one paged query per descriptor in parallel
> and leaves any same-leader batching to the gRPC transport layer. If same-leader fan-out ever
> shows up as a bottleneck, that's the place to optimize.

### Per-range locks (the core payoff)

This is *why* the whole feature exists. `LocateAndTryAcquireExclusiveRangeLock` fans out one
sub-lock per intersecting descriptor's partition, with clamped bounds. A lock on `[A,B)` does
**not** block a concurrent operation on the disjoint `[B,C)` — they're on different partitions.
That's the concurrency win hashing could never give.

Implementation details worth knowing:

- **Deterministic acquire order.** Sub-locks are acquired in `StartKey` order so two
  overlapping range-lock requests can never deadlock by grabbing in opposite orders.
- **Rollback on partial failure.** If one sub-lock fails, the already-held ones are released
  (release failures are logged).
- **Acquire-time fence.** After all sub-locks are held, re-read `FindIntersecting`
  and compare to the pre-acquire snapshot via `DescriptorSetStable` (count + per-descriptor
  `(PartitionId, Generation)`). If a split landed in the acquire window, roll back everything
  and return `MustRetry`. *Known limitation:* this compares two reads of the **local** map, so
  it only catches splits this node has already observed; cross-node map skew is backstopped by
  the write-path fence and the split's quiesce lock, not fully closed here.

> **Prefix locks are deprecated on ranged spaces.** The old whole-bucket prefix lock can't
> express a clean boundary once a space is split, so on a key-range space both acquire and
> release return the typed `KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace` (105)
> rather than a generic error — pushing callers to migrate to the range lock deliberately. On
> hash spaces, prefix locks work as before.

---

## 8. Automatic split & merge triggers

Splits and merges don't have to be manual. Two background actors watch the ranges:

- **`RangeSplitCheckerActor` / `RangeSplitTrigger` / `RangeSplitPolicy`** — samples range sizes
  (hooking into the existing `KeyValueCollectSampler`), and when a range crosses the size
  threshold, picks a split key (median, or hot-tail for monotonic-append) and drives the split.
  A clamp guard prevents cascades and below-min splits.

- **`RangeMergeCheckerActor` / `RangeMergeTrigger` / `RangeMerger`** — finds adjacent under-min
  ranges and merges them. The trigger tracks failed `RemovePartitionAsync` calls in a
  `pendingRemovals` set and retries them each tick, so a transient removal failure doesn't
  permanently orphan an empty Raft group.

> **Operational gotcha:** both auto-split and auto-merge require **one node to lead both
> partition 0 and partition 1** simultaneously — because creating/removing a partition needs the
> P0 leader and the cutover needs the P1 leader, and the trigger runs on a single node. In a
> multi-partition cluster this colocation isn't guaranteed. The tests *force* it
> (`ForceLeaderForTestingAsync` on both P0 and P1). Whether to require this colocation in
> production, or to split the orchestration across two nodes with an RPC, is an open design
> decision — flagged, not yet resolved.

---

## 9. End-to-end: the life of a ranged write

Putting it all together, here's a write to a key-range space, start to finish:

```
1. Client calls TrySet("users/0300", value).

2. KeyValueLocator → RangeRouting.Locate:
     KeySpaceRegistry says "users" is KeyRange mode.
     RangeMap.Find("users", "users/0300") → partition 6, generation 42.

3. Locator routes the request to partition 6's LEADER NODE,
   stamping it with routedGeneration = 42.

4. On the leader, KeyValueProposalActor re-derives the partition
   (same RangeRouting.Locate → partition 6) and, just before replicating,
   calls TryFenceKeyRange:
     - current descriptor for "users/0300" is still gen 42 on P6?  → OK, proceed.
     - (if a split had bumped it to 43 or moved it)  → reject MustRetry.

5. The write replicates through partition 6's Raft log, applies to the
   node-global store, and the client promise resolves Set.

   --- meanwhile, a split of range [users/0250, users/0700) at users/0500 ---

6. RangeSplitter, on the node leading both P0 and P1:
     - creates partition 8 (on the P0 leader),
     - quiesces [users/0500, users/0700) (range lock + RangeQuiesceStore),
     - bulk + catch-up exports that slice to P8,
     - MutateAsync cutover on P1: P6 keeps [0250,0500) gen 43,
       P8 gets [0500,0700) gen 43, old descriptor removed,
     - releases the quiesce.

7. A client that still thinks users/0600 → P6 gen 42 sends a write there.
   P6's fence sees gen 42 ≠ 43 (and it no longer owns 0600) → MustRetry.
   Client re-resolves → users/0600 → P8 gen 43 → retries → applied once.
```

---

## 10. Current limitations and where to take this next

The system as it stands delivers **logical range routing + range-scoped locking** — fully
working and tested. What it does **not** yet deliver is *physical* load/space distribution,
because of the node-global store. Be honest about this when reasoning about the system.

The foundational gap and its consequences:

- **The persistence backend is node-global, keyed by key string.** Every node holds every
  key's data (it arrived via Raft). Splitting changes routing, not physical placement. So a
  split spreads *which leader coordinates* a range, but not *where the bytes live* — every node
  still stores everything.

- Because of that, several correctness windows are currently **masked rather than fixed**:
  - The direct-write quiesce (§6 step 4) is best-effort and only catches writes entering on the
    split-executor node — a write entering elsewhere during the window would be lost *if* the
    store were partition-scoped. Today it isn't, so P and P' share the store and the write is
    still readable.
  - The merge late-write window is similarly masked.
  - Orphan retention "works" only because the source's copy is the same physical row P' reads.

### The next major piece: partition-scoped storage

The highest-leverage future work is making the persistence backend **partition-scoped** so a
partition's data is physically distinct and removable. The recommended approach is **composite
keying**: prefix every stored key with its partition id (`{partitionId}\x00{key}`) in the
existing store, so `RemovePartition` becomes a bounded prefix-range delete. (The alternative is
a separate store per partition — cleaner isolation, heavier lifecycle.)

Once storage is partition-scoped, the follow-on work becomes *real* instead of vestigial:

- **Real transfer into P'** — install `[K,E)` into P''s Raft-replicated state across all its
  replicas, not just the executor's local store. Then killing the executor still leaves the data
  readable from another P' replica.
- **Re-enable the source delete** — drop `[K,E)` from P after a *successful* cutover, partition-
  scoped and prefix-bounded, replacing orphan retention.
- **Re-validate the masked windows** — the direct-write and merge quiesce windows become genuine
  loss windows; confirm the quiesce actually closes them.

### Other deferred work

- **Load-based split** — split a *hot* range, not just a large one. Needs a per-range load
  signal (ops/sec, p99). For insert-heavy tables, a "hash-shard a hot prefix" knob.
- **Rebalance** — move a range's *replicas* across nodes for space/load. **Blocked upstream:**
  Kommander only exposes cluster-wide `JoinCluster`; this needs per-group
  `AddServer`/`RemoveServer`. Per Kahuna's rule, that's a Kommander capability to add, not a
  Kahuna workaround.
- **Meta-range sharding** — if partition 1 itself gets hot, shard the descriptor map across
  several meta partitions (CRDB-style meta/meta ranges).
- **Serializability (SSI)** — the per-range locks are the foundation for a serializable
  isolation layer; that's a separate design.

---

## 11. Rules to internalize before you touch this code

A condensed checklist — violating any of these silently corrupts the system:

1. **Ordinal comparisons everywhere.** Any new range bound / split key / lookup compares with
   `StringComparison.Ordinal`. A culture-aware compare is a silent ordering corruption.
2. **No gap / no overlap.** `RangeMap.Validate()` must hold at every observable step. The
   cutover `MutateAsync` is the *only* thing allowed to mutate descriptors.
3. **The fence catches stale routing.** Every stale-routed write returns `MustRetry` and retries
   to the correct partition — never double-applies. The descriptor `Generation` is the fence,
   and it's a Kahuna mechanism, not Kommander's.
4. **Both routing call sites must agree.** `KeyValueLocator` and `KeyValueProposalActor` both go
   through `RangeRouting.Locate`. The local worker-actor hash router is a different thing — leave
   it alone.
5. **Never range-split the schema log.** `{db}/meta` (CamusDB's DDL log) must stay
   single-partition for total ordering — the registry rejects ranging it.
6. **Snapshot before checkpoint** in `RangeMapStore` — reversing it loses the map on restart.
7. **Key-range data lives on partition ≥ 2.** Partition 0 is Kommander-reserved; partition 1 is
   the meta map (plus hash pool). Splits always allocate fresh partitions ≥ 2.
8. **Missing Kommander primitive? Flag it upstream, don't work around it in Kahuna.**

---

## 12. File map

Everything lives under `Kahuna.Core/KeyValues/Ranges/` unless noted.

| File | Responsibility |
|---|---|
| `RangeDescriptor.cs` | The `(KeySpace, StartKey, EndKey, PartitionId, Generation)` record. |
| `RangeMap.cs` | Sorted descriptor set; `Find`, `FindIntersecting`, `Validate`. |
| `RangeMapStore.cs` | Replicated meta map on partition 1; `MutateAsync` (single writer); durable snapshot + checkpoint. |
| `RoutingMode.cs` | The `Hash` / `KeyRange` enum. |
| `KeySpaceRegistry.cs` | Per-key-space mode lookup (prefix before last `/`). |
| `DataPartitionRouter.cs` | Kahuna's hash assignment over user partitions `[1, N]`. |
| `RangeRouting.cs` | The single routing funnel: `Locate`, `IsKeyRange`, `TryFenceKeyRange`. |
| `KvStateMachineTransfer.cs` | `ExportRangeAsync` / `ImportRangeAsync`; paged, checksummed snapshot transfer. |
| `RangeSplitter.cs` | The split transaction: create → transfer → quiesce → atomic cutover. |
| `RangeSplitPolicy.cs` | Split-key selection (median / hot-tail), thresholds, min-size guard. |
| `RangeSplitTrigger.cs`, `RangeSplitCheckerActor.cs` | Auto-split sampling + orchestration. |
| `RangeMerger.cs` | The merge transaction (inverse of split). |
| `RangeMergeTrigger.cs`, `RangeMergeCheckerActor.cs` | Auto-merge candidate-finding + orchestration + `pendingRemovals` retry. |
| `RangeQuiesceStore.cs` | Best-effort direct-write quiesce during a split window. |
| `KeyValueLocator.cs` *(parent dir)* | Request routing: `LocateRange`, multi-range scans/buckets, range locks. |
| `KeyValueProposalActor.cs` *(parent dir)* | The second routing site + the write-path generation fence. |
