# Spec: Key-range splits (Spanner/CockroachDB-style sharding)

> **Status:** proposed / design
> **Layers:** Kommander (partition = Raft group, routing) + Kahuna (KV range descriptors, split
> orchestration, data movement). CamusDB is the downstream consumer.
> **Motivation:** today a single logical key space (e.g. a CamusDB table's rows `{tableId}:r/…`)
> lives entirely in **one** partition. That caps a table at one Raft group's throughput/size and
> makes range-scoped concurrency impossible — an exclusive prefix lock over a table's bucket
> blocks *every* reader and writer of the table (Kahuna `TryGetHandler` returns `MustRetry` to any
> read of a prefix-locked bucket). Splitting a key space into contiguous **key ranges**, each in
> its own partition, removes both limits: load spreads across Raft groups, and a predicate/range
> lock contends only within the one range's partition. This is the foundation that makes
> lock-based **serializability scale** (CRDB's per-range latch+lock model) instead of serializing
> a whole table.

---

## 1. Current state (grounded)

- **Routing is by hash-range, fixed at init.** `RaftManager.GetPartitionKey(key)` computes
  `rangeId = HashUtils.InversePrefixedStaticHash(key, '/')` and returns the partition whose
  `[StartRange, EndRange]` contains `rangeId`. Partitions already own a *contiguous slice of the
  hash space* — but the slices are assigned **statically** from `InitialPartitions` at cluster
  startup (`EmbeddedKahunaNode` creates partitions `1..InitialPartitions`). There is no runtime
  split/merge and no change to the slice boundaries.
- **`GetPrefixPartitionKey(prefix)`** hashes the whole prefix string so an entire `{db}/meta` or
  `{tableId}:r` prefix lands on **one** partition (used for schema-log ordering and the prefix
  lock redirect, `KeyValueLocator:620`).
- **Cross-partition transactions already exist** — Kahuna does 2PC across partitions
  (`LocateAndCommitTransaction` fans out per-key by `GetPartitionKey`). A transaction that spans
  several partitions is already supported machinery.
- **Locks are per-key write-intents + an exclusive prefix (predicate) lock** scoped to a bucket on
  its owning partition.

What today's model gives: good *hash* load distribution. What it does **not** give: key
**locality**. Because routing is `hash(key)`, contiguous keys (`{tableId}:r/0001`,
`…/0002`, …) scatter across partitions. An ordered range scan or a `WHERE x BETWEEN a AND b` can't
be served from one partition, and a "lock this key range" predicate is meaningless over a hash
slice. That is the difference from Spanner/CRDB.

---

## 2. Goal

Shard a logical key space into **contiguous key ranges** (ordered by the raw key, not its hash),
each range owned by its own partition (Raft group), with:

1. **Key-order routing** — a lookup `key → range → partition` over a consistent **range-descriptor
   map**, replacing/augmenting the pure `hash(key)` function for key spaces that opt into ranging.
2. **Dynamic split** — when a range grows past a threshold (size or load), pick a split key, create
   a new Raft group for the upper half, move its data, and atomically cut routing over.
3. **Dynamic merge** — adjacent small ranges coalesce.
4. **Rebalance** — move range replicas across nodes for load/space.
5. **Range-scoped locking** — predicate/prefix locks and per-key intents contend only within a
   range's partition, so transactions on disjoint ranges never block each other.

Non-goal (this spec): the SQL/transaction-isolation layer. Serializability is a *consequence* this
unlocks (range-scoped locks + SSI), specified separately.

---

## 3. The central decision: hash-range vs key-range

Two ways to split, and they are not interchangeable:

| | Hash-range (extend today's model) | **Key-range (this spec / Spanner/CRDB)** |
|---|---|---|
| Split point | bisect the hash slice | a real **key** (`{tableId}:r/{rowId}`) |
| Locality | none — keys scatter | contiguous keys co-locate |
| Ordered range scan | hits all partitions | served by the few ranges it spans |
| Range/predicate lock | meaningless | locks exactly the contended key range |
| Hot-spot risk | low (hash spreads) | sequential inserts hit one range until it splits |

Spanner/CRDB choose **key-range** for locality and range locks, and accept the sequential-insert
hot-range (mitigated by load-based splitting + sharding hot prefixes). This spec adopts
**key-range** for opted-in key spaces. Hash-range routing stays the default for key spaces that
want spread without locality (and for the reserved/system partitions).

> **Per-key-space opt-in.** Routing mode is a property of a key space (prefix), not global:
> CamusDB row/index data (`{tableId}:r/…`, `{tableId}:i:…`) opts into **key-range**; the schema
> log (`{db}/meta`) stays **single-partition** (see §8). The router picks the mode from the key's
> registered descriptor.

---

## 4. Range descriptors + routing

Introduce a **range-descriptor table** — the source of truth for `key → partition`:

```
RangeDescriptor {
    string  KeySpace;        // the prefix this range belongs to, e.g. "{tableId}:r"
    string  StartKey;        // inclusive, ordinal-ordered raw key  (null = -inf within KeySpace)
    string  EndKey;          // exclusive                            (null = +inf within KeySpace)
    int     PartitionId;     // the Raft group currently serving [StartKey, EndKey)
    long    Generation;      // bumped on every split/merge/move — fences stale routing
}
```

- **Ordering is ordinal** everywhere (`StringComparison.Ordinal` / `CompareOrdinal`) — consistent
  with the rest of Kahuna and the documented culture-vs-ordinal hazard. Split keys, range bounds,
  and lookups all compare ordinally.
- **The map itself must be consistent and replicated.** Like CRDB's meta ranges, store descriptors
  in a system key space that is itself Raft-replicated (start with a single well-known
  "meta" partition; shard the meta later if it grows). A node resolves a key by an ordinal
  range lookup over the cached descriptors, refreshing on a `Generation` miss.
- **Routing:** `Locate(key)` → find the descriptor whose `[StartKey, EndKey)` (within the key's
  KeySpace) contains `key` → its `PartitionId`. Callers carry the descriptor `Generation` they
  routed on; the owning partition rejects a request whose generation is stale (the range moved),
  triggering a client refresh + retry. This is the routing-fence that makes split atomic to clients.

`GetPartitionKey` stays for hash-routed/system key spaces; a new `LocateRange(key)` serves
key-ranged spaces.

---

## 5. Split lifecycle (the hard part)

Splitting range `R = [S, E)` on partition `P` at split key `K` (S < K < E):

1. **Choose `K`** — size-based (R exceeds a target bytes/row count) or load-based (hot range). `K`
   must be an existing-or-virtual key boundary; for even splits, sample R's keys and pick the
   median. Never split below a min-range-size (avoids thrash).
2. **Create the target Raft group `P'`** for `[K, E)` — *requires the Kommander dynamic-partition
   primitive* (§7). `P'` starts empty with its own replica set (initially co-located with `P`'s
   replicas, rebalanced later).
3. **Snapshot + transfer** `[K, E)` from `P` to `P'`. Two options:
   - **learner/snapshot**: `P'` ingests a consistent snapshot of `[K, E)` at a fixed HLC, then
     catches up the tail — mirrors how a follower bootstraps.
   - **copy-then-verify**: bounded `GetByRange` pages (the streaming primitive) from `P` into `P'`.
4. **Atomic cutover** — in one replicated meta transaction: bump generations, write
   `R1 = [S, K) @ P` and `R2 = [K, E) @ P'`, remove `R`. After commit, routing sends `[K, E)`
   traffic to `P'`. Requests still in flight against `P` for `[K, E)` fail the generation fence and
   retry → land on `P'`. `P` drops `[K, E)` data after cutover.
5. **Quiesce window** — between snapshot and cutover, either (a) briefly latch `[K, E)` on `P` to
   freeze writes during the final catch-up (short, range-scoped — not table-wide), or (b) keep
   `P` authoritative until `P'` is fully caught up, then flip. CRDB uses a short split-trigger
   transaction; adopt the same: the split is itself a transaction, so it's atomic or it doesn't
   happen.

**Invariant:** a key is owned by exactly one range at every instant (no gap, no overlap). The
cutover transaction is the only thing that mutates descriptors, so overlap is impossible if it's
serialized on the meta range.

---

## 6. Merge & rebalance (sketch)

- **Merge** adjacent ranges `[A,B) @ P1` + `[B,C) @ P2` when both are under-min: move `[B,C)` to
  `P1` (or a chosen survivor), atomic descriptor swap, retire the empty group. Same generation
  fence.
- **Rebalance** moves a range's *replicas* (not its key bounds) across nodes for space/load — pure
  Raft membership change on that group, no descriptor key-bound change.

---

## 7. Kommander dependencies

This spec needs Kommander to expose, beyond today's static partitions:

1. **Dynamic partition (Raft-group) lifecycle** — create and destroy a partition/group at runtime
   with a chosen replica set: `CreatePartitionAsync(partitionId, members)` /
   `RemovePartitionAsync(partitionId)`. Today partitions are created `1..InitialPartitions` at
   startup only.
2. **Per-group membership change** (likely already present for elections) — used by rebalance.
3. **A consistent system/meta partition** to host the range-descriptor map (can reuse a reserved
   partition initially).
4. Keep `GetPartitionKey` for hash/system routing; key-range routing is a Kahuna-level lookup over
   descriptors, so Kommander does **not** need to learn about key ranges — it just needs to host
   more groups on demand. (This keeps the Kommander surface small: it provides groups; Kahuna maps
   keys to groups.)

Per the upstream-fix rule: these are Kommander capabilities to add, not Kahuna workarounds. The
dynamic-partition lifecycle is the single gating primitive — everything else is Kahuna-level.

> **Status (Kommander 0.10.14 — shipped).** The gating primitive (§7.1) and more is now exposed:
> `CreatePartitionAsync` / `RemovePartitionAsync` (leader-only on the system partition, idempotent),
> a consistent system/meta partition (`RaftSystemConfig.SystemPartition`, fenced from split/merge),
> `GetPartitionKey` retained, and a **generation fence** for §4 — `ReplicateLogs(…,
> expectedGeneration)` rejects a stale-generation write and `GetPartitionGeneration(partitionId)`
> reads the committed generation. Kommander additionally ships a two-phase `SplitPartitionAsync` /
> `MergePartitionsAsync` coordinator and an `IRaftStateMachineTransfer` (`ExportRange`/`ImportRange`)
> data-movement hook.
>
> **The shipped `SplitPartitionAsync` is the *hash-range* flavor, not key-range.** `RaftSplitPlan`
> carries only a `HashBoundary` (an `int` hash-slice midpoint) and `RaftRoutingMode` is
> `{HashRange, Unrouted}` — there is **no `KeyRange` mode and no raw-key split boundary** (by design;
> per §7.4 Kommander deliberately doesn't learn about key ranges). So the key-range split of §3/§5
> is **not** a call to `SplitPartitionAsync`: Kahuna orchestrates it at its own level —
> `CreatePartitionAsync(target, Unrouted)` → move `[K, E)` via Kahuna's descriptor map + data copy →
> atomic descriptor cutover → enforce with `expectedGeneration`. Kommander provides the *group* and
> the *fence*; the key-range semantics live in Kahuna. (Reserve `SplitPartitionAsync` for any
> hash-routed key space that wants Kommander-native hash bisection.)
>
> Not yet present: per-group membership change for **rebalance** (§7.2/§6) — only cluster-wide
> `JoinCluster()` exists today. Not blocking phases 2–5.

---

## 8. Consumer contract (CamusDB) — what must hold

- **Schema log stays single-partition.** `{db}/meta` keeps `GetPrefixPartitionKey` routing (one
  partition, total DDL order). The two-version invariant, ack gate, and `OnLogRestored` replay all
  depend on total ordering of the schema log — **never** range-split the schema log. Only the
  *row/index data* key spaces (`{tableId}:r/…`, `{tableId}:i:{indexId}/…`) opt into key-range
  sharding.
- **Ordered scans become multi-range.** `KvTableStore.ScanRows`/`ScanIndex` (already cursor-paged
  over `LocateAndScanRange`) must iterate the **ordered set of ranges** the scan spans, page within
  each, and stitch — instead of assuming one partition. Cursor continuation must encode
  `(rangeGeneration, lastKey)` so a split mid-scan resumes correctly (re-resolve the range on a
  generation miss; the key-based cursor already survives splits because bounds are by key).
- **Range locks replace table locks.** `AcquireRowRangeLockAsync` becomes per-range: lock the
  ranges a serializable scan touches, not the whole table — concurrency for disjoint ranges.
- **`gen_id()` rowIds are time-ordered**, so sequential inserts append to the top range → that
  range is the split/hot-spot candidate. Load-based splitting must handle the monotonic-append
  pattern (split the hot tail, or hash-shard a prefix for insert-heavy tables — a future knob).

---

## 9. Phasing

1. **Kommander: dynamic partition lifecycle** (§7.1) + tests (create/destroy group, host data,
   leave cluster). Gating prerequisite; nothing else starts without it.
2. **Kahuna: range-descriptor map + `LocateRange` routing + generation fence** behind a
   per-key-space flag, defaulting OFF (everything still hash-routed). Additive.
3. **Kahuna: split transaction** (snapshot → create group → transfer → atomic cutover) for one
   key space, manually triggered. Prove atomicity + the no-gap/no-overlap invariant under
   concurrent reads/writes.
4. **Kahuna: automatic split trigger** (size, then load) + merge.
5. **CamusDB: multi-range `ScanRows`/`ScanIndex`** + per-range locks; opt `{tableId}:r`/`:i:` into
   key-range routing; keep `{db}/meta` single-partition.
6. **Rebalance** + the SQL/serializability layer on top (separate spec).

Items 2–3 are the load-bearing risk; everything after is incremental.

---

## 10. Open questions / risks

- **Split atomicity vs availability.** The quiesce window (§5.5) trades a brief range-scoped write
  latch for a clean cutover. Confirm it can be kept short (CRDB's is ~ms) and never table-wide.
- **Meta-range scaling & caching.** Descriptor lookups are on the hot path; clients cache and
  refresh on generation miss. Decide the meta-range replication/caching story before step 2.
- **Transaction across a mid-flight split.** A 2PC touching `[K,E)` exactly as it moves from `P`
  to `P'` must fail-and-retry via the generation fence, not double-apply. Test explicitly.
- **Hash-routed vs key-routed coexistence.** Both routers run; a key space must be unambiguously
  one mode. The descriptor's presence (or a registered key-space flag) decides; define the
  registration path.
- **Index vs row split independence.** A table's `:r` and each `:i:{indexId}` are distinct key
  spaces and split independently — good for locality, but a transaction touching a row and its
  index entries now spans more ranges (more 2PC participants). Measure the 2PC fan-out cost.
