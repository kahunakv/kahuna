# Completion plan: Key-range splits — remaining work

> **Companion to** [`spec-range-splits.md`](spec-range-splits.md) (design) and
> [`spec-range-splits-tasks.md`](spec-range-splits-tasks.md) (the build-out, Phases A–D — **done**).
> This document scopes everything still missing, task by task, in the same format: each task states
> its dependency, the real files it touches, the approach, and a concrete **Done when** (named
> tests). Do tasks in order within a phase unless a task says otherwise.

---

## 0. Status & conventions

**Phases A–D are complete.** Tasks 1–11 (+ 2c, 10b) are implemented, tested, reviewed: register a
key space → key-range routing + generation fence → split (auto, size-based) → multi-range
scans/buckets → range-scoped locks → auto-merge. Locks/KV/sequencer are regression-clean (102/102).

**What remains, in three buckets:**
- **Phase F — Hardening:** close the correctness/perf holes left open inside the "done" tasks.
  F1–F8 are done; **Phase F is complete**.
- **Phase G — Foundation:** partition-scoped storage + real cross-node data movement. This is the
  load-bearing piece that turns *logical range routing* into actual *load/space distribution* — and
  several Phase-F holes are only "masked" today because it's absent.
- **Phase H — Deferred features:** the originally-Phase-E work (load-based split, rebalance,
  meta-range sharding, SSI).

**Conventions (carry over from the build-out spec):** ordinal compares everywhere; no backwards
compat; flag missing Kommander primitives upstream rather than working around them; build
`dotnet build Kahuna.sln -c Debug`; tests under `Kahuna.Tests/Server/`, run **one `dotnet test` at a
time** (per `AGENTS.md`), scoped to `Kahuna.Tests.Server.*`.

**The G-before-most-of-F caveat:** F3 (direct-write quiesce) and parts of F (merge/split windows)
are only *real* once storage is partition-scoped (Phase G). They are listed in F because they're
small and worth doing first as guards, but their **acceptance tests can only be meaningful after
G**. Each notes this.

**Invariants that must continue to hold (from the build-out plan's G1–G5):** no gap/no overlap;
every stale route → `MustRetry` not double-apply; quiesce stays range-scoped; `{db}/meta` never
range-split; ordinal-only.

---

## Phase F — Harden the existing split machinery

### Task F1 — Batch multi-set cross-node generation fence  ✅ **done**
**Depends on:** none (Task 9 single-set fence already done). **Files:**
`KahunaSetKeyValueRequestItem` (+ its gRPC proto), `KeyValueLocator` (many-set grouping),
`Communication/InterNode/MemoryInterNodeCommmunication.cs` (`TrySetManyNodeKeyValue`, ~`:188`),
`Communication/InterNode/Grpc/GrpcInterNodeCommunication.cs` (~`:1000`); tests
`TestGenerationFence.cs`.
**Why:** the single-key `TrySetKeyValue` threads `routedGeneration` across nodes (Task 9), but the
batch `TrySetMany` path does **not** — `KahunaSetKeyValueRequestItem` has no `RoutedGeneration`, so a
batched multi-key set of ranged keys redirected across nodes during a split is **unfenced** (can
apply stale-routed).
- Add a per-item `RoutedGeneration` to `KahunaSetKeyValueRequestItem` and its proto.
- In the locator's many-set grouping, resolve each ranged item via `LocateRange` and populate its
  `RoutedGeneration` (hash items stay 0).
- Thread it through both batch transports to the remote `TrySetKeyValue`, exactly like the
  single-key fix.
**Done when** (`dotnet test --filter TestGenerationFence` green): ✅
- [x] `BatchSet_StaleGeneration_ItemFenced` — a batched set whose ranged item carries a pre-split
  generation is rejected with `MustRetry` for that item, applied for the rest.
- [x] `BatchSet_CrossNode_FencedLikeSingleKey` — a cross-node batch redirect carries each item's
  routed generation; behaviour matches the single-key path.
- [x] `BatchSet_ZeroGeneration_LocatorStampsCurrent` — an item with the default `RoutedGeneration = 0`
  is stamped by the locator with the live descriptor generation and applied (populate-when-0 path).

### Task F2 — Confirm + lock down the range-lock acquire-time fence  ✅ **done**
**Files:** `KeyValueLocator.cs` (`LocateAndTryAcquireExclusiveRangeLock` + `DescriptorSetStable`
+ `afterSnapshot` test seam, ~`:834`); tests `TestLocks.cs`.
**Resolution:** a **check-after-acquire** fence — after sub-locks are held, re-read `FindIntersecting`
and compare to the pre-acquire snapshot via `DescriptorSetStable` (count + per-descriptor
`(PartitionId, Generation)`; robust because every split/merge bumps the generation). On a mismatch it
**rolls back all sub-locks** (failures logged) and returns `MustRetry`. Applied to the single- and
multi-descriptor branches; the hash/0-descriptor path skips it (hash spaces don't split). A clean
`afterSnapshot` callback on an `internal` overload injects a split into the exact window for the
test. Also absorbed the Task 11 minor (rollback-release failures are now logged).
> **Known limitation (documented, not closed here):** the fence compares two reads of the **local**
> `RangeMapStore.Current`, so it only detects splits this node has **already observed**. Under
> cross-node descriptor-map skew (lock-holder behind, writer ahead), a not-yet-replicated split is
> not caught — the write-path generation fence remains the primary serializability guard, and the
> split's quiesce lock backstops the in-progress window. Fully closing the skew window (lock carries
> a generation verified against the writer's view) is out of F2 scope.
**Done when** (`dotnet test --filter TestLocks` green): ✅
- [x] `RangeLock_SplitDuringAcquire_FencesAndRetries` — a split injected between `FindIntersecting`
  and sub-lock acquisition makes the whole acquire return `MustRetry` with **no** sub-lock left held
  (proven by a follow-up acquire succeeding).
- [x] existing `RangeLock_DisjointRange_NotBlocked` / `SameRange_StillContends` /
  `ScopedToTouchedRanges` stay green.

### Task F3 — Direct (non-2PC) write quiesce during split  ✅ **done (best-effort; cross-node closure deferred to G)**
**Files:** `Ranges/RangeQuiesceStore.cs` (new), `KeyValueLocator.cs` (pre-route `IsQuiesced` check,
`:124`), `Ranges/RangeSplitter.cs` (`Quiesce`/`Release` around the catch-up→cutover window, `:207`/
`:297`), `KeyValuesManager.cs` (wiring); tests `TestRangeSplit.cs`.
**Why:** Task 6 §5.5 — the split's range lock only quiesces **2PC** commits. A direct `TrySet` to
`[K,E)` between the catch-up export and cutover commits on `P`, is absent from the catch-up
snapshot, and after cutover routes to `P'`. Today this is **masked** by the node-global store
(`P'` reads the same store); once storage is partition-scoped (G) it is a **lost write**.
**Resolution:** a per-node `RangeQuiesceStore` (O(1) empty fast path; range-scoped `IsQuiesced`).
The splitter marks `[K,E)` quiesced right after the range lock and clears it in the cutover `finally`;
the locator checks it **pre-route** and bounces a direct write to a quiescing range with `MustRetry`.
> **Best-effort / known gap (documented in `RangeSplitter` + `RangeQuiesceStore` docs):** the check is
> locator-pre-route, so it only catches writes that **enter on the split-executor node**. A write
> entering on another node bypasses it (lost only post-G). Fully closing the cross-node window needs
> the quiesce state replicated to the data-partition proposal actor — **deferred to Phase G**.
**Done when** (`dotnet test --filter TestRangeSplit` green): ✅
- [x] `Split_DirectWriteDuringQuiesce_MustRetry` — a direct `TrySet` to `[K,E)` during the quiesce
  window (on the split node) returns `MustRetry`; after cutover the same write succeeds (`Set`).

### Task F4 — Resolve the prefix-lock-on-split-space gap  ✅ **done — Option B (deprecation)**
**Files:** `Kahuna.Shared/KeyValue/KeyValueResponseType.cs` (new typed value),
`Kahuna.Shared/Communication/Grpc/Protos/keyvalues.proto` (wire enum),
`KeyValueLocator.cs` (`LocateAndTryAcquireExclusivePrefixLock` ~`:649`,
`LocateAndTryReleaseExclusivePrefixLock` ~`:788`); tests `TestLocks.cs`.
**Resolution (Option B — deprecate, the recommended path):** prefix locks stay single-partition; on
a key-range-split space both acquire and release now return the **typed**
`KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace = 105` (wire:
`TYPE_PREFIX_LOCK_UNSUPPORTED_ON_RANGED_SPACE`) instead of a generic `Errored`, so callers migrate
to the range lock deliberately rather than treating it as transient. The range lock (Task 11) is the
designed replacement (design §8). *(Per-range fan-out into the prefix lock — Option A — was declined
as redundant surface.)*
**Done when** (`dotnet test --filter TestLocks` green): ✅
- [x] `PrefixLock_OnSplitSpace_ReturnsTypedUnsupported` — acquire **and** release on a split space
  return the typed response; the range lock succeeds on the same space (the replacement).

### Task F5 — Multi-range `GetByBucket`: parallel fan-out + mid-scan test  ✅ **done (coalescing deferred)**
**Files:** `KeyValueLocator.cs` (`LocateAndGetByBucket` + `FetchDescriptorSlotAsync`, `:1499`–`:1623`,
`MaxParallelBucketDescriptors = 8`, `beforeQuery`/`afterDescriptor` test seam); tests
`TestGetByBucketMultiRange.cs`.
**Why:** Task 10b shipped the correct-but-simple sequential version (one RPC per descriptor).
**Resolution:** bounded-parallel fan-out via `Task.WhenAll` over a `SemaphoreSlim(8)`, lock-free
by-index result slots, ordered concatenation (no redundant sort), `MustRetry`/`WaitingForReplication`
short-circuit. The deferred mid-scan-split test is added via the `afterDescriptor` seam (orphan
retention keeps the stale descriptor answerable). Key-visibility wait added before the setup split so
the parallelism test isn't flaky.
> **Leader-coalescing — deferred (not implemented).** The locator does **not** group descriptors by
> leader into one RPC; it fires one paged query per descriptor (in parallel). Any same-leader
> coalescing is left to the gRPC `GrpcServerBatcher` at the transport layer and is **not verified**
> (the in-memory test transport doesn't batch). The fan-out test asserts a *no-duplicate-work* bound
> (≤ one remote RPC per descriptor), not coalescing. Revisit if same-leader fan-out shows up as a
> bottleneck.
**Done when** (`dotnet test --filter TestGetByBucketMultiRange` green): ✅
- [x] `Bucket_FanOut_IsParallel` — gate-TCS proof that all descriptor tasks are simultaneously
  in-flight (sequential ⇒ deadlock ⇒ timeout); + the no-duplicate-RPC bound.
- [x] `Bucket_SplitMidScan_NoDupNoMissing` — a second split injected after the first descriptor's
  pages; the result equals the pre-split baseline (no dup / no missing).

### Task F6 — Cleanups (batch, low risk)  ✅ **done**
**Depends on:** none. **Files:** `KeyValueLocator.cs` (`ClipRange`), `RangeMergeTrigger.cs`
(`pendingRemovals`), tests `TestAutoMerge.cs`.
- [x] Consolidated `ClampRange` + `ClipRange` into a single `ClipRange` helper (`KeyValueLocator.cs:1767`);
  locks (`:916`/`:1022`) and scans/bucket (`:1596`/`:1700`) now share it. Lock + scan suites stay
  green — no edge-flag regression.
- [x] Added `TestAutoMerge` (`UnderMin_TriggersExactlyOneMerge`, `AboveMin_NoMerge`). Both
  **deterministically force** system(0)+meta(1) leadership onto one node via
  `ForceLeaderForTestingAsync` (`Assert.Fail` if not achieved — no silent skip), so the
  `RangeMergeTrigger` orchestration (find candidates → `MergeAsync` → `RemovePartitionAsync`)
  actually runs. 2/2 green.
- [x] `RangeMergeTrigger` tracks failed removals in a `pendingRemovals` set (single-threaded actor,
  no locking) and retries them at the top of each tick (`:71-89`), so a transient
  `RemovePartitionAsync` failure no longer permanently orphans an empty Raft group.
> **Open (low priority, tracked):** the `pendingRemovals` retry path has **no direct test** (needs a
> `RemovePartitionAsync` fault hook). Separately, auto-merge **and** auto-split require **one node to
> lead both P0 and P1** to fire — not guaranteed in a multi-partition cluster (the test now *forces*
> it). That's a Task 7/8 design decision to confirm, not an F6 bug.

### Task F7 — Auto-seed the initial descriptor on registration  ✅ **done (in tree; not yet published)**
**Depends on:** Task 9 (registration), Task 2 (`RangeMapStore`). **Files:** `IKahuna.cs`
(`RegisterKeyRangeAsync`), `KahunaManager.cs` (delegate, `:1174`), `KeyValues/KeyValuesManager.cs`
(`RegisterKeyRangeAsync` / `EnsureKeyRangeSeededAsync` / `SeedPartitionFor`); tests
`Kahuna.Tests/Server/TestLocateRange.cs`.
**Why:** Task 9 left a production hole (its notes, `spec-range-splits-tasks.md:401`): registering a
space as KeyRange flips the routing **mode** but **nothing seeds an initial descriptor**, so the
first routed key throws `KahunaServerException: No range descriptor covers key …`. The only seeding
path was the **internal** `RangeMapStore.MutateAsync`, reachable from Kahuna's own tests but not from
an embedding consumer (CamusDB). Registration had to become self-sufficient.
**Resolution:** new public `IKahuna.RegisterKeyRangeAsync(keySpace, ct)`. It flips the mode (node-local,
unchanged `RegisterKeyRange`) **and** runs `EnsureKeyRangeSeededAsync`: idempotent — `FindAll`
fast-path, then `AmILeader(MetaPartitionId)` gate, then `MutateAsync` appends the trivial whole-space
descriptor `[null,null) → SeedPartitionFor(keySpace)` at generation 1. `SeedPartitionFor` hashes the
key space (`HashUtils.SimpleHash`) over the data pool `[FirstDataPartitionId(2), InitialPartitions]`,
spreading different spaces' initial ranges instead of piling all on P2. The seed needs **no**
key-distribution or PK-type knowledge — real boundaries come later from the auto-splitter sampling live
data. **Graceful degradation:** when `InitialPartitions < 2` the whole call is a no-op (the space stays
hash-routed — with one partition there is nothing to shard, and range locks transparently use the
single-partition hash path, `KeyValueLocator.LocateAndTryAcquireExclusiveRangeLock` `descriptors.Count
== 0`). This keeps the opt-in safe to enable on small/test clusters instead of throwing.
> **Known limitation → Task F8.** The seed `MutateAsync` is **leader-only**, so it commits only when
> the registering node *is* the meta-partition (P1) leader. Single-node / colocated-leader deployments
> always satisfy this; a true multi-node cluster does not (a table opened on a non-P1-leader node flips
> that node's mode but does not seed). Forwarding the seed to the P1 leader is **F8**.
**Done when** (`dotnet test --filter TestLocateRange` green): ✅
- [x] `RegisterKeyRangeAsync_AutoSeedsInitialDescriptor` — after registration alone (no manual
  `MutateAsync`), `LocateRange("t:r/0001")` resolves to a data partition (≥ 2) at generation ≥ 1; a
  second call commits nothing new (idempotent).
- [x] `RegisterKeyRangeAsync_BelowMinPartitions_IsNoOp` — with `InitialPartitions = 1`, registration
  returns false, does not throw, and the space stays hash-routed (`LocateRange` generation 0).

### Task F8 — Forward the seed to the meta-partition leader  ⬜ **not started — multi-node blocker**
**Depends on:** F7. **Files:** `Communication/InterNode/IInterNodeCommunication.cs` (+ its gRPC proto,
`GrpcInterNodeCommunication.cs`, `MemoryInterNodeCommmunication.cs`), `KeyValues/KeyValuesManager.cs`
(`EnsureKeyRangeSeededAsync` non-leader branch), leader-endpoint resolution (the mechanism the locator
already uses to find a partition leader); tests `TestGenerationFence.cs` or new
`TestRegisterKeyRangeForwarding.cs` (**multi-node**).
**Why:** F7's seed is leader-only. In a multi-node cluster the node that registers a space (e.g. the
node a client query landed on) is usually **not** the P1 leader, so the seed is skipped and the space
stays registered-but-unseeded → first write throws `No range descriptor covers key`. F7's mode flip is
correctly per-node; only the *seed* needs the leader. This is the seeding analog of the auto-split
dual-leader caveat (F6 / Task 7/8) — but unlike rebalance/auto-split it is **fully solvable** here,
because the seed has one clear target: whoever leads P1.
**Approach:** when `EnsureKeyRangeSeededAsync` runs on a non-leader, resolve the current P1-leader
endpoint and **forward** an `EnsureKeyRangeSeeded(node, keySpace, ct)` RPC (new
`IInterNodeCommunication` method + proto + both transports, mirroring the leader-routed `TrySetKeyValue`
/ range-lock RPCs). The leader runs the existing local `EnsureKeyRangeSeededAsync`. Keep idempotency
and the `< 2 partitions` no-op. Optionally wait for the descriptor to replicate back to the calling
node before returning (the `WaitUntil(… Find(…).Generation …)` pattern in `TestGenerationFence.Setup`),
so a follow-on write on this node sees it.
**Done when** (multi-node test green):
- [ ] `RegisterKeyRangeAsync_FromNonLeader_SeedsViaLeader` — on a 3-node cluster, register a space from
  a node that is **not** the P1 leader; a covering descriptor ends up on **every** node's map and a
  subsequent write/route to the space succeeds cluster-wide (no `No range descriptor covers key`).
- [ ] existing `TestLocateRange` F7 cases stay green (single-node path unchanged).

> **CamusDB consumer note.** CamusDB drives F7 from `TableOpener.LoadTable`
> (`await …RegisterKeyRangeAsync(rowKeySpace)`) and tracks its own remaining adoption work
> (multi-node data-path test, index spaces, repoint off the temporary ProjectReference, republish as
> 0.3.3) in `camusdb/docs/key-range-sharding-spec.md`. **F8 is the gating item** for enabling
> key-range on a multi-node CamusDB cluster.

---

## Phase G — Foundation: partition-scoped storage + real data movement

> **This is the load-bearing phase.** Today the persistence backend is a single **node-global store
> keyed by key string**, so splits/merges move *routing*, not *physical data*. Consequences proven
> across reviews: the Task-5/6 export/import is near-vestigial (data already on every node via Raft);
> orphan-retention "works" and direct-write/merge losses are "masked" only because `P` and `P'` share
> one store; sampling/`GetByBucket` read the local node and get away with it. Until this phase lands,
> the feature delivers **range-scoped routing + locking** but **not** the throughput/space
> distribution that motivates sharding.

> **Decision (storage keying).** Two shapes — pick one before G1:
> - **(A) Composite key:** prefix every stored key with its partition id, `{partitionId}\x00{key}`,
>   in the existing single store. Smallest change; `RemovePartition` = range-delete a prefix.
> - **(B) Per-partition store:** a separate backend instance (DB file / RocksDB column family) per
>   partition. Cleaner isolation; heavier lifecycle.
> Recommendation: **(A)** for the first cut — least churn across the 3 backends, and a split's drop
> becomes a bounded prefix-range delete.

### Task G1 — Partition-scoped persistence backend
**Depends on:** the keying decision. **Files:** `Persistence/Backend/IPersistenceBackend.cs` and all
three impls (`Memory`/`Sqlite`/`RocksDb`PersistenceBackend.cs), `KeyValueRestorer.cs`,
`KeyValueReplicator.cs`, `KeyValueActor.cs` read-through; tests `TestPartitionScopedStore.cs`.
**Goal:** a key's physical storage is scoped to its owning partition, so a partition's data is
distinct and removable.
- Thread `partitionId` into `StoreKeyValues`/`GetKeyValue`/`GetKeyValueByRange`/`GetKeyValueByPrefix`
  (and locks if applicable), keying per option A/B.
- `OnReplicationReceived`/`OnLogRestored` already carry `partitionId` — route writes into the scoped
  store.
**Done when** (`dotnet test --filter TestPartitionScopedStore` green):
- [ ] `Store_KeysAreScopedToPartition` — the same key string under two partition ids is two distinct
  rows; a read for one never returns the other's.
- [ ] `Restore_RoutesToScopedStore` — replay of a partition's WAL lands only in that partition's
  scope.
- [ ] full `dotnet test Kahuna.sln` (Server) stays green — hash KV/locks/sequencer unaffected.

### Task G2 — Real transfer into P′ (through P′'s replicas, not the local store)
**Depends on:** G1. **Files:** `Ranges/KvStateMachineTransfer.cs` (`ImportRangeAsync`),
`Ranges/RangeSplitter.cs`, `Ranges/RangeMerger.cs`; tests `TestKvRangeTransfer.cs` (multi-node),
`TestRangeSplit.cs`.
**Goal:** importing `[K,E)` installs it into `P'`'s **Raft-replicated** state (all replicas), not
just the executor node's local store.
- Replicate the imported entries through `P'`'s log (or use Kommander's coordinator-driven snapshot
  install — `IRaftStateMachineTransfer` is already registered; drive it for real splits). On commit,
  every `P'` replica's scoped store has `[K,E)`.
- Make the import genuinely byte-identical at the snapshot HLC across replicas.
**Done when** (`dotnet test --filter TestKvRangeTransfer` + `TestRangeSplit` green, **multi-node**):
- [ ] `Transfer_LandsOnAllTargetReplicas` — after a split, every replica of `P'` serves `[K,E)` from
  its own scoped store (kill the executor node; reads still succeed on another `P'` replica).
- [ ] `Split_DataReadableFromPPrime_AfterExecutorLeaves` — the Task-6 data-integrity read holds even
  when the split executor is not a `P'` replica.

### Task G3 — Drop `[K,E)` from P after cutover (re-enable the delete, now correct)
**Depends on:** G1, G2. **Files:** `IPersistenceBackend` (`DeleteKeysByRange` re-added, partition-
scoped + prefix-bounded — see Task 6 Findings 1/2), `Ranges/RangeSplitter.cs`,
`Ranges/RangeMerger.cs`; tests `TestRangeSplit.cs`, `TestRangeMerge.cs`.
**Goal:** with scoped storage, "drop `[K,E)` from P" is meaningful and safe — replacing
orphan-retention.
- Re-introduce a **partition-scoped** `DeleteKeysByRange(partitionId, prefix, start, end)` that is
  **prefix-bounded** (the Task-6 over-delete bug when `endKey==null`), runs **only on cutover
  success**, and is replicated to P's replicas.
**Done when** green:
- [ ] `Split_SourceDropsRange_OnSuccessOnly` — after a successful split `P` no longer serves
  `[K,E)`; after a *failed* cutover `P` retains it (no data loss).
- [ ] `Split_TopRange_DoesNotCorruptBystander` (carried over) still holds with real deletes.

### Task G4 — Re-validate the masked windows under scoped storage
**Depends on:** G1–G3, F3. **Files:** `RangeSplitter.cs`, `RangeMerger.cs`; tests
`TestRangeSplit.cs`, `TestRangeMerge.cs`.
**Goal:** the direct-write split window (F3) and the merge window (Task 8) are now *real* loss
windows; confirm the quiesce actually closes them.
**Done when** green:
- [ ] `Split_DirectWriteDuringQuiesce_NotLost` (F3's test, now meaningful) — the write is on `P'`
  exactly once.
- [ ] `Merge_LateWriteToRight_NotLost` — a write to `[B,C)` during the merge window survives on the
  survivor (add a merge quiesce if needed).

---

## Phase H — Deferred features

### Task H1 (was Task 12) — Load-based split trigger
**Depends on:** Phase G (so a split actually relieves load). **Files:** new
`Ranges/RangeLoadSampler.cs`, `Ranges/RangeSplitTrigger.cs` (add load path),
`KeyValueCollectSampler.cs`; config `KahunaConfiguration`; tests `TestLoadSplit.cs`.
**Goal:** split a **hot** range, not just a large one (design §5.1, §8).
- Track a per-range load signal (ops/sec and/or p99 latency) via the existing collector. When a
  range exceeds a load threshold (even under the size threshold), split it; for monotonic-append
  tails reuse the §8 hot-tail policy (already in `RangeSplitPolicy`).
- Add the **hash-shard-a-hot-prefix knob** (design §8) for insert-heavy tables that can't be helped
  by key-range split alone.
**Done when** (`dotnet test --filter TestLoadSplit` green):
- [ ] `HotRange_UnderSizeThreshold_SplitsOnLoad`.
- [ ] `LoadSplit_NoThrash` — load below the hysteresis low-water mark doesn't re-split.

### Task H2 (was Task 13) — Rebalance range replicas across nodes  **[BLOCKED upstream]**
**Depends on:** a Kommander primitive that does not exist. **Files (Kahuna side):** new
`Ranges/RangeRebalancer.cs`; tests `TestRangeRebalance.cs`. **Upstream:** Kommander per-group
membership change.
**Blocker:** Kommander exposes only cluster-wide `JoinCluster` — **no per-group
`AddServer`/`RemoveServer`**. Rebalance moves a range's *replicas* (not its key bounds), which is a
Raft membership change on that one group. Per the upstream-fix rule, **flag and stop**:
> **Kommander ask:** `AddServerAsync(partitionId, endpoint)` / `RemoveServerAsync(partitionId,
> endpoint)` (leader-only, joint-consensus membership change) on `IRaft`. Until it lands, Kahuna
> cannot rebalance.
**Done when:** the Kommander primitive exists, then `RangeRebalancer` moves a replica and
`TestRangeRebalance` proves the range stays available throughout (no key-bound/descriptor change,
pure membership change).

### Task H3 (was Task 14) — Meta-range sharding
**Depends on:** evidence the single meta partition (1) is hot (design §4, §10). **Files:**
`Ranges/RangeMapStore.cs`, `Ranges/RangeRouting.cs` (meta lookup), `KeySpaceRegistry`; tests
`TestMetaSharding.cs`.
**Goal:** shard the descriptor map across multiple meta partitions when partition 1 saturates.
- Partition the descriptor key space itself (CRDB-style meta/meta ranges): a fixed first-level shard
  (e.g. hash of key space) selects which meta partition holds a key space's descriptors; routing
  consults the right meta partition.
- Keep `MutateAsync` the single serialized writer **per meta shard**; the no-gap/no-overlap invariant
  is per key space so it survives sharding.
**Done when** (`dotnet test --filter TestMetaSharding` green):
- [ ] `Descriptors_ShardedAcrossMetaPartitions` — descriptors for distinct key spaces live on
  distinct meta partitions; routing resolves each correctly.
- [ ] `MetaShard_SurvivesRestartAndFailover` — per-shard durability/failover like Task 2.

### Task H4 — SSI / serializability layer  **[separate spec]**
**Depends on:** Phase F (range locks) + Phase G. **Out of scope here** — design §2 says
serializability is a *consequence* this work unlocks (range-scoped locks + SSI), specified
separately. Reference only: when written, it builds on per-range locks (Task 11) and partition-
scoped MVCC (Phase G).

---

## Recommended order & risk

1. **Phase F1–F2, F6** first — small, self-contained correctness/cleanup wins on the live path
   (batch fence, lock-fence test, helper consolidation), no foundation dependency. **F7 (auto-seed) is
   done; F8 (seed-forwarding) is the remaining live-path item — do it next if any multi-node consumer
   (CamusDB) is enabling key-range, since it is the multi-node blocker and has no Phase-G dependency.**
2. **Phase G** — the foundation. **Highest leverage and highest risk** (touches all storage
   backends + the transfer path). Everything that delivers the spec's *motivation* (load/space
   distribution) and the *real* correctness of split/merge depends on it. G1 → G2 → G3 → G4.
3. **Phase F3/F4/F5** interleaved with/after G where their tests become meaningful.
4. **Phase H1 (load split)** after G. **H2 (rebalance)** only after the Kommander primitive lands.
   **H3 (meta sharding)** only when partition 1 is measurably hot. **H4 (SSI)** is a separate spec.

**Single biggest item:** Phase G. Without it the feature is *logical* range routing/locking
(working, tested) but not *physical* sharding — and F3/G4's loss windows stay masked rather than
fixed.
