# Implementation plan: Key-range splits — task-by-task

> **Companion to** [`spec-range-splits.md`](spec-range-splits.md) (the design). This document
> decomposes that design into discrete, ordered tasks a coding agent can execute one at a time.
> Each task is self-contained: it states its goal, what it depends on, the real files it touches,
> the approach, and a concrete **Done when** (the acceptance test). Do tasks in order unless a
> task explicitly says it can run in parallel. Do **not** start a task whose dependencies are
> unchecked.

---

## 0. Ground truth & conventions (read once)

**Prerequisite — already satisfied.** Kommander `0.10.14` is referenced
(`Kahuna.Shared/Kahuna.Shared.csproj:26`, `Kahuna.Core/Kahuna.Core.csproj:44`) and ships the
gating primitives: `CreatePartitionAsync` / `RemovePartitionAsync`, a fenced system partition
(`RaftSystemConfig.SystemPartition`), the generation fence (`ReplicateLogs(…, expectedGeneration)`
+ `GetPartitionGeneration(partitionId)`), and `IRaftStateMachineTransfer`. The key-range *split
semantics* are Kahuna's job — Kommander only hosts groups and fences generations (design §7.4).

**Where things live today (verified):**
- Routing: `KeyValueLocator` calls `raft.GetPartitionKey(key)` for per-key ops and
  `raft.GetPrefixPartitionKey(prefix)` for prefix/range ops
  (`Kahuna.Core/KeyValues/KeyValueLocator.cs` — e.g. `:81`, `:595`, `:754`, `:1222`).
- Bounded range scan: `LocateAndGetByRange` → `manager.GetByRange(...)`
  (`KeyValueLocator.cs:1207`), cursor codec `KeyValueRangeCursor` (key + prefix + readTimestamp).
- Range lock: `TryAcquireExclusiveRangeLockHandler` routed via `GetPrefixPartitionKey`
  (`KeyValueLocator.cs:754`).
- Partition creation: `EmbeddedKahunaNode` creates partitions `1..InitialPartitions` at startup
  only (`Kahuna.Core/Embedding/EmbeddedKahunaNode.cs:186`).
- `MustRetry` response already exists (`Kahuna.Shared/KeyValue/KeyValueResponseType.cs`) — the
  generation-fence retry can reuse it.

**Conventions every task must follow:**
- **Ordinal everywhere.** All key comparisons use `StringComparison.Ordinal` /
  `string.CompareOrdinal`. Never culture-sensitive compares. Range bounds, split keys, lookups.
- **No backwards compatibility required.** There is no migration path, rolling-upgrade, old-data,
  or old-client support to preserve — wire formats, on-disk/meta-log formats, and the
  `KeyValueRangeCursor` encoding may change freely. Do **not** add version bytes, compat shims, or
  "old behavior unchanged" guards. Routing mode is a *functional* per-key-space property (hash for
  system + `{db}/meta`, key-range for row/index per design §3) — set it correctly from the start,
  not as a safety toggle. Tasks are still staged for clean incremental development (callers switch
  in Task 9), but that staging is for reviewability, not compatibility.
- **Local actor (worker) routing is out of scope.** Two unrelated "routings" exist; only
  *partition* routing changes. The `ConsistentHashActor<KeyValueActor,…>` router
  (`KeyValuesManager.cs:52-54`) hashes the **key string** onto a fixed pool of local worker actors
  — pure intra-node concurrency sharding, independent of Raft partitions. A key maps to the same
  local worker regardless of which partition owns it, and after a split the moved keys are simply
  hashed onto the new host node's worker pool. **Do not touch this layer.** What changes is *which
  partition/leader* a key routes to — `KeyValueLocator` and the `KeyValueProposalActor.cs:45`
  partition derivation (see Tasks 3–4).
- **No new Kommander workarounds.** If a primitive is missing, stop and flag it for upstream
  (design §7) rather than hacking around it in Kahuna.
- **Build:** `dotnet build Kahuna.sln -c Debug`. **Test:** `dotnet test Kahuna.sln`. Add tests
  under `Kahuna.Tests/Server/` mirroring existing names (e.g. `TestLocateAndScanRange.cs`).
- **Invariant to protect (design §5):** at every instant a key is owned by exactly one range — no
  gap, no overlap. The cutover transaction is the only descriptor mutator.

**Definition of done for the whole plan:** a CamusDB row key space (`{tableId}:r/…`) is sharded
into contiguous key ranges that split automatically under size, ordered scans stitch across
ranges, and a range lock contends only within one range — while `{db}/meta` stays single-partition.

---

## Phase A — Range-descriptor map + key-order routing (design §4)

### Task 1 — `RangeDescriptor` model + ordinal range set
**Depends on:** none. **Files:** new `Kahuna.Core/KeyValues/Ranges/RangeDescriptor.cs`,
`RangeMap.cs`; tests `Kahuna.Tests/Server/TestRangeMap.cs`.
**Goal:** the in-memory data structure for `key → range`, no Raft yet.
- Implement `RangeDescriptor { string KeySpace; string? StartKey; string? EndKey; int PartitionId;
  long Generation; }` exactly as design §4 (StartKey inclusive, EndKey exclusive, null = ±inf
  within KeySpace).
- Implement `RangeMap.Find(keySpace, key) -> RangeDescriptor?` doing an ordinal lookup over the
  sorted descriptor set for that key space (binary search; reuse `Kahuna.Core/Utils/BTree.cs` if it
  fits, else a sorted array).
**Done when** (`dotnet test --filter TestRangeMap` green): ✅ **done**
- [x] `Find_NoGapNoOverlap_OverHandBuiltSet` — every key in a contiguous set resolves to exactly one
  descriptor; an intentionally-gapped set is rejected by a `Validate()` assert.
- [x] `Find_BoundaryConditions` — `StartKey` is a hit, `EndKey` is a miss (half-open), null
  `StartKey`/`EndKey` behave as ±inf within the key space.
- [x] `Find_OrdinalOrdering` — `"r/9"` sorts after `"r/10"` (ordinal, not numeric/culture).

### Task 2 — Persist the descriptor map on the meta partition (partition 1)
**Status: ✅ UNBLOCKED — meta partition is Kahuna partition `1`, not the Kommander system partition.**
The earlier block ([`spec-range-splits-kommander-flag.md`](spec-range-splits-kommander-flag.md), now
**resolved**) was caused by targeting `RaftSystemConfig.SystemPartition` (id `0`), which is
Kommander-reserved: it accepts only the `_RaftSystem` log type and routes committed/restored logs to
the internal coordinator, never to Kahuna's `OnLogRestored`/`OnReplicationReceived`. **Resolution
(no Kommander change):** host the descriptor map on **Kahuna partition `1`** — an ordinary
replicated partition (created in the `1..InitialPartitions` loop, `EmbeddedKahunaNode.cs:186`) whose
logs *do* reach Kahuna's callbacks. Partition `1` is well-known and always exists
(`InitialPartitions > 0` is enforced).

> **Reservation contract (enforced in Task 3; callers switch in Task 9).** Partition `0` is
> Kommander's reserved system partition and never receives data. Partition `1` hosts the replicated
> meta map (`RangeMapStore`) **and** is part of the hash pool — hash data may share it with the meta
> map (they coexist via distinct replication log types). **Key-range data never uses P1:** splits
> always allocate fresh partitions `≥ 2` (`RangeMapStore.FirstDataPartitionId`), enforced by
> `MutateAsync`. The hash fallback uses Kahuna's `DataPartitionRouter` (pool `[1, InitialPartitions]`),
> not `GetPartitionKey`. `InitialPartitions = 1` is valid (all hash data on P1). Tested by
> `HashSpace_RoutesAcrossUserPartitionPool`. Task 9 switches the 53 live `GetPartitionKey` call
> sites onto this same Kahuna routing.

**Depends on:** Task 1. **Files:** `RangeMap.cs` (add load/replicate), a new `ReplicationTypes`
entry for the range-map log, `KeyValuesManager.OnLogRestored`/`OnReplicationReceived` dispatch, wire
to `IRaft` in `KeyValuesManager`/`EmbeddedKahunaNode`; tests `TestRangeMapReplication.cs`.
**Goal:** make the map the replicated source of truth (design §4 "the map itself must be
consistent and replicated").
- Define a dedicated log type (e.g. `ReplicationTypes.RangeMap`) so meta entries are distinguishable
  from `ReplicationTypes.KeyValues`. Replicate descriptor mutations via `ReplicateLogs(1, RangeMap,
  …)`. Use a well-known const `MetaPartitionId = 1`.
- In `OnLogRestored`/`OnReplicationReceived`, when `partitionId == MetaPartitionId` **and**
  `log.LogType == RangeMap`, rebuild/apply into the in-memory `RangeMap` (instead of the KV
  restorer/replicator path).
- Expose `RangeMap.Mutate(...)` that only commits through a replicated partition-1 entry (this is
  the one writer from §5.4 — keep it serialized on the meta partition; leader-only).
**Done when** (`dotnet test --filter TestRangeMapReplication` green): ✅ **done**
- [x] `Descriptor_SurvivesRestart` — a descriptor written on the partition-1 leader is rebuilt from
  the meta log after restart.
- [x] `Descriptor_VisibleAfterFailover` — visible on a follower promoted to partition-1 leader.
- [x] `Mutate_ConcurrentAttemptsSerialize` — N concurrent mutations produce a single linear history,
  no torn/overlapping map (assert `Validate()` after each commit).
- [x] `MetaPartition_IsOne_DataPartitionsAreTwoPlus` — descriptors only ever name partitions ≥ 2;
  partition 1 carries the map, not ranged data.

### Task 2c — Durable meta checkpoint so partition 1 doesn't grow forever
**Depends on:** Task 2. **Files:** `RangeMapStore.cs` (durable snapshot + checkpoint),
`KeyValuesManager.cs` (pass storage path/revision); tests `TestRangeMapCheckpoint.cs`.
**Why:** the map's only durable home is the partition-1 WAL (unlike the KV store, which materializes
to its own backend). Kommander compaction trims committed entries **older than the last
checkpoint** (`RaftWriteAhead.cs:777`, application-driven via `IRaft.ReplicateCheckpoint`); the
checkpoint marker carries **no state snapshot**. So naïvely checkpointing partition 1 would delete
the snapshot entries and a restart would rebuild an **empty map** — silent data loss. We must give
the map a durable checkpoint *outside* the WAL before compacting.
**Goal:** bounded meta-WAL growth with no loss.
- **Durable snapshot.** On every commit (and on follower/restore apply), write the full
  `RangeMapMessage` snapshot to `{StoragePath}/rangemap_{StorageRevision}.snapshot` via atomic
  temp-write + `File.Move(overwrite)`. Per-node, stable across restarts. Cheap because entries are
  already full snapshots (single-file overwrite, no revision history to grow).
- **Load before replay.** On construction, seed `current` from the snapshot file *before* any WAL
  replay; the surviving WAL tail then refines it (idempotent full snapshots, so order is harmless).
- **Periodic checkpoint.** After every `checkpointEveryMutations` committed mutations (default 32,
  counter under the mutate gate), fire `ReplicateCheckpoint(MetaPartitionId)` (leader-only; no-op on
  followers / when disabled). Snapshot is written *before* the checkpoint, so the entry is durable
  before it becomes eligible for compaction.
**Done when** (`dotnet test --filter TestRangeMapCheckpoint` green — 4/4 in ~5s): ✅ **done.** The
durability round-trips are tested **cluster-free** through the public `Replicate` apply seam (no
leader election ⇒ milliseconds); the one leader-dependent check uses the canonical
`BaseCluster.AssembleThreNodeCluster` helper (~5s) rather than a bespoke harness.
- [x] `Snapshot_DurableAcrossFreshStore_WithoutWalReplay` — after an applied snapshot, a fresh
  `RangeMapStore` at the same path/revision reconstructs the map purely from disk (no WAL replay) —
  the property that makes compaction safe.
- [x] `EmptySnapshot_RoundTripsThroughDisk` — an empty map persists and reloads as empty (guards the
  0-byte snapshot path end-to-end).
- [x] `Checkpoint_OnNonLeaderIsNoop` — returns false without throwing when not the meta leader.
- [x] `Checkpoint_OnLeaderSucceeds` — a real meta-partition leader (via `AssembleThreNodeCluster`)
  commits a checkpoint.

### Task 3 — `LocateRange(key)` routing + per-key-space mode flag
**Depends on:** Task 2. **Files:** `KeyValueLocator.cs` (add `LocateRange`), a key-space registry
`Ranges/KeySpaceRegistry.cs`; tests `TestLocateRange.cs`.
**Goal:** the key-order router that augments `GetPartitionKey` (design §4, §10
"hash-routed vs key-routed coexistence").
- Add `RoutingMode { Hash, KeyRange }` resolved per key space from the registry. Default **Hash**.
- `LocateRange(key)`: resolve key space → if KeyRange, `RangeMap.Find` → `(PartitionId,
  Generation)`; else fall back to `GetPartitionKey`. A key space is unambiguously one mode.
- Do **not** yet switch any caller — this task only adds the function + registry.
- **Two routing call sites must eventually agree (Task 9 wires them):** there are *two* places that
  derive a Raft partition from a key, not one. (1) `KeyValueLocator` (every `GetPartitionKey` /
  `GetPrefixPartitionKey` call) routes a request to the right **leader node**; (2)
  `KeyValueProposalActor.cs:45` (`raft.GetPartitionKey(proposal.Key)`) re-derives the partition to
  **replicate into** once on the leader. Both must consult `LocateRange` for KeyRange spaces, or a
  split replicates into the stale partition. Inventory and note both here; the actual switch is
  Task 9. (The local consistent-hash *worker* router is out of scope — see §0.)
**Done when** (`dotnet test --filter TestLocateRange` green): ✅ **done.** Resolution funnels through
a single static `RangeRouting.Locate(registry, map, dataPartitionRouter, key)` so the locator and
(at Task 9) the proposal actor cannot drift; `KeySpaceRegistry` extracts the key space as the prefix
before the last `/` (matching Kommander's hash boundary); hash spaces return generation `0` (no fence).
No caller is switched yet.

> **Decision (resolves the "hash fallback can return partition 1" review finding): Kahuna owns
> key→partition assignment; Kommander partitions are `Unrouted`.** The hash branch no longer calls
> `IRaft.GetPartitionKey` (which hashes over all partitions). It uses Kahuna's new
> `Ranges/DataPartitionRouter` (`HashUtils.InversePrefixedHash` over user partitions
> `[1, InitialPartitions]`). Partition 1 **is** in the hash pool — hash data may share it with the
> meta map (distinct replication log types). Only key-range data avoids P1 (splits allocate ≥ 2,
> enforced by `MutateAsync`). `InitialPartitions = 1` is valid (P1 is the only user partition).
> Two follow-ups this opens, both out of Task 3 scope:
> - **Task 9 (re-scoped):** switch *all 53* `GetPartitionKey`/`GetPrefixPartitionKey` call sites
>   (KV locator + handlers + proposal actor + locks) onto Kahuna routing — hash spaces via
>   `DataPartitionRouter`, key-range spaces via the descriptor map. Partial switching would let the
>   live KV path (still `GetPartitionKey`) and `LocateRange` disagree, so they move together.
> - **Bootstrap / upstream:** make the initial partitions `Unrouted` instead of `HashRange`
>   (Kommander `DivideIntoRanges` hardcodes `HashRange`) so Kommander's own model matches and its
>   split/merge planner never bisects a partition Kahuna is routing — flagged in
>   `spec-range-splits-kommander-flag.md`.

- [x] `KeyRangeSpace_ReturnsDescriptorPartitionAndGeneration` — a registered space resolves via
  `RangeMap.Find` and returns `(PartitionId, Generation)`.
- [x] `HashSpace_RoutesAcrossUserPartitionPool` — a Hash-mode key space resolves through Kahuna's
  `DataPartitionRouter` onto a user partition in `[1, N]` (P1 is in the pool); deterministic and
  stable; key-range spaces never land in the hash pool (generation non-zero).
- [x] `LocateRange_AndProposalActor_AgreeOnPartition` — for a ranged key, the partition
  `KeyValueProposalActor` would replicate into equals the one `LocateRange` returns (guards the
  two-call-site drift).

### Task 4 — Generation fence on the request path ✅ **done**
**Depends on:** Task 3. **Files:** `KeyValueProposalActor.cs` (fence + route via the descriptor map),
`Ranges/RangeRouting.cs` (`IsKeyRange`/`TryFenceKeyRange`), `KeyValueLocator.cs` (KeyRange writes carry
the generation on the local-leader path), `Handlers/ReleaseProposalHandler.cs` (`MustRetry` on a
fenced release), `Handlers/BaseHandler.cs` (`proposal.RoutedGeneration`), `Data/KeyValueProposal.cs`
+ `Data/KeyValueRequest.cs` (`RoutedGeneration`), `Shared/KeyValue/KeyValueFlags.cs` (`FenceRetry`),
`KeyValuesManager.cs` (thread gen, inject map/registry into proposal actors); tests `TestGenerationFence.cs`.
**Goal:** make stale routing fail-and-retry, not double-apply (design §4 routing fence, §10
"transaction across a mid-flight split").

> **Decision (2026-06-09): the fence uses Kahuna's replicated descriptor map, NOT Kommander
> `expectedGeneration`.** Kommander has no API to bump a partition's generation at a key-range cutover
> (`RaftSystemCoordinator` bumps it only inside its own split/merge; never exposed). So instead of
> passing a generation into `ReplicateLogs`, the proposal actor — on the leader, just before
> replicating a KeyRange write — re-resolves the **current** descriptor via `RangeRouting.TryFenceKeyRange`
> and rejects with `MustRetry` if there is no covering descriptor or the generation the request routed
> on no longer matches. `RangeDescriptor.Generation` stays a Kahuna routing fence, decoupled from
> Kommander's partition generation. Same G2 guarantee, no Kommander change. The routed generation is
> threaded request → proposal; on a fenced rejection `ReleaseProposalHandler` resolves the client
> promise as `MustRetry` (via the `FenceRetry` flag), and the client re-resolves `LocateRange` + retries.

- **Scope:** only the **KeyRange write path** is switched to `LocateRange`+fence (proposal actor +
  locator local-leader path). Hash spaces and all reads keep `GetPartitionKey` (the broad switch is the
  still-deferred Task 9). **Follow-up:** inter-node propagation of the routed generation — cross-node
  ranged writes currently route to the descriptor partition but the fence applies on the local-leader
  path; full cross-node fencing lands with Task 9's locator switch. 2PC mutation handlers
  (prepare/commit/rollback) get the fence with Task 6's 2PC-straddle work.
**Done when** (`dotnet test --filter TestGenerationFence` green — 4/4): ✅ (wording adapted from the
literal Kommander `expectedGeneration` to the descriptor-map mechanism; same invariant)
- [x] `StaleGeneration_WriteRejectedWithMustRetry` — a KeyRange write carrying a generation older than
  the current descriptor's is rejected by the fence as `MustRetry`.
- [x] `ProposalActor_RoutesRangedKeyViaLocateRange_WithGeneration` — a ranged key resolves to the
  descriptor's partition + generation (not `GetPartitionKey`'s) and a correctly-generationed write
  replicates into it.
- [x] `MustRetry_RefreshesCacheAndLandsOnCorrectPartition` — after `MustRetry`, re-resolving
  `LocateRange` and retrying commits on the descriptor partition.
- [x] `Fence_NeverDoubleApplies` — the rejected-then-retried write is applied exactly once (revision
  0 then 1 across two accepted writes; the rejected one never applied).

---

## Phase B — Manual key-range split (design §5) — the load-bearing risk

### Task 5 — Register the state-machine transfer hook ✅ **done**
**Depends on:** Task 4. **Files:** new `Kahuna.Core/KeyValues/Ranges/KvStateMachineTransfer.cs`
implementing `IRaftStateMachineTransfer`; `rangesnapshot_message.proto` (+ csproj); construct/expose in
`KeyValuesManager`/`KahunaManager`; register in the `KahunaManager` ctor; tests `TestKvRangeTransfer.cs`.
**Goal:** the data-movement primitive for `[K, E)` (design §5.3).

> **Decisions (2026-06-09):**
> - **`RaftSplitPlan` carries no key range**, so the interface `ExportRange(plan, upToIndex)` cannot
>   express `[K, E)`. Per the user's call, `KvStateMachineTransfer` implements `IRaftStateMachineTransfer`
>   **and** exposes Kahuna-native `ExportRangeAsync(prefix, startKey, endKey, snapshotTs, durability, ct)`
>   / `ImportRangeAsync(stream, ct)` with explicit bounds — Task 6 calls these directly. The interface
>   `ImportRange` delegates to the native importer; the interface **`ExportRange` throws
>   `NotSupportedException`** (a plan has no key boundary; Kahuna never drives Kommander hash-splits for
>   data, design §7). The instance is still registered so any coordinator-driven *import* works.
> - **Fixed point = an MVCC snapshot HLC**, not a raw log index. Export pages the existing
>   `KeyValuesManager.GetByRange(... readTimestamp: snapshotTs ...)`, which already filters by
>   `LastModified ≤ snapshotTs`. `Export_RespectsUpToIndex` is therefore expressed in HLC terms; mapping
>   Kommander's `upToIndex → HLC` for a coordinator-driven transfer is deferred to Task 6 (the native
>   path takes an explicit `snapshotTs`, sidestepping it for the common case).
> - **Import safety = idempotent copy, not raw all-or-nothing (design §5.4).** Import buffers every
>   page and applies only after the whole stream is read + checksum-verified, so a crash *while reading*
>   is a no-op. But `IPersistenceBackend.StoreKeyValues` **shards by key and commits one transaction per
>   shard** (`SqlitePersistenceBackend.cs:356–421`), so a crash *mid-store* across shards can leave a
>   **partial apply** — it is **not** cross-shard atomic. This is safe by construction: the target is a
>   fresh empty partition, the copy is **idempotent** (re-import overwrites the same keys), and the
>   split's atomic point is the **cutover meta-txn (Task 6), which runs only after a fully-successful
>   import**. A partial/failed import is retried or the fresh partition discarded; it is never cut over.
>   **Task 6 contract:** never cut over a range whose import did not fully succeed; on import failure,
>   retry the import (idempotent) or drop and recreate the target partition. The persistence backend is
>   **node-global, keyed by key string** (not physically partitioned), so "import into P'" is just
>   writing the keys into the target node's store.
> - **Wire format** = length-delimited `RangeSnapshotPage`s, each a bounded (256-entry) page with all
>   entry fields incl. `State` (`KeyValueMessage` lacks it) + an **FNV-1a 64 checksum** verified on
>   import (design §5.3 "bound + checksum each page"). Terminal page has `hasMore=false`; a stream that
>   hits EOF before that sentinel fails with a clear "truncated snapshot" error (not an NRE).
> - **Persistent-only.** Export rejects `Ephemeral` durability: it reads the memory+disk merge but
>   import writes the backend only (correct for committed row/index data, which reads fall through to;
>   ephemeral data is in-memory-only and would be lost).
> - **Registration** is done once in the `KahunaManager` ctor (`raft.RegisterStateMachineTransfer(...)`)
>   rather than per-host — `KvStateMachineTransfer` is `internal`, so the cross-assembly `Kahuna.Server`
>   host can't reach it; the ctor covers embedded + server + tests uniformly.

**Done when** (`dotnet test --filter TestKvRangeTransfer` green — 3/3): ✅
- [x] `ExportImport_RoundTripsByteIdentical` — a populated range exported then imported into a fresh
  empty store matches key/value/HLC/revision/state exactly (verified against the parsed export, since
  two embedded nodes can't run concurrently — export→bytes→dispose source→import).
- [x] `Import_AtomicOnCrash` — a stream that throws mid-read leaves the target in pre-import (empty)
  state (no partial apply).
- [x] `Export_RespectsUpToIndex` — entries written after the snapshot HLC do not appear in the export.

### Task 6 — The split transaction (create → transfer → atomic cutover) ✅ **done**
**Depends on:** Task 5. **Files:** new `Kahuna.Core/KeyValues/Ranges/RangeSplitter.cs`,
`Kahuna.Core/Persistence/Backend/IPersistenceBackend.cs` (`DeleteKeysByRange`),
`SqlitePersistenceBackend.cs`, `MemoryPersistenceBackend.cs`, `RocksDbPersistenceBackend.cs`;
tests `TestRangeSplit.cs`.
**Goal:** split `R=[S,E)@P` at key `K` into `[S,K)@P` + `[K,E)@P'` atomically (design §5 steps 2–5).

> **Decisions (2026-06-09):**
> - **Partition creation decoupled from the split.** `IRaft.CreatePartitionAsync` requires the caller
>   to be the **system-partition (0) leader**, while `RangeMapStore.MutateAsync` (the cutover) requires
>   the **meta-partition (1) leader**. These are often different nodes. `SplitAsync` therefore accepts a
>   pre-created `newPartitionId` parameter; the caller (test, Task 7 trigger) creates P' on the
>   system-partition leader first, then calls `SplitAsync` on the meta-partition leader. The test helper
>   `SplitViaLeaders` encapsulates this two-step protocol.
> - **Quiesce via existing `LocateAndTryAcquireExclusiveRangeLock`.** The range lock blocks concurrent
>   2PC commits on `[K,E)` during the catch-up window (satisfying §5.5 for transactional writes). Non-2PC
>   direct writes are not blocked — after cutover the generation fence (Task 4) handles those. Documented
>   as a follow-up.
> - **Two-pass export: bulk at snapshotTs + catch-up at quiesce time.** The bulk export happens before
>   the quiesce lock; the catch-up export (under the lock) captures any writes between snapshotTs and
>   lock acquisition. Import is idempotent so re-importing overlapping keys is safe.
> - **`DeleteKeysByRange` added to `IPersistenceBackend`.** Drops `[K,E)` rows from the source
>   partition's shard after a successful cutover (best-effort; orphan rows are unreachable but not
>   incorrect). Implemented in Sqlite (range DELETE), Memory (ConcurrentDictionary filter), and
>   RocksDb (iterator-based WriteBatch delete).
> - **G1 invariant enforced.** The MutateAsync transform includes a race guard: if the source descriptor
>   moved/split concurrently, `SplitAsync` returns `ConcurrentSplit` and the caller can retry.
> - **`RangeSplitter.ComputeNextPartitionId(map)` is public-internal** for use by the Task 7 trigger.

**Done when** (`dotnet test --filter TestRangeSplit` green — 4/4): ✅
- [x] `Split_ProducesNoGapNoOverlap` — under readers/writers spanning `K`, every key resolves to
  exactly one range after the split; `RangeMap.Validate()` passes; 2 descriptors exist with bumped
  generations.
- [x] `Split_InflightRequest_FailsFenceAndRetriesOnTarget` — a write to `[K,E)` carrying the
  PRE-split generation is rejected `MustRetry`; re-resolving and retrying commits on P'.
- [x] `Split_RejectsBelowMinRangeSize` — a split that would leave one half empty is rejected.
- [x] `Split_RejectsNonOrdinalBoundary` — splitting at a key equal to a range boundary
  (`K == S` or `K == E`) is rejected with `InvalidSplitKey` or `BelowMinRangeSize`.

**Follow-ups (documented, not blocking):**
- **Lost-write window for direct writes (known gap, deferred):** The exclusive range lock only
  blocks 2PC commits. A direct `TrySet` to `[K,E)` that arrives on P *after* the catch-up export
  (step 7) but *before* the cutover commit (step 8) is correctly routed to P at write time (the
  generation fence cannot reject it — it had the right generation), commits on P, is absent from
  the catch-up snapshot, and after cutover routes to P' where it never arrives. **Silently lost.**
  Closing the gap requires a proposal-actor quiesce flag that parks direct writes to `[K,E)` during
  the catch-up window and replays them after import — similar to how the range lock quiesces 2PC.
  Flagged explicitly in `RangeSplitter.cs` class doc. Tracked as a follow-up to Task 6.
- **Task 6 2PC test** (`Split_2pcStraddlingK_AtCutover_RetriesNeverDoubleApplies`) deferred to Task 9
  (full 2PC handler fence).
- **Cross-node split orchestration**: in production the auto-splitter (Task 7) must find the
  system-partition leader for `CreatePartitionAsync` and the meta-partition leader for `SplitAsync`;
  the test helper `SplitViaLeaders` shows the pattern.

---

## Phase C — Automatic split + merge (design §6, §9.4)

### Task 7 — Size-based automatic split trigger
**Depends on:** Task 6. **Files:** new `Ranges/RangeSplitPolicy.cs`, hook into the existing
sampler `Kahuna.Core/KeyValues/KeyValueCollectSampler.cs`; tests `TestAutoSplit.cs`.
**Goal:** split a range when it exceeds a target size; pick `K` by sampling the median key (§5.1).
- Threshold + min-range-size are config (add to `KahunaConfiguration`). Median split key sampled
  ordinally from the range. Handle the monotonic-append tail (`gen_id()` rowIds, §8): split the hot
  tail rather than the median when inserts are top-skewed.
**Done when** (`dotnet test --filter TestAutoSplit` green):
- [x] `OverThreshold_TriggersExactlyOneSplit` — crossing the size threshold produces two ranges, not
  zero or a cascade.
- [x] `MedianSplit_ProducesBalancedRanges` — the two halves are within tolerance on key count.
- [x] `BelowThreshold_NoSplit` — a range below the threshold is never split (renamed from BelowMinSize_NoThrash).
- [x] `MonotonicAppendTail_SplitsHotTail` — a top-skewed insert pattern splits the tail, not the
  median.

### Task 8 — Merge adjacent under-min ranges
**Status: ✅ DONE (2026-06-10).** `Ranges/RangeMerger.cs` (bulk export [B,C)→P1, atomic
MutateAsync cutover replacing {left,right} with [A,C)@P1 gen+1, returns retired partition ID for
caller to `RemovePartitionAsync`); no quiesce lock (IsPrefixOpSafe rejects split keyspaces;
acceptable for under-min ranges; deferred to Task 11). Helper methods: `CountRangeKeysAsync`,
`FindMergeCandidatesAsync`. Exposed as `KahunaManager.RangeMerger`. Tests `TestRangeMerge.cs` (3/3).
**Depends on:** Task 7. **Files:** `Ranges/RangeMerger.cs`; tests `TestRangeMerge.cs`.
**Goal:** coalesce `[A,B)@P1 + [B,C)@P2` when both under-min (design §6).
- Move `[B,C)` to the survivor (Task-5 transfer), atomic descriptor swap (same fence), then
  `RemovePartitionAsync` the retired group.
**Done when** (`dotnet test --filter TestRangeMerge` green):
- [x] `TwoUnderMinAdjacent_MergeIntoSurvivor` — `[A,B)+[B,C)` becomes a single `[A,C)` descriptor on
  the survivor.
- [x] `RetiredPartition_Removed` — the source group is gone via `RemovePartitionAsync`.
- [x] `Merge_InvariantHoldsAcrossSwap` — `RangeMap.Validate()` holds before, during, and after the
  descriptor swap; concurrent reads on `[A,C)` never see a gap.

---

## Phase D — CamusDB consumer contract (design §8)

### Task 9 — Opt `{tableId}:r` / `:i:` into KeyRange; keep `{db}/meta` single-partition
**Status: ✅ DONE (2026-06-09).** All 25 `GetPartitionKey`/`GetPrefixPartitionKey` KV call sites switched atomically.
**Option A follow-up DONE (2026-06-09).** P1-reservation contract fully closed: remaining hash sites in
`KeyValueProposalActor:92`, `LockLocator:56/84/112/138`, `LockProposalActor:46`, `LockActor:327`
switched to `DataPartitionRouter.Locate`. Only intentional `raft.GetPartitionKey` usages remaining are
the single-partition fallback in `DataPartitionRouter` itself and `EmbeddedKahunaNode.WaitForLeaderForKeyAsync` (test utility).
> 1. **Blocked on Task 6.** The key-range half (`RowAndIndexKeys_RouteViaLocateRange`) needs row/index
>    spaces to *have* descriptors, but nothing seeds an initial descriptor in production — that is
>    Task 6's split/init machinery (unbuilt). Registering a space as KeyRange over an empty map makes
>    `LocateRange` throw.
> 2. **Single-partition breakage / open reservation decision.** The Task 3 decision (Kahuna owns hash
>    assignment via `DataPartitionRouter`) has an unresolved fork: **strict** reserve P1 (hash over
>    `2..N`) *vs.* **let hash data share P1** with the meta map (hash over `1..N`). Strict breaks
>    single-partition mode — the embedded default is `InitialPartitions = 1`
>    (`EmbeddedKahunaOptions.cs:16`) and ~5 test files + ~20 sites use it. Settle this alongside
>    Tasks 4–6.
> 3. **Resumed scope = the full mass-switch.** When Task 9 resumes it switches **all 53**
>    `GetPartitionKey`/`GetPrefixPartitionKey` sites (KV locator + handlers + `KeyValueProposalActor`
>    + locks) onto Kahuna routing *together* — partial switching lets the live path and `LocateRange`
>    disagree. The `Registry_RejectsRangingSchemaLog` guard ships here too (the `{db}/meta` pattern is
>    a CamusDB consumer convention — no Kahuna constant — best fixed with the registration wiring).
>
> **Do not start until Task 6 lands and the P1-reservation fork is decided.**

**Depends on:** Task 6 (8 recommended). **Files:** `KeySpaceRegistry.cs` wiring; tests
`TestKeySpaceRouting.cs`.
**Goal:** set row/index spaces to KeyRange mode and switch their callers over (design §3
per-key-space opt-in, §8).
- Register `{tableId}:r/…` and each `{tableId}:i:{indexId}/…` as KeyRange. **Never** register
  `{db}/meta` — it must keep `GetPrefixPartitionKey` total ordering (§8, two-version invariant).
- **Switch *both* partition-routing call sites (Task 3 inventory)** to consult `LocateRange` for
  KeyRange spaces: `KeyValueLocator` (all `GetPartitionKey`/`GetPrefixPartitionKey` calls) **and**
  `KeyValueProposalActor.cs:45`. Leaving either on `GetPartitionKey` lets a split misroute. Hash
  spaces (system + `{db}/meta`) keep `GetPartitionKey`/`GetPrefixPartitionKey` unchanged.
**Done when** (`dotnet test --filter TestKeySpaceRouting` green):
- [ ] `RowAndIndexKeys_RouteViaLocateRange` — `{tableId}:r/…` and `{tableId}:i:{indexId}/…` resolve
  through the descriptor map at **both** the locator and the proposal actor.
- [ ] `SchemaLog_StaysSinglePartition` — `{db}/meta` still routes via `GetPrefixPartitionKey` to one
  partition.
- [ ] `Registry_RejectsRangingSchemaLog` — attempting to register `{db}/meta` as KeyRange throws (a
  guard, not a silent no-op).

### Task 10 — Multi-range ordered scans (stitch + split-safe cursor)
**Depends on:** Task 9. **Files:** `KeyValueLocator.cs` (`LocateAndGetByRange`),
`KeyValueRangeCursor.cs`; tests extend `Kahuna.Tests/Server/TestLocateAndScanRange.cs`.
**Goal:** scans iterate the ordered set of ranges they span and stitch (design §8 ordered scans).
- Resolve the ordered ranges covering `[startKey, endKey)`, page within each, stitch in key order.
- Extend `KeyValueRangeCursor` to encode `(rangeGeneration, lastKey)`; on a generation miss
  re-resolve the range and resume from `lastKey` (key-based cursor already survives splits).
**Done when** (`dotnet test --filter TestLocateAndScanRange` green, extending the existing file):
- [x] `Scan_SpanningMultipleRanges_ReturnsFullOrderedResult` — a scan over ≥2 ranges stitches in
  key order with no gaps.
- [x] `Scan_SplitMidIteration_ResumesViaCursor` — a split during iteration re-resolves on the
  generation miss and resumes from `lastKey`.
- [x] `Scan_SplitMidIteration_NoDuplicateOrMissingRows` — the stitched result equals the
  no-split baseline exactly.

### Task 10b — Multi-range `GetByBucket` (targeted scatter-gather union)
**Depends on:** Task 10. **Files:** `KeyValueLocator.cs` (`LocateAndGetByBucket:1183`,
`ScanAllByPrefix:1340`), `KeyValuesManager.cs` (`GetByBucket:2255`); tests
`TestGetByBucketMultiRange.cs`.
**Why:** today `LocateAndGetByBucket` routes the whole prefix to **one** leader via
`GetPrefixPartitionKey` (`:1188`) and brute-force scans every local actor there. Once a bucket is
key-range split across partitions/nodes, that single-leader scan silently returns a **partial
result**. `GetByBucket` over a ranged space must union across the spanned ranges. (Hash and
`{db}/meta` buckets stay single-partition → keep the existing single-leader path untouched.)
**Goal:** an optimal multi-range union that touches only the partitions holding the data.
- **Prefix → key interval.** Treat prefix `P` as `[P, P⁺)` where `P⁺` is `P`'s ordinal upper bound.
- **Targeted resolution.** `RangeMap` returns only descriptors whose `[StartKey, EndKey)`
  *intersects* `[P, P⁺)` — O(spanned ranges) targets, **not** `ScanAllByPrefix`'s blind O(nodes)
  broadcast and **not** all ranges.
- **Single-range fast path.** If `P` resolves to exactly one range, short-circuit to today's
  single-leader path — pay no coordination cost when there's nothing to coordinate.
- **Coalesce by leader.** Group target ranges by `PartitionId`, then by leader endpoint; issue **one
  RPC per leader** covering its sub-ranges (reuse `GrpcServerBatcher`), not one RPC per range.
- **Clip each scan.** Each partition scans only `[max(StartKey, P), min(EndKey, P⁺))` via the
  bounded `GetByRange` path (Task 5/10) — never the legacy brute-force-all-local-actors engine, and
  never keys outside the prefix.
- **Bounded parallel fan-out + k-way ordered merge.** `Task.WhenAll` across leaders capped by a
  config semaphore; each range returns key-sorted, so the union is a cheap merge of sorted streams
  (stream as they arrive; no global sort).
- **Page, don't materialize.** Route ranged `GetByBucket` through the Task-10 cursor stitch
  (`(rangeGeneration, lastKey)`); a split mid-scan retries **only the affected sub-range**, not the
  whole bucket. Reserve whole-bucket materialization for callers that explicitly demand it.
**Done when** (`dotnet test --filter TestGetByBucketMultiRange` green):
- [x] `Bucket_SpanningMultipleRanges_ReturnsCompleteOrderedSet` — a bucket split across ≥2 ranges
  returns every key, ordered, equal to the pre-split baseline (no partial result).
- [x] `Bucket_SingleRange_UsesFastPath` — a prefix within one range issues one single-leader call,
  no scatter-gather.
- [x] `Bucket_HashAndSchemaLog_KeepSingleLeaderPath` — hash-routed buckets still go through the
  existing single-leader path, unchanged.
- [ ] `Bucket_SplitMidScan` — **covered by reasoning, no test.** The map is snapshotted once at
  call start; a split landing between two descriptor queries means the stale source partition is
  queried. Safe because Task 6 orphan-retains `[K,E)` on the source; MVCC resolves correctly from
  there. Equivalent to `LocateAndGetByRange` Finding 3 (annotated at `KeyValueLocator.cs`).
  A precise test requires mock-transport injection between descriptor queries; deferred.
- ~~`Bucket_QueriesOnlySpannedPartitions`~~ — **removed.** A whole-bucket scan legitimately spans
  every range in the key space by definition; there is no "subset of partitions" to assert against.
  The targeting optimisation (intersect-only) matters for `ScanAllByPrefix`'s blind all-nodes
  broadcast, not for a bucket that covers every range.

### Task 11 — Per-range locks replace table-wide prefix lock ✅
**Depends on:** Task 9. **Files:** `KeyValueLocator.cs` range-lock routing,
`TryAcquireExclusiveRangeLockHandler.cs`; tests extend `Kahuna.Tests/Server/TestLocks.cs`.
**Goal:** a range lock contends only within the one range's partition (design §8, the core payoff).
- `LocateAndTryAcquireExclusiveRangeLock` fans out to each intersecting descriptor's partition
  leader with clamped bounds. Removed `IsPrefixOpSafe` guard that previously returned `Errored`
  for split keyspaces. Added `ClampRange` helper and private `AcquireRangeLockOnPartition` /
  `ReleaseRangeLockOnPartition` helpers. Multi-descriptor acquire rolls back on partial failure.
**Done when** (`dotnet test --filter TestLocks` green, extending the existing file):
- [x] `RangeLock_DisjointRange_NotBlocked` — holding an exclusive lock on `[A,B)` lets a concurrent
  read/write on `[B,C)` proceed without `MustRetry` (the core payoff).
- [x] `RangeLock_SameRange_StillContends` — a conflicting op on `[A,B)` is correctly blocked
  (no over-narrowing of the lock).
- [x] `RangeLock_ScopedToTouchedRanges` — a scan touching two ranges acquires exactly those two
  descriptors' locks, not the whole prefix.

---

## Phase E — Deferred (track, do not start without a follow-up spec)

- **Task 12 — Load-based split** (hot range, not just size) — design §5.1, §10. Needs the load
  signal; design §8 monotonic-append knob (hash-shard a hot insert prefix).
- **Task 13 — Rebalance** (move a range's *replicas* across nodes) — design §6, §7.2. **Blocked:**
  Kommander exposes only cluster-wide `JoinCluster()`; per-group `AddServer`/`RemoveServer` must
  land upstream first (flag per design §7 upstream-fix rule).
- **Task 14 — Meta-range sharding** when the single meta partition is hot — design §4, §10.
- **SQL/serializability layer** (SSI on per-range locks) — explicitly a separate spec (design §2).

---

## Global acceptance gate (run after every Phase B/C/D task)

This is a **hard gate**, not advice: after completing any task from Task 5 onward, the agent must
run the gate and only mark the task done if all of it is green. Each item maps to a regression test
that must exist and pass on **every** subsequent task — a later task that breaks an earlier
invariant fails the gate even if its own `Done when` boxes are checked.

Gate command: `dotnet test Kahuna.sln --filter "TestRangeMap|TestGenerationFence|TestRangeSplit|TestRangeMerge|TestKeySpaceRouting"`

- [ ] **G1 — No gap / no overlap.** `RangeMap.Validate()` holds at every observable step; the
  cutover meta-txn is the only descriptor writer. (Origin: Tasks 1, 6, 8.)
- [ ] **G2 — Fence, never double-apply.** Every stale route returns `MustRetry` and retries to the
  correct partition; the operation applies exactly once. (Origin: Task 4; re-asserted by 6, 8, 10.)
- [ ] **G3 — Quiesce stays range-scoped & short.** The split latch covers only `[K,E)`, never a
  whole prefix/table, and is released at cutover. (Origin: Task 6, design §10.)
- [ ] **G4 — `{db}/meta` never range-split.** The registry rejects ranging the schema log; meta
  routing stays `GetPrefixPartitionKey`. (Origin: Task 9, design §8.)
- [ ] **G5 — Ordinal compares only.** No culture-sensitive compare on any range bound, split key, or
  lookup (one silently corrupts ordering). Enforce with a test asserting `"r/9" > "r/10"` ordinally
  on each new comparison path.

> If a task cannot satisfy the gate without weakening an invariant, **stop and flag it** rather than
> relaxing G1–G5 — these are the design's §10 risks and are non-negotiable.
