# Upstream flag (Kommander): system-data replication for the range-descriptor map

> **Raised by:** Task 2 of [`spec-range-splits-tasks.md`](spec-range-splits-tasks.md).
> **Rule invoked:** §0 of the task plan — *"No new Kommander workarounds. If a primitive is
> missing, stop and flag it for upstream rather than hacking around it in Kahuna."*
> **Kommander version inspected:** `0.10.14` (source at `/Users/andresgutierrez/kommander`).
> **Status: ✅ RESOLVED — no Kommander change needed.** The flag assumed the meta map had to live on
> the Kommander *system* partition (id `0`). It does not: host it on **Kahuna partition `1`**, an
> ordinary replicated partition whose logs reach `OnLogRestored`/`OnReplicationReceived`. Data
> ranges then live on partitions `≥ 2`. Task 2 proceeds with that approach; this document is kept
> only as a record of why partition `0` is unusable for application data (and as the standing ask if
> we ever *do* want app payloads co-located on the system partition).

---

## Original analysis (retained for the record)

---

## What the design asks for

Spec §4 / §7.3 and Task 2 require a **consistent, Raft-replicated home for the range-descriptor
map** — "store descriptors in a system key space that is itself Raft-replicated (start with a
single well-known *meta* partition; shard the meta later)." Task 2 names
`RaftSystemConfig.SystemPartition` as that home and says to replicate descriptor entries through it
with `ReplicateLogs`, rebuilding the in-memory `RangeMap` from the log on startup / `OnLogRestored`.

## Why it cannot be implemented today

The Kommander system partition (`RaftSystemConfig.SystemPartition == 0`) is **reserved for the
partition-map coordinator** and rejects all other payloads:

1. **Restore/replication of system-partition logs bypass Kahuna entirely.** When
   `PartitionId == SystemPartition`, committed and restored logs are dispatched to the *system*
   callbacks, not the user ones:
   - `RaftPartitionStateMachine.cs:1664` → `InvokeSystemReplicationReceived` (not
     `InvokeReplicationReceived`).
   - `RaftWriteAhead.cs:171` → `InvokeSystemLogRestored` (not `InvokeLogRestored`).
   Kahuna only subscribes to `OnLogRestored` / `OnReplicationReceived`
   (`EmbeddedKahunaNode` wiring), so it never observes anything on partition 0.

2. **The system callbacks hard-reject non-`_RaftSystem` types.**
   `RaftManager.SystemReplicationReceived` (`:330`) and `SystemLogRestored` (`:316`) both do:
   ```csharp
   if (log.LogType != RaftSystemConfig.RaftLogType /* "_RaftSystem" */ || log.LogData is null)
   {
       Logger.LogError("Invalid log type: {LogType} in system partition", log.LogType);
       return Task.FromResult(true); // dropped
   }
   ```
   So `ReplicateLogs(0, "rangemap", …)` is silently discarded.

3. **No public system-data API exists.** `IRaft` exposes only `ReplicateLogs(partitionId, …)` for
   user partitions plus the lifecycle/coordinator methods (`CreatePartitionAsync`,
   `RemovePartitionAsync`, `Split/MergePartitionsAsync`, `GetPartitionGeneration`). There is no
   `ReplicateSystemLogs` or equivalent for attaching application payloads to the system partition.

Net: there is no supported way to make the system partition the replicated source of truth for
Kahuna's descriptor map.

## What Kommander needs to expose (proposed)

One of the following, in rough order of preference:

- **(A) System-partition app payloads.** A `ReplicateSystemLogs(type, data)` (leader-only on
  partition 0) that co-locates application log types alongside `_RaftSystem` on the system
  partition, and surfaces their commit/restore via the existing `OnReplicationReceived` /
  `OnLogRestored` events (or dedicated app-system events). This matches the design's literal intent
  of hosting the meta map on the consistent system partition.

- **(B) A first-class reserved meta partition.** A blessed, non-hash-routed user partition
  (created at join, fenced from split/merge like the system partition) that Kahuna may
  `ReplicateLogs` into through the normal user path. Effectively §7.3's "reuse a reserved
  partition," but guaranteed by Kommander rather than improvised by Kahuna.

Either lets Kahuna keep `RangeMap.Mutate(...)` as the single serialized writer (§5.4) and rebuild
the map from the log on restore/failover — satisfying Task 2's three acceptance tests
(`Descriptor_SurvivesRestart`, `Descriptor_VisibleAfterFailover`,
`Mutate_ConcurrentAttemptsSerialize`) without a Kahuna-side workaround.

## Interim Kahuna-side option (explicitly NOT taken)

Kahuna *could* designate an ordinary user partition as the meta partition and `CreatePartitionAsync`
it at startup. This was considered and **declined** for now: it is exactly the kind of
"reserve/improvise around a missing primitive" that §0 says to flag rather than build. If the
upstream fix is deferred, revisit this as a deliberate, separately-approved interim — not a silent
workaround.

---

## Open upstream item 2 — `Unrouted` initial partitions (Task 3 decision)

> **Raised by:** the Task 3 routing decision (*Kahuna owns key→partition assignment; Kommander
> partitions are `Unrouted`*). **Status: open, non-blocking.** Kahuna already routes correctly
> without it (see below); this is the clean-up that makes Kommander's own model consistent.

**Decision.** Hash-routed key spaces are now assigned by Kahuna's `Ranges/DataPartitionRouter`
(hash over data partitions `[2, InitialPartitions]`), not `IRaft.GetPartitionKey`. This enforces the
reservation contract in code: the system partition (0) and meta partition (1) are structurally
unreachable for data.

**Why an upstream item.** Kommander's `RaftSystemCoordinator.DivideIntoRanges` hardcodes the initial
partitions to `RaftRoutingMode.HashRange` (each owns an int-space slice). Once Kahuna owns
assignment, those `HashRange` slices are dead metadata that nothing should consult — but Kommander's
split/merge planner still keys off `RoutingMode == HashRange`, so a stray Kommander-driven hash
bisection could split a partition Kahuna is actively routing (worst case: the meta partition).

**Ask.** A way to bring up the initial partitions as `Unrouted` — e.g. a `DivideIntoRanges` that
emits `Unrouted` partitions (or an `InitialRoutingMode` config), with partition 1 reserved for the
meta map. Then Kommander hosts groups and fences generations (design §7.4) and never routes or
auto-splits data partitions on its own.

**Not blocking because** `DataPartitionRouter` ignores Kommander's `RoutingMode` entirely — it maps
onto `2..InitialPartitions` from config — and Kahuna never *triggers* Kommander hash split/merge for
data (key-range splits go through `CreatePartitionAsync(Unrouted)` per design §7). So routing is
already correct; leaving the initial partitions `HashRange` is stale-but-harmless until this lands.
The live `GetPartitionKey` call sites (53 of them) are switched to Kahuna routing in Task 9.
