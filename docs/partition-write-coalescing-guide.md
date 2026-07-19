# Kahuna partition write-coalescing guide

This guide explains how Kahuna turns many independent direct key/value writes into a small number of
Raft proposals, why that matters, and the invariants a maintainer must preserve. It is written for two
audiences:

- **Developers using Kahuna** who want to understand the latency/throughput trade-off of bulk writes
  and how to tune it.
- **Developers maintaining Kahuna** who need the write lifecycle, batching policy, and correctness
  invariants in one place.

No knowledge of the internals is required to start; the tuning and operations sections stand alone.

---

## 1. The big picture

A "direct write" is a persistent, non-transactional key/value mutation — a `TrySet` (including
conditional and no-revision variants), a `TryDelete`, or a `TryExtend`. Each such write must be
committed to the Raft log of the key's partition before it is acknowledged.

Committing each write on its own is expensive. Every proposal is a WAL append, a quorum round trip,
and — with WAL sync enabled — an `fsync`. A bulk operation that touches hundreds of keys, or hundreds
of clients writing the same partition at once, would otherwise pay that cost per key.

Kahuna avoids this with a **leader-local partition write aggregator**. Instead of proposing each
direct write immediately, the owning node collects writes bound for the same Raft partition and
proposes the collected log records in **one** `ReplicateLogs` call. Writes from different clients,
single-key calls, and bulk (`SetMany`/`DeleteMany`) calls all meet in the same partition queue, so
coalescing is *cross-request*, not limited to one API call.

The result: N direct writes to a partition become a handful of proposals (bounded by batch-size and
byte caps), collapsing WAL appends, quorum round trips, and `fsync`s in the same proportion.

Three things the aggregator deliberately does **not** touch:

- **Ephemeral writes** — they never use Raft; they apply in memory on the owning actor.
- **Transactional writes** — a write under an open transaction stages an MVCC entry; it is not an
  auto-committed Raft record yet.
- **Two-phase commit** — interactive and script transactions propose through their own coordinator
  path with a shared prepare/commit ticket. That traffic is never merged with auto-commit writes,
  because independent transactions need independent commit/rollback outcomes.

---

## 2. The write path, end to end

A single persistent set/delete/extend flows like this:

```
client → gRPC/REST/SDK
  → KahunaManager → KeyValuesManager        (routes to the key's partition leader)
  → KeyValueActor  (owns the key's in-memory state; single-threaded per shard)
       validate → resolve+fence partition → build proposal → serialize record
       → install ReplicationIntent → hand the envelope to the aggregator
  → PartitionWriteAggregator.TryEnqueue      (synchronous admission; reserve or reject)
  → PartitionWriteAggregatorActor (a "lane")  (queue, linger, batch selection)
       → one IRaft.ReplicateLogs(partition, [records], autoCommit:true)   (off the mailbox)
  ← success → CompleteProposal back to the KeyValueActor → applies value, resolves caller
  ← failure → ReleaseProposal back to the KeyValueActor → clears intent, returns MustRetry
```

The key structural point is that the **owning `KeyValueActor` stages the write and installs the
replication intent, but does not perform the Raft call**. It serializes the committed record, hands a
lean envelope to the aggregator, and frees its mailbox. The aggregator performs the Raft round trip
off any actor mailbox and sends the outcome back as a control message.

`CreateProposal` (in `BaseHandler`) is the seam where a persistent write leaves the actor for the
aggregator. It resolves and fences the partition, allocates a proposal id, serializes the record,
installs the `ReplicationIntent`, stores the proposal in the actor's proposal dictionary, and calls
`TryEnqueue`. On synchronous rejection it unwinds the intent and proposal on the same mailbox and
returns `MustRetry`; on acceptance it bypasses the reply and lets completion resolve the caller later.

`SetMany`/`DeleteMany` do not have a private batching path. They fan their items out through the
ordinary `TrySet`/`TryDelete` entry points launched together; the persistent items then meet in the
aggregator exactly like independent single-key writes.

---

## 3. Components

All types live in `Kahuna.Core/KeyValues/Writes/`.

- **`PartitionWriteAggregator`** — the facade owned by `KeyValuesManager`. It owns a fixed set of lane
  actors, a per-partition admission registry, and exposes the synchronous
  `TryEnqueue(envelope) → accepted | retryable-reject`. It selects a lane directly from the partition
  id (modulo lane count), so admission takes no mailbox hop and a rejection can be unwound
  immediately by the calling actor.

- **`PartitionWriteAggregatorActor`** (a "lane") — a single-threaded, reply-capable Nixie actor that
  owns the `PartitionWriteState` for each partition it serves. One lane may serve many partitions;
  because no lane awaits Raft, a slow partition never parks another partition owned by the same lane.

- **`PartitionWriteState`** — the pure, single-threaded FIFO state for one partition: the pending
  buffer, byte totals, the one-in-flight marker, batch selection, queue-age expiry, and the linger
  epoch. It contains no actors, timers, or Raft, so its ordering and batching logic is
  deterministic.

- **`KeyValueProposalRequest`** — the immutable envelope the aggregator carries. It holds the resolved
  partition, the already-serialized log record and its byte length, the proposal id, the routed
  generation, the originating actor, the caller promise, and the enqueue tick — **not** the full
  proposal, which stays in the owning actor's dictionary until completion.

- **`IPartitionBatchExecutor`** / `RaftPartitionBatchExecutor` — the one Raft round trip for a batch,
  behind an interface (replaceable in tests).

- **`IWriteCompletionRouter`** / `KeyValueActorCompletionRouter` — delivers each item's terminal
  outcome by sending the existing `CompleteProposal` / `ReleaseProposal` control request to the
  item's owning key actor.

- **`IWriteRangeFence`** / `RangeMapWriteFence` — re-checks a key-range write's routed generation
  against the live range map at flush time.

- **`PartitionAdmissionRegistry`** — per-partition item/byte reservation accounting: the facade
  reserves on admission, the lane releases as items complete or are released.

- **`PartitionWriteAggregatorMetrics`** — the `System.Diagnostics.Metrics` instruments.

---

## 4. Batching and flush policy

Each partition accumulates writes in a FIFO buffer. A partition's buffer flushes on the first of:

- the pending item count reaches the batch-items cap;
- the pending serialized bytes reach the batch-bytes cap;
- the **oldest** item's linger deadline elapses; or
- an in-flight batch completes and a buffer accumulated behind it.

The linger deadline is measured from the oldest queued item and is **not** reset by new arrivals, so
sustained traffic cannot postpone a flush forever. Linger is modelled by an *epoch* rather than a wall
clock: opening an empty buffer bumps the epoch, the lane arms a timer tagged with that epoch, and a
timer wake only flushes if its epoch is still current and no batch is in flight. A count/byte flush
therefore makes a later, stale timer harmless.

Batch selection walks the buffer in admission order up to the item and byte caps. If the very first
item alone exceeds the byte cap, it is dispatched **alone** — the cap is a batching target, not a
value-size limit. A many-key request may span several batches; its per-item responses already allow
that, and no new atomicity promise is made.

**At most one batch is in flight per partition.** This is not unwanted serialization — the Raft
partition is itself the serialization point, and Kommander otherwise reports/retries a concurrent
proposal. The important property is that *different* partitions can have Raft calls in flight and
complete concurrently. Once an in-flight batch completes, the buffer behind it is re-driven
immediately (a linger timer that fired during the in-flight window was ignored, so completion is the
point that must re-dispatch).

---

## 5. Correctness invariants

A maintainer changing this subsystem must preserve all of these:

1. **The owning key actor stays authoritative.** Only that actor validates a write, installs or clears
   its replication intent, stores or removes its proposal, and applies the confirmed value. The
   aggregator owns queue state only.

2. **No mailbox awaits Raft.** A lane's message handler mutates queue state and returns. A detached
   task owns an immutable batch, calls the executor, catches every exception, and sends a completion
   control message back. Detached tasks never touch lane state.

3. **One in-flight batch per partition.** A later batch for a partition is not sent until its earlier
   batch has a terminal result.

4. **Partitions progress independently.** A partition being in flight or stalled does not stop another
   from flushing, even when both are owned by the same lane.

5. **One compatibility class per batch.** A batch contains only auto-commit `KeyValues` records for a
   single partition. Two-phase-commit tickets and other log types never enter it.

6. **Every accepted item terminates exactly once** — a `CompleteProposal` after confirmed success, or
   a `ReleaseProposal` after a pre-dispatch rejection or a confirmed failure. Completion is delivered
   as a priority control message so a saturated inbox cannot strand it.

7. **The generation fence is checked twice, in the right domain.** A key-range write is fenced against
   the live range map both before its intent is installed (on the actor) and again at flush (on the
   lane); a stale item is released individually and does not fail its valid siblings. This is the
   Kahuna range-descriptor generation, checked in Kahuna — it is *not* passed to Kommander's physical
   `expectedGeneration`, which is a different namespace Kahuna does not synchronize from descriptors;
   that argument is always zero.

8. **Bounds are hard and reservations always release.** Admission and queue limits include items still
   waiting behind an in-flight batch. A limit rejection is retryable and reserves nothing. Every path
   that removes an item — success, transient failure, fence rejection, queue-age expiry, and shutdown
   drain — releases both the caller (complete/release) and the admission reservation.

9. **Queue age is bounded.** An item that waits longer than the configured maximum is released as
   `MustRetry` **before** dispatch (and swept even while a batch is in flight), so it can never be
   proposed after its replication-intent lease could have expired. The queue-delay bound is validated
   to stay comfortably below that lease.

10. **A caller cancellation cannot poison siblings.** Once admitted, an item is not removed from an
    in-flight batch and its caller token does not cancel the shared Raft call. The result is still
    applied or released so no actor state leaks.

11. **Success means local completion.** A caller is resolved only after its key actor processes
    `CompleteProposal`, preserving read-your-writes and background persistence.

12. **Leader change retries through normal routing.** The old leader does not forward staged
    proposals. Leadership loss, a moved partition, restore-in-progress, queue-full, and timeout are
    released as `MustRetry`; the client re-routes and re-stages on the new leader. Because the same
    `KeyValueMessage` records are replicated as for a single-key write, follower apply and cold
    restore reconstruct identical state.

---

## 6. Configuration and tuning

The aggregator is tuned through `KahunaConfiguration` (exposed on the server CLI as `--kv-write-*`
options and on `EmbeddedKahunaOptions` for the embedded/standalone engine):

| Setting | Default | Meaning |
|---|---:|---|
| `KeyValueWriteLingerMs` | `1` | Delay from the oldest queued item before a partition batch is proposed. `0` dispatches an idle partition immediately (still batching work that accumulates behind an in-flight batch). |
| `KeyValueWriteMaxBatchItems` | `512` | Maximum log entries per Raft call. |
| `KeyValueWriteMaxBatchBytes` | `4 MiB` | Target serialized bytes per Raft call; an oversized single item dispatches alone. |
| `KeyValueWriteMaxQueuedItemsPerPartition` | `8192` | Maximum admitted items per partition, including those in flight. |
| `KeyValueWriteMaxQueuedBytesPerPartition` | `32 MiB` | Maximum admitted serialized bytes per partition. |
| `KeyValueWriteMaxQueueDelayMs` | `1000` | Maximum time an admitted write may wait before dispatch; on expiry it is released as `MustRetry`. |
| `MaxKeyValueWriteAggregatorInboxSize` | `16384` | Ordinary-submission inbox bound per lane; control messages are exempt. |

The number of lanes is derived from the key/value worker count; there is no separate knob, because
lane count does not limit Raft concurrency (detached work is per partition).

Validation normalizes non-positive capacities to their defaults, clamps a negative linger to zero,
clamps the batch limits so a batch can never select more than a partition may hold, and **rejects** a
queue-delay that is not comfortably below the write-intent lease.

### Choosing linger

`LingerMs = 0` is the low-latency escape hatch: an uncontended single write dispatches immediately, so
the aggregator adds an extra actor hop but no wait. A small positive linger (the default `1`) lets
concurrent same-partition writes accumulate before dispatch, which is where the fsync/round-trip
saving comes from. There is no benefit to a large linger unless writes arrive faster than the disk can
flush; a rising queue-delay with batch sizes near one usually indicates a routing, leader, or
partition-health problem rather than a linger that is too short.

### Key layout matters

Coalescing is per partition. Keys in one hash key-space route to a single partition, so a bulk insert
whose rows share a key-space collapses into as few batches as the caps allow. Keys spread across many
key-spaces spread across partitions and coalesce less. If you control the schema, grouping a logical
batch under one key-space maximizes the win.

---

## 7. Observability

Instruments are published on the `Kahuna` meter with low-cardinality tags only (a bounded reason or
outcome string — never a key, partition id, or transaction id):

- **Counters** — admitted writes; rejections (tagged `queue_full` / `stopping` / `fence_stale` /
  `queue_expired`); dispatched batches; dispatched log entries; batch outcomes (`success` /
  `transient` / `permanent`).
- **Histograms** — entries per batch, serialized bytes per batch, oldest-item queue age, and Raft-call
  duration.
- **Observable gauges** — queued items, queued serialized bytes, and in-flight partitions.

The primary effectiveness signal is **dispatched entries ÷ dispatched batches**. Under a coalescing
burst it should approach the configured batch cap; a value near one means writes are arriving too
slowly to batch, or are spread across too many partitions.

---

## 8. Failure and lifecycle behavior

- **Transient Raft failure** (leadership churn, a moved partition, proposal timeout, queue full,
  restore in progress) releases every item in the batch as `MustRetry`; the caller retries.
- **Permanent/structural failure** releases as a terminal error.
- **Backpressure**: `TryEnqueue` rejects synchronously in two cases — the partition's queued item/byte
  bound is full, or the lane's inbox is saturated (the lane's fire-and-forget `TrySend` returns false).
  Either way the reservation is released, the calling actor unwinds its just-installed intent, and it
  returns `MustRetry`. Nothing is left pinned.
- **Shutdown**: disposal drains the aggregator observably *before* the lane actor system or Raft is torn
  down. New admissions are rejected, each lane releases its still-pending (not yet dispatched) items as
  `MustRetry` (delivered as a priority control stop a saturated inbox cannot reject), and the drain then
  **awaits** every already-issued batch settling its Raft round trip — so no in-flight completion is dropped
  on a disposed lane. No accepted caller is silently abandoned.

A released write returns `MustRetry` — never a false success and never a silently dropped write. Retrying on
`MustRetry` is **at-least-once**: a released write may in fact have committed (a timeout or an exception around
the round trip does not prove otherwise), and a non-transactional set/delete is *not* idempotent — a retry
bumps the revision again. Callers that need exactly-once must use a transaction or an idempotency key; the
aggregator does not provide exactly-once application on its own.

---

## 9. Maintaining and extending

- The batching and one-in-flight logic lives in the **pure** `PartitionWriteState`; prefer changing it
  there, where it is deterministically testable without actors or timers.
- The Raft round trip is behind `IPartitionBatchExecutor` and completion behind
  `IWriteCompletionRouter`; both are replaceable, so the lane can be exercised with a recording
  executor that counts and gates calls and selects statuses. The manager can be constructed with a
  test decorator over the real executor, letting the real public write entry points be driven while
  the aggregator's Raft calls are observed and gated.
- The fence is behind `IWriteRangeFence`, so the lane can be tested without a live range map.
- If you add a new direct-write kind, route it through `CreateProposal` so it stages an intent,
  serializes one record, and enqueues like set/delete/extend — do not add a second direct Raft path.
  The serializer that builds the committed record is shared; keep it byte-identical to what a
  single-key write produces so followers and cold restore reconstruct the same state.
- Heterogeneous coalescing across log types (locks, receipts, decision records, independent 2PC
  tickets) is intentionally out of scope: the current bulk Raft primitive commits one log type under
  one ticket. Merging independent transactions' tickets would couple their atomic outcomes and is a
  correctness violation.
