# Kahuna backend I/O scheduler guide

This guide explains how Kahuna runs its persistence-backend I/O (RocksDB / SQLite reads, scans, and
background batch writes) on its **own** dedicated thread pools, separate from Kommander's WAL read
pool, and how to size those pools. It is written for two audiences:

- **Developers operating Kahuna** who want to tune I/O concurrency and understand how it interacts
  with Kommander's WAL I/O.
- **Developers maintaining Kahuna** who need the ownership, lifecycle, and back-pressure invariants
  in one place.

---

## Why dedicated pools

Synchronous backend calls (a point `GetKeyValue`, a prefix/range/bucket scan page, a batched
`StoreKeyValues` flush) must not run on Nixie actor threads or the .NET thread pool — they would
block an actor or starve unrelated work. Kahuna therefore runs them on a fair, partition-aware
scheduler (`FairReadScheduler`).

Historically Kahuna borrowed **Kommander's** scheduler (`raft.ReadScheduler`) for this. That coupled
the data plane to the consensus control plane:

- Background batch writes were enqueued on meta partition 0's queue, head-of-line-blocking the range
  map / snapshot-floor WAL reads behind bulk flushes.
- User-facing disk reads competed with the WAL reads that replication, leader catch-up, and recovery
  depend on — a scan storm could slow Raft down.
- The per-partition depth limit and throw-on-full back-pressure budget were shared, so heavy Kahuna
  traffic could push Kommander's own WAL reads into rejection, and vice versa.
- The scheduler stopped with `RaftManager`, so in-flight backend I/O during shutdown could fault
  instead of draining on Kahuna's terms.

Kahuna now owns **two** dedicated `FairReadScheduler` instances, and Kommander's instance is WAL-only.

## The two pools

| Pool | Serves | Knob (threads) | Default |
|---|---|---|---|
| **Backend read** | Every persistence-backend *read*: point gets, `TryExists`, the read-before-write of a set, exclusive-lock loads, and prefix/range/bucket scan pages. Partition routing (`ResolvePartition` / the data-partition router) is unchanged. | `BackendReadIOThreads` | 8 |
| **Backend writer** | `BackgroundWriterActor` batch writes — `StoreKeyValues`, `StoreLocks`, and revision pruning. | `BackendWriteIOThreads` | 2 |

Both pools share one back-pressure knob:

| Knob | Meaning | Default |
|---|---|---|
| `BackendReadQueueDepth` | Per-partition pending-queue depth before an enqueue is rejected with back-pressure. | 4096 |

### Writer keying

The writer pool is a separate instance, so background writes no longer collide with meta-partition
WAL reads regardless of how they are keyed. Batches aggregate keys across many partitions into one
backend call, so there is no single "real" partition to key on; every batch is enqueued under one
stable writer key. Read-your-flush correctness comes from the `FlushedRevision` ack the writer sends
back to the owning actor after a durable store — **not** from queue ordering — so a single serialized
writer queue is both correct and simplest. Do not introduce a dependency on cross-queue ordering.

## Configuration surfaces

The knobs are exposed identically on all three configuration surfaces:

- **`KahunaConfiguration`** — `BackendReadIOThreads`, `BackendWriteIOThreads`, `BackendReadQueueDepth`.
- **`EmbeddedKahunaOptions`** — same three properties.
- **Server command line** — `--backend-read-io-threads`, `--backend-write-io-threads`,
  `--backend-read-queue-depth`.

A value of `0` or negative for a thread count auto-sizes to the processor count.

## Sizing and the thread budget

The goal is to keep total per-node dedicated threads roughly flat while giving the data plane its own
concurrency:

- **Kommander WAL read pool** (`ReadIOThreads`, `--read-io-threads`) now serves only WAL reads (log
  catch-up, `GetMaxLog`, term reads, compaction). Its Kahuna-side default was lowered to **4** once
  backend reads moved off it.
- **Backend read pool** (`BackendReadIOThreads`) carries the data-plane scan/point-read load; the
  default of **8** sizes it for scan concurrency.
- **Backend writer pool** (`BackendWriteIOThreads`) stays small (**2**) — backend writes are
  fsync-heavy and serialize on the backend anyway.
- Kommander's WAL **write** pool (`WriteIOThreads`) and WAL group commit are unchanged by this
  feature.

**Test harnesses:** keep all of these tiny. Parallel embedded clusters multiply per-node dedicated
threads; `BaseCluster` sets the backend pools to 1 thread each.

## Back-pressure and shutdown behavior

`FairReadScheduler.EnqueueTask` throws synchronously in two cases; every migrated call site handles
both:

- **`ReadBackpressureExceededException`** (per-partition queue at its depth limit). Reads map this to
  a retriable `MustRetry` outcome rather than faulting the actor: scan handlers catch it locally, and
  the actor's message boundary maps it for the synchronous point-read paths. The writer pool feeds a
  rejection into its existing decorrelated-jitter retry/back-off loop, keeping the batch queued
  (never dropped).
- **`InvalidOperationException`** (scheduler stopping). This surfaces as a deterministic fault of the
  in-flight request (its awaiter completes with an error), never a hang.

**Lifecycle:** the schedulers are owned by `KahunaManager`. They start when it is constructed and are
stopped and disposed in `KahunaManager.Dispose()`, which runs **after** the actor system has drained
in both the embedded and server hosts. Stopping after the drain means in-flight backend I/O completes
rather than faulting on a scheduler that Raft teardown already stopped — the ordering hazard that
existed while the pools were shared with Kommander.

## Invariants for maintainers

- No production code path enqueues Kahuna persistence-backend I/O on `raft.ReadScheduler`. A
  tree-wide grep for `ReadScheduler.EnqueueTask` should show only the Kahuna backend schedulers.
- The `FlushedRevision` ack is the sole source of read-your-flush correctness. Do not make any read
  path depend on writer-queue ordering.
- Keep the two pools sized so the total per-node dedicated thread count stays roughly flat against
  Kommander's `ReadIOThreads` / `WriteIOThreads`.
