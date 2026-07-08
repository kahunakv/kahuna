# Kahuna shared RocksDB memory guide

This guide explains how Kahuna can make its **two RocksDB instances share one memory budget** instead of
paying for two. It is written for two audiences:

- **Operators** running Kahuna who want to bound and reduce process memory, and need to know how to turn
  the feature on, how to size it, and what to watch.
- **Developers** maintaining the code who need the mental model, the wiring, and the ownership rules that
  must hold.

No prior RocksDB knowledge is assumed. Concepts are introduced as they come up.

The feature is **opt-in and additive**: with it off (the default) Kahuna behaves byte-for-byte as before.
It changes only in-process memory objects — never the on-disk format, WAL semantics, recovery, or wire
behaviour.

---

## 1. The big picture

A Kahuna node that persists to RocksDB runs **two independent RocksDB databases**:

1. The **KV/locks persistence backend** — random point lookups, range scans, revision writes, prune
   sweeps, and checkpoints.
2. Kommander's **Raft write-ahead log (WAL)** — sequential append, tail reads, and truncation on the
   consensus hot path.

Keeping them separate is deliberate: it insulates the consensus log from the data path, so a KV
compaction storm cannot stall Raft appends. We do **not** merge them into one database.

But two separate databases means two separate memory footprints. RocksDB's memory is dominated by two
things:

- the **block cache** — cached SST data blocks, the main *read* memory cost;
- **memtables** — in-RAM write buffers, one set per column family, the main *write* memory cost.

By default the backend keeps a private 256 MiB block cache, and the two databases never share anything:
two block caches, two unbounded memtable footprints, two budgets. On a memory-constrained node that
duplication is pure waste — one bounded budget would serve both.

**Shared RocksDB memory** lets Kahuna create **one** block cache and **one** write-buffer manager (WBM)
and hand the same pair to both databases:

```
   sharing off:   [ backend: cache A + memtables A ]   [ WAL: cache B + memtables B ]
                                                         two independent budgets

   sharing on:    [ backend ]──┐                ┌──[ WAL ]
                               ├── one cache ─────┤
                               └── one WBM ───────┘
                                  one unified budget
```

The two databases keep their own paths, files, and column families. Only the *memory objects* are shared.

---

## 2. What is shared — and what is not

| Resource | Shared? | How |
|---|---|---|
| **Block cache** | ✅ | One cache applied to every column family of both databases. |
| **Memtable budget** | ✅ | One write-buffer manager, cost-charged into that same cache, attached to both databases. |
| **Background thread pools** | already shared | Both databases use RocksDB's process-wide default env — nothing to do. |
| **On-disk data / column families** | ❌ | Each database keeps its own path, files, and CFs. |
| **WAL semantics, recovery, wire format** | ❌ unchanged | The feature only touches in-process memory objects. |

Because the memtable budget is **cost-charged into the block cache**, one number bounds *both* cached
blocks and memtable memory across *both* databases:

```
   ┌───────────────── total budget (block cache) ─────────────────┐
   │  cached SST blocks          │  memtable charge (≤ memtable budget)  │
   └──────────────────────────────────────────────────────────────┘
```

Two deliberate safety choices make this stable under write load:

- The cache is **soft** (not strict-capacity). With memtables charged into it, a hard cap would surface
  as write stalls/errors on *both* databases whenever either flushed. A soft limit is a target, not a
  brick wall.
- The WBM is **non-stalling**. Cross-database flush coupling never turns into a write stall; size the
  budget conservatively instead of relying on stalls for back-pressure.

---

## 3. Turning it on

Sharing applies **only when both databases are RocksDB** — that is, both the storage backend and the WAL
storage are `rocksdb`. If either is `sqlite` or `memory`, there is nothing to share and the feature is a
no-op even if the flag is set.

### Server (command line)

| Flag | Default | Meaning |
|---|---|---|
| `--rocksdb-shared-memory` | off | Enable sharing (requires `--storage rocksdb` **and** `--wal-storage rocksdb`). |
| `--rocksdb-shared-memory-budget-mb` | `320` | Total shared block-cache budget in MiB. The memtable sub-budget lives inside it. |
| `--rocksdb-shared-memtable-budget-mb` | `128` | Memtable sub-budget in MiB, cost-charged into the shared cache. Must be ≤ the total budget. |

Example:

```sh
kahuna-server \
  --storage rocksdb --wal-storage rocksdb \
  --rocksdb-shared-memory \
  --rocksdb-shared-memory-budget-mb 512 \
  --rocksdb-shared-memtable-budget-mb 128
```

### Embedded engine (in-process)

The same knobs exist on `EmbeddedKahunaOptions`:

```csharp
EmbeddedKahunaOptions options = new()
{
    Storage    = "rocksdb",
    WalStorage = "rocksdb",
    RocksDbSharedMemoryEnabled    = true,
    RocksDbSharedMemoryBudgetMb   = 512,
    RocksDbSharedMemtableBudgetMb = 128,
    // ... the rest of your options
};

await using EmbeddedKahunaNode node = new(options);
```

With `RocksDbSharedMemoryEnabled = false` (the default), or with either storage not RocksDB, no bundle is
created and both databases open on their default per-database resources.

---

## 4. Sizing the budget

The **memtable sub-budget** bounds total memtable memory across *every column family of both databases*.
The WAL alone uses 10 column families; the backend uses 2. RocksDB's defaults are generous
(64 MiB × up to 3 buffers per CF on the backend, similar on the WAL), so the *potential* memtable memory
across all those CFs is far larger than any typical sub-budget. When you share a WBM, the sub-budget — not
the per-CF sizes — is what actually caps memtable memory; ordinary write bursts that would exceed it cause
more frequent cross-CF / cross-database flushing.

Practical guidance:

- **Total budget** ≥ the old 256 MiB backend cache plus headroom for the combined memtables. `320` is the
  conservative default; raise it on nodes with memory to spare and hot read sets.
- **Memtable sub-budget** should leave room for both databases' write bursts. `128` is the default. Too
  small ⇒ constant flushing (and write coupling between the two databases); too large ⇒ less memory
  actually reclaimed. It must be ≤ the total budget (Kahuna refuses to start otherwise).
- If write-path coupling ever becomes a problem, the safe fallback is to **share only the cache** (the
  larger, workload-agnostic win) and leave memtables per-database — but the current wiring shares both;
  reach for that fallback only if measurement shows coupling hurting the consensus path.

**This is a memory feature — measure it with memory, not a stopwatch.** Before enabling it in production,
compare, under a representative write load on both the WAL and the KV store:

- process RSS and the RocksDB cache/memtable usage counters (see §6) with sharing **off** vs **on**, to
  confirm the reduction; and
- Raft-commit / WAL-append latency with sharing **off** vs **on** under concurrent KV load, to confirm KV
  memory pressure does not stall the log.

That measurement is why the feature ships **off by default**.

---

## 5. Ownership and lifetime (for developers)

**The composition root owns the shared bundle. The databases only borrow it.** This is the single rule
that must never be broken.

- In **standalone / embedded** mode, `EmbeddedKahunaNode` creates the bundle (when enabled and both
  databases are RocksDB), injects the same instance into the WAL and the persistence backend, and disposes
  it **last** — after both databases are closed.
- In **cluster** mode, `Program.cs` creates the bundle, registers it as a singleton so the DI container
  disposes it after the Raft and Kahuna singletons that borrowed it, and passes it to both the WAL and the
  `KahunaManager` (which forwards it to the backend).

Neither the WAL nor the backend ever disposes the bundle. The native cache and WBM are reference-counted:
each open database holds its own reference, so disposing the bundle early does **not** free memory out from
under a live database (no use-after-free) — it is only a *budget-accounting* error, not a crash. Still,
always dispose it last.

The wiring is a nullable, optional constructor parameter end to end
(`RocksDbPersistenceBackend`, `KahunaManager`, `RocksDbWAL`). When it is `null`, every database follows its
byte-for-byte default open path — no shared cache, default buffers, default column-family options. That
null path is the default and must stay sacred.

---

## 6. Observability

To confirm sharing is real (one budget, not two), watch these counters while either database writes:

| Signal | Meaning | When it moves |
|---|---|---|
| WBM memtable memory usage | Live memtable bytes tracked by the shared write-buffer manager. | Rises on **writes** from *either* database — the primary proof that the budget is shared. |
| Block-cache usage | Current block-cache occupancy. | Grows on **reads** (cached blocks) and via the WBM's cost-to-cache charge. Don't rely on it alone for a write-only workload. |

The decisive check: with sharing on, writing to **only** the KV/locks backend still raises the shared
write-buffer manager's memtable usage — proving the backend's memtables are charged to the same budget as
the WAL's, rather than to a private per-database one.

---

## 7. Caveats

- **Both sides must be RocksDB.** With a SQLite or in-memory WAL or backend, there is nothing to share and
  the feature is a no-op; the flag is silently ignored in that configuration.
- **Workload coupling.** A shared WBM means a write burst on one database can trigger flushes affecting the
  other. This is acceptable and bounded, but it is the reason to measure the consensus path under load
  before enabling (see §4).
- **Never a strict-capacity cache.** With memtables charged into the cache, a hard cap would surface as
  write stalls on both databases. Kahuna always uses a soft cache and a non-stalling WBM here.
- **Opt-in and reversible.** Turn it off and restart, and both databases return to independent resources
  with no on-disk change.

---

*For the internals of how Kommander's WAL applies the shared bundle, see Kommander's own
`shared-rocksdb-memory-developer-guide.md`. For the WAL write and durability path, see the
[MVCC snapshot floor guide](mvcc-snapshot-floor-guide.md) and related WAL documentation.*
