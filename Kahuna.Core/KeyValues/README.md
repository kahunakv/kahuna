# KeyValues

KeyValues implements Kahuna's key-value store, including conditional writes, revisions, expiration, prefix scans, pessimistic locks for transactions, and script-driven transaction execution.

The normal operation path is:

1. `KahunaManager` delegates to `KeyValuesManager`.
2. `KeyValueLocator` redirects to the Raft leader for the key's partition when the local node is not responsible.
3. `KeyValueActor` owns key state for a consistent-hash shard.
4. Persistent mutations are represented as proposals, replicated through Raft, and completed back on the owning actor.
5. `BackgroundWriterActor` persists committed state asynchronously.

Important subfolders:

- `Handlers`: operation-specific actor logic.
- `Transactions`: transaction coordinator and script command execution.
- `Data`: internal key-value state records.
- `Logging`: source-generated logger extensions.
- `Operators`: helper operators used by transaction/script execution.

Keep request routing in `KeyValueLocator`, state transitions in handlers, and cross-operation orchestration in `KeyValuesManager`.

## Scan APIs and result limits

There are two families of multi-key reads, with different scaling characteristics:

- **Paginated range scan** — `GetByRange` / `LocateAndScanRange`. Streams results page-by-page
  through an opaque cursor, lazily walking the in-memory B-tree and k-way-merging against disk
  one bounded page at a time. This is the path to use for unbounded or large result sets (e.g.
  full table/index iteration). It has no fixed ceiling — keep paging until `HasMore` is false.

- **Non-paginated prefix/bucket scan** — `GetByBucket` / `ScanByPrefix` / `ScanByPrefixFromDisk`.
  These materialize the entire matching set in a single call, so they are bounded by a hard cap
  of **`KeyValueScanLimits.MaxPrefixScanResults` (4096)** entries. Results beyond the cap are
  silently truncated at both the disk backend (`GetKeyValueByPrefix`) and the in-memory
  accumulation in the handlers. They are intended for small, known-bounded prefixes only.

  If a prefix may contain more than 4096 entries, do **not** rely on these APIs to return the
  full set — use the paginated range scan instead.

## In-memory cache, budgets, and eviction

Each `KeyValueActor` keeps its shard's hot keys in an in-memory `BTree<string, KeyValueEntry>`.
For `Persistent` keys this is a write-through cache over the persistence backend; for `Ephemeral`
keys it is the only copy. Memory is capped per actor by `MaxEntriesPerActor` and `MaxBytesPerActor`,
and reclaimed by the collector (`TryCollectHandler`), which runs every `CollectThreshold` operations
but only while the actor is over budget, doing at most `CollectBatchMax` evictions per cycle.

The collector never scans the whole store. It works from three structures maintained on
`KeyValueContext` as keys mutate:

- **Tombstone queue** — a FIFO of keys whose entry turned `Deleted`/`Undefined`. Drained first.
- **Expiry min-heap** — `(Expires, key)` ordered by deadline. The collector pops only entries whose
  TTL has elapsed and stops at the first still-live one. Stale heap nodes (after an `Extend`, or a
  re-set) are discarded on pop via re-validation.
- **Intrusive LRU list** — a doubly-linked list threaded through `KeyValueEntry` (`LruPrev`/`LruNext`)
  with head = coldest, tail = hottest. `TouchEntry` moves an entry to the tail on access; eviction
  pops from the head. This is exact O(1)-per-access recency, not sampling.

Two safety rules gate every eviction path:

- **Intent-held entries are never evicted** (`WriteIntent`/`ReplicationIntent` in flight).
- **Unflushed entries are never evicted** — `KeyValueEntry.IsDirty` returns true while a committed
  revision may not yet be on disk (`Revision > FlushedRevision` within a flush safety window). This
  prevents a disk-fallback read from returning a stale revision after eviction.

Maintenance invariants for anyone editing this code:

- Every write that updates recency must go through `context.TouchEntry`, never a raw
  `entry.LastUsed =` assignment (except the pre-insert disk-load initializer). `TouchEntry` is a
  no-op for entries not in the store (`StoreKey == null`), so scan reads that resolve from disk
  without caching (`populateCache: false`) must not be assumed to be linked.
- Revision history is trimmed to `RevisionRetention` at archive time, and expired sibling MVCC
  entries are trimmed at transaction resolve time (commit / rollback / lock release). The collector
  does **not** trim metadata — these inline trims are the only enforcement, so any new
  archive/resolve path must call them.

For a conceptual, beginner-friendly walkthrough of all of this, see
[`docs/keyvalue-caching-and-eviction-guide.md`](../../docs/keyvalue-caching-and-eviction-guide.md).
