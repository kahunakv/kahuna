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
