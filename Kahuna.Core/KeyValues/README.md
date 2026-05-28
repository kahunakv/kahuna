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
