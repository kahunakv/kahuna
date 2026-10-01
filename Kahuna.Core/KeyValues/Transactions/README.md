# Key-Value Transactions

The transaction subsystem coordinates multi-step key-value operations and Kahuna script execution.

There are two main modes:

- Script execution: a bare multi-statement script uses an auto-commit transaction; a single command
  can dispatch directly without 2PC.
- Interactive sessions, where a client starts a transaction, performs operations, then commits or rolls back.

The transaction coordinator handles locking mode, mutation tracking, prepare/commit/rollback calls, and error mapping. Command classes under `Commands` translate parsed script AST nodes into key-value manager calls.

This layer should orchestrate behavior; it should not own low-level key state. Actor state transitions belong in `KeyValues/Handlers`.

Persistent transactions use replicated prepared intents and a canonical decision record. Their
in-memory session and pre-prepare staging are not persisted. Latest transactional reads pin the first
committed observation per key; revision advancement can abort a later read/write early, while finalize
still validates dependencies and staging continuity. Deferred settlement is the default, so committed
values can remain visible through intents after commit returns.

See the [coordinator guide](../../../docs/reusable-transaction-coordinator-guide.md) for the public
contract, the [lifecycle guide](../../../docs/transaction-lifecycle-guide.md) for routing and failure
semantics, and the [settlement guide](../../../docs/durable-settlement-guide.md) for log encodings.

For latest-read pins, historical-read fences and transaction lock behavior, see
[transaction reads and locks](../../../docs/transaction-read-and-lock-semantics-guide.md).
