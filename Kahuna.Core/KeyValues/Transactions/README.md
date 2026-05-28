# Key-Value Transactions

Transactions coordinates multi-step key-value operations and Kahuna script execution.

There are two main modes:

- Script execution, where a parsed Kahuna script runs as a transaction-like unit.
- Interactive sessions, where a client starts a transaction, performs operations, then commits or rolls back.

The transaction coordinator handles locking mode, mutation tracking, prepare/commit/rollback calls, and error mapping. Command classes under `Commands` translate parsed script AST nodes into key-value manager calls.

This layer should orchestrate behavior; it should not own low-level key state. Actor state transitions belong in `KeyValues/Handlers`.
