# Sequencer

Sequencer implements distributed numeric sequences.

Sequences are currently backed by the key-value subsystem instead of a separate actor type. Each sequence is stored under an internal key:

```text
__kahuna:sequences:{name}
```

Allocation uses key-value compare-and-set semantics:

- create uses `SetIfNotExists`,
- next/reserve reads the current sequence state and writes it back with `SetIfEqualToRevision`,
- optional idempotency keys are stored in the sequence state to make retries return the same allocation.

This design reuses the key-value actor, Raft replication, and persistence path as the single source of truth. If sequence throughput becomes a bottleneck, a dedicated sequence actor can be introduced behind the same `IKahuna` sequence methods.
