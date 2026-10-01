# Replication

Replication defines log type tags and protobuf serialization shared by Kahuna's producers, live
consumer apply and restore paths. Lock and key/value records are only part of the log: range-map and
snapshot-floor deltas, canonical transaction records, prepared-intent deltas and completion-receipt
handoffs have their own types and stores.

Persistent direct key/value mutations and durable transaction submissions pass through the partition
write aggregator. Durable store transitions apply in ordered Raft consumer callbacks on leaders and
followers; producer completion waits for that apply instead of mutating replicated stores independently.
Restart dispatches records to their corresponding restorers/stores.

Key/value materialization records can carry values or reference durable prepared intents. An opt-in
materializing settlement installs values during prepared-intent resolve apply instead. Unknown encodings
must not be produced until every replica can apply them. See the
[durable settlement guide](../../docs/durable-settlement-guide.md) for compatibility and replay rules,
and the [snapshot guide](../../docs/snapshot-and-raft-recovery-guide.md) for whole-partition seeding.
