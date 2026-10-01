# Embedding

Embedding provides the in-process API for hosting Kahuna without running the external server executable.

`EmbeddedKahunaNode` wires together:

- an actor system,
- a local Raft instance,
- `KahunaManager`,
- in-memory inter-node communication for embedded clusters.

Use this component for tests, local tools, and applications that need a Kahuna node inside their own process. External HTTP/gRPC hosting is intentionally not handled here.

The standalone constructor `EmbeddedKahunaNode(options)` uses in-process phantom witnesses. They
acknowledge Raft traffic but persist no replicas; check-quorum and backfill are disabled for this
constructor. The cluster constructor accepts caller-supplied transports and discovery and retains
normal cluster quorum checks. `EmbeddedKahunaCluster.CreateInMemoryAsync` builds real in-process members
with memory backend/WAL; a restarted member catches up from its peers.

Embedded nodes with a SQLite/RocksDB backend and `StoragePath` default received-snapshot staging
to a private directory under that path.
Memory-only nodes stage in memory unless a directory is supplied. See the
[snapshot and Raft recovery guide](../../docs/snapshot-and-raft-recovery-guide.md) for the receive caps,
deadlines and per-member directory rules. Settlement options are described in the
[durable settlement guide](../../docs/durable-settlement-guide.md); they are independent of storage choice.
