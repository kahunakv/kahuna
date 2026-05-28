# Embedding

Embedding provides the in-process API for hosting Kahuna without running the external server executable.

`EmbeddedKahunaNode` wires together:

- an actor system,
- a local Raft instance,
- `KahunaManager`,
- in-memory inter-node communication for embedded clusters.

Use this component for tests, local tools, and applications that need a Kahuna node inside their own process. External HTTP/gRPC hosting is intentionally not handled here.
