# Replication

Replication contains the serialization boundary between Kahuna core state changes and Kommander/Raft log entries.

Kahuna actors do not write arbitrary objects into Raft. They convert committed lock and key-value changes into compact protobuf messages, tag them with a `ReplicationTypes` value, and serialize them through `ReplicationSerializer`.

On restore, managers inspect Raft log type values and dispatch records to the appropriate restorer:

- lock logs go to `LockRestorer`,
- key-value logs go to `KeyValueRestorer`.

Add new replicated subsystems here only when they need their own Raft log record type.
