# Locks

Locks implements Kahuna's distributed lock subsystem.

The typical path is:

1. `KahunaManager` receives an `IKahuna` lock call.
2. `LockManager` validates and routes the operation.
3. `LockLocator` forwards to the Raft leader for the lock's partition when needed.
4. `LockActor` owns the in-memory lock state for its consistent-hash shard.
5. Persistent changes are proposed through `LockProposalActor`, replicated through Raft, and then completed by the actor.

There are separate routers for ephemeral and persistent locks. Ephemeral locks are local actor state; persistent locks are replicated and restored through Raft/persistence.

The `Data` folder contains internal state models. The `Logging` folder contains source-generated logger extensions for hot paths.
