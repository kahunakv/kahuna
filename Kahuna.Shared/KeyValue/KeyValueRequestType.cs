
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Specifies the type of operations that can be performed on key-value storage.
/// These operations include setting, extending, deleting, retrieving, and managing
/// keys and their associated values, as well as handling transactional and concurrency mechanisms.
/// </summary>
public enum KeyValueRequestType
{
    TrySet,
    TryExtend,
    TryDelete,
    TryGet,
    TryExists,
    TryAcquireExclusiveLock,
    TryAcquireExclusivePrefixLock,
    TryAcquireExclusiveRangeLock,
    TryReleaseExclusiveLock,
    TryReleaseExclusivePrefixLock,
    TryReleaseExclusiveRangeLock,
    TryPrepareMutations,
    TryCommitMutations,
    TryRollbackMutations,
    ScanByPrefix,
    ScanByPrefixFromDisk,
    GetByBucket,
    GetByRange,
    CompleteProposal,
    ReleaseProposal,
    Collect,
    TryCheckWriteIntent,
    GetRangeLocks,
    ImportRangeLocks,
    GetSafeTimestamp,
    ResumeRead,
    InvalidateOrApply,
    FlushAck,

    /// <summary>
    /// Actor-internal maintenance message: removes every resident entry owned by the partition
    /// carried in the request, after this node stopped being one of its replicas. Never sent by
    /// clients and never serialized into the Raft log.
    /// </summary>
    EvictPartition,

    /// <summary>
    /// Drops the belief-only state an actor holds for a partition this node stopped leading: staged
    /// transactional entries with their write intents, and exclusive prefix and range locks. Committed
    /// entries stay resident. Sent to every shard when Raft reports the leadership lost.
    /// </summary>
    DropLeaderState,

    /// <summary>
    /// A committed durable-transaction mutation replicated BY REFERENCE: the record names the prepared
    /// intent (transaction id, epoch, key) whose value every replica already holds, and carries no value
    /// bytes of its own. Consumers resolve the value from their own prepared-intent store and apply it
    /// exactly as the value-carrying form would.
    ///
    /// <para>Never sent by clients. Appended last on purpose: the numeric value travels in Raft logs and
    /// WAL segments, so no existing member may be renumbered.</para>
    /// </summary>
    MaterializeIntent,

    /// <summary>
    /// Prepare, commit-time range-lock check, and commit of one ephemeral mutation in a single actor turn.
    /// Sent only to the local actor that owns the key, by the coordinator of a transaction whose whole write
    /// set is that one key. Never replicated and never sent between nodes.
    /// </summary>
    TryFinalizeMutation,

    /// <summary>
    /// Runs a unit of work inside one turn of the actor that owns a key, so the requests that work issues for
    /// the key are served without a mailbox hop each. Local to a node; never replicated, never sent between nodes.
    /// </summary>
    RunActorTurn,
}