
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
    /// A committed durable-transaction mutation replicated BY REFERENCE: the record names the prepared
    /// intent (transaction id, epoch, key) whose value every replica already holds, and carries no value
    /// bytes of its own. Consumers resolve the value from their own prepared-intent store and apply it
    /// exactly as the value-carrying form would.
    ///
    /// <para>Never sent by clients. Appended last on purpose: the numeric value travels in Raft logs and
    /// WAL segments, so no existing member may be renumbered.</para>
    /// </summary>
    MaterializeIntent,
}