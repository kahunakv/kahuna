
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
}