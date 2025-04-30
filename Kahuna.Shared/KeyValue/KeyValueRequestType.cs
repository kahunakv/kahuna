
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
    TryReleaseExclusiveLock,
    TryReleaseExclusivePrefixLock,
    TryPrepareMutations,
    TryCommitMutations,
    TryRollbackMutations,
    ScanByPrefix,
    ScanByPrefixFromDisk,
    GetByPrefix
}