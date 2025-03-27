
namespace Kahuna.Shared.KeyValue;

public enum KeyValueRequestType
{
    TrySet,
    TryExtend,
    TryDelete,
    TryGet,
    TryExists,
    TryAcquireExclusiveLock,
    TryReleaseExclusiveLock,
    TryPrepareMutations,
    TryCommitMutations,
    TryRollbackMutations,
    ScanByPrefix
}