
namespace Kahuna.Shared.KeyValue;

public enum KeyValueRequestType
{
    TrySet,
    TryExtend,
    TryDelete,
    TryGet,
    TryAcquireExclusiveLock,
    TryReleaseExclusiveLock,
    TryPrepareMutations,
    TryCommitMutations
}