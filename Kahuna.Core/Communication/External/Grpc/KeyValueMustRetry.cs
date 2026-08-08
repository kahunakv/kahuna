
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Typed "no definitive answer was produced; retry to resolve it" responses for the key-value gRPC
/// surface, one factory per response message.
///
/// <para>Multi-key responses carry no message-level outcome — the type lives on each item — so their
/// refusal echoes one refused item per requested key. Returning an empty item list instead would read
/// to the caller as "none of these keys produced a result", which is a definitive answer the server
/// never actually reached.</para>
/// </summary>
internal static class KeyValueMustRetry
{
    private const GrpcKeyValueResponseType Type = (GrpcKeyValueResponseType)KeyValueResponseType.MustRetry;

    public static GrpcTrySetKeyValueResponse TrySetKeyValue() => new() { Type = Type };

    public static GrpcTryExtendKeyValueResponse TryExtendKeyValue() => new() { Type = Type };

    public static GrpcTryDeleteKeyValueResponse TryDeleteKeyValue() => new() { Type = Type };

    public static GrpcTryGetKeyValueResponse TryGetKeyValue() => new() { Type = Type };

    public static GrpcTryExistsKeyValueResponse TryExistsKeyValue() => new() { Type = Type };

    public static GrpcTryCheckWriteIntentResponse TryCheckWriteIntent() => new() { Type = Type };

    public static GrpcTryAcquireExclusiveLockResponse TryAcquireExclusiveLock() => new() { Type = Type };

    public static GrpcTryAcquireExclusivePrefixLockResponse TryAcquireExclusivePrefixLock() => new() { Type = Type };

    public static GrpcTryReleaseExclusiveLockResponse TryReleaseExclusiveLock() => new() { Type = Type };

    public static GrpcTryReleaseExclusivePrefixLockResponse TryReleaseExclusivePrefixLock() => new() { Type = Type };

    public static GrpcTryAcquireExclusiveRangeLockResponse TryAcquireExclusiveRangeLock() => new() { Type = Type };

    public static GrpcTryReleaseExclusiveRangeLockResponse TryReleaseExclusiveRangeLock() => new() { Type = Type };

    public static GrpcTryPrepareMutationsResponse TryPrepareMutations() => new() { Type = Type };

    public static GrpcTryCommitMutationsResponse TryCommitMutations() => new() { Type = Type };

    public static GrpcTryRollbackMutationsResponse TryRollbackMutations() => new() { Type = Type };

    public static GrpcTryExecuteTransactionScriptResponse TryExecuteTransactionScript() => new() { Type = Type };

    public static GrpcGetByBucketResponse GetByBucket() => new() { Type = Type };

    public static GrpcGetByRangeResponse GetByRange() => new() { Type = Type };

    public static GrpcScanByPrefixResponse ScanByPrefix() => new() { Type = Type };

    public static GrpcScanAllByPrefixResponse ScanAllByPrefix() => new() { Type = Type };

    public static GrpcStartTransactionResponse StartTransaction() => new() { Type = Type };

    public static GrpcCommitTransactionResponse CommitTransaction() => new() { Type = Type };

    public static GrpcRollbackTransactionResponse RollbackTransaction() => new() { Type = Type };

    public static GrpcCloseTransactionResponse CloseTransaction() => new() { Type = Type };

    public static GrpcAcquireSnapshotHoldResponse AcquireSnapshotHold() => new() { Type = Type };

    public static GrpcRenewSnapshotHoldResponse RenewSnapshotHold() => new() { Type = Type };

    public static GrpcReleaseSnapshotHoldResponse ReleaseSnapshotHold() => new() { Type = Type };

    public static GrpcGetSnapshotFloorResponse GetSnapshotFloor() => new() { Type = Type };

    public static GrpcTrySetManyKeyValueResponse TrySetManyKeyValue(GrpcTrySetManyKeyValueRequest request)
    {
        GrpcTrySetManyKeyValueResponse response = new();

        foreach (GrpcTrySetManyKeyValueRequestItem item in request.Items)
            response.Items.Add(new GrpcTrySetManyKeyValueResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }

    public static GrpcTryDeleteManyKeyValueResponse TryDeleteManyKeyValue(GrpcTryDeleteManyKeyValueRequest request)
    {
        GrpcTryDeleteManyKeyValueResponse response = new();

        foreach (GrpcTryDeleteManyKeyValueRequestItem item in request.Items)
            response.Items.Add(new GrpcTryDeleteManyKeyValueResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }

    public static GrpcTryGetManyValuesResponse TryGetManyValues(GrpcTryGetManyValuesRequest request)
    {
        GrpcTryGetManyValuesResponse response = new();

        foreach (GrpcTryManyValuesRequestItem item in request.Items)
            response.Items.Add(new GrpcTryGetManyValuesResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }

    public static GrpcTryExistsManyValuesResponse TryExistsManyValues(GrpcTryExistsManyValuesRequest request)
    {
        GrpcTryExistsManyValuesResponse response = new();

        foreach (GrpcTryManyValuesRequestItem item in request.Items)
            response.Items.Add(new GrpcTryExistsManyValuesResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }

    public static GrpcTryCheckManyWriteIntentsResponse TryCheckManyWriteIntents(GrpcTryCheckManyWriteIntentsRequest request)
    {
        GrpcTryCheckManyWriteIntentsResponse response = new();

        foreach (GrpcTryCheckManyWriteIntentsRequestItem item in request.Items)
            response.Items.Add(new GrpcTryCheckManyWriteIntentsResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }

    public static GrpcTryAcquireManyExclusiveLocksResponse TryAcquireManyExclusiveLocks(GrpcTryAcquireManyExclusiveLocksRequest request)
    {
        GrpcTryAcquireManyExclusiveLocksResponse response = new();

        foreach (GrpcTryAcquireManyExclusiveLocksRequestItem item in request.Items)
            response.Items.Add(new GrpcTryAcquireManyExclusiveLocksResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }

    public static GrpcTryReleaseManyExclusiveLocksResponse TryReleaseManyExclusiveLocks(GrpcTryReleaseManyExclusiveLocksRequest request)
    {
        GrpcTryReleaseManyExclusiveLocksResponse response = new();

        foreach (GrpcTryReleaseManyExclusiveLocksRequestItem item in request.Items)
            response.Items.Add(new GrpcTryReleaseManyExclusiveLocksResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }

    public static GrpcTryPrepareManyMutationsResponse TryPrepareManyMutations(GrpcTryPrepareManyMutationsRequest request)
    {
        GrpcTryPrepareManyMutationsResponse response = new();

        foreach (GrpcTryPrepareManyMutationsRequestItem item in request.Items)
            response.Items.Add(new GrpcTryPrepareManyMutationsResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }

    public static GrpcTryCommitManyMutationsResponse TryCommitManyMutations(GrpcTryCommitManyMutationsRequest request)
    {
        GrpcTryCommitManyMutationsResponse response = new();

        foreach (GrpcTryCommitManyMutationsRequestItem item in request.Items)
            response.Items.Add(new GrpcTryCommitManyMutationsResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }

    public static GrpcTryRollbackManyMutationsResponse TryRollbackManyMutations(GrpcTryRollbackManyMutationsRequest request)
    {
        GrpcTryRollbackManyMutationsResponse response = new();

        foreach (GrpcTryRollbackManyMutationsRequestItem item in request.Items)
            response.Items.Add(new GrpcTryRollbackManyMutationsResponseItem
            {
                Type = Type, Key = item.Key, Durability = item.Durability
            });

        return response;
    }
}
