
using Kahuna.Shared.Communication.Grpc;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Builds the answer a batched request gets when its handler failed without producing one.
///
/// <para>A batched request is answered by RequestId on a shared duplex stream, so a handler that
/// throws leaves the caller's promise unresolved forever: the request hangs until the caller's own
/// deadline or until the whole stream dies and takes every unrelated in-flight request with it.
/// Answering the failed request — and only it — with the surface's MustRetry keeps the stream and
/// its other requests alive while telling the caller exactly what a retry loop needs to hear.</para>
///
/// <para>A handful of inter-node payloads carry no response-type field at all (they answer with a
/// bare <c>Success</c>/<c>Found</c> flag or a collection). For those, "retry" is inexpressible and
/// the negative value would be a lie a caller cannot distinguish from a real answer — an empty lock
/// set during a range handoff reads as "there were no locks". They are answered with the envelope's
/// None type instead, which the inter-node reader turns into a retryable transport failure.</para>
/// </summary>
internal static class BatchRefusalResponses
{
    /// <summary>
    /// Answers an inter-node key-value batch request. The response payload mirrors the request's
    /// type so the caller's per-type unwrapping finds what it expects.
    /// </summary>
    public static GrpcBatchServerKeyValueResponse ForServerKeyValue(GrpcBatchServerKeyValueRequest request)
    {
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = request.Type,
            RequestId = request.RequestId
        };

        switch (request.Type)
        {
            case GrpcServerBatchType.ServerTrySetKeyValue:
                response.TrySetKeyValue = KeyValueMustRetry.TrySetKeyValue();
                break;

            case GrpcServerBatchType.ServerTrySetManyKeyValue:
                response.TrySetManyKeyValue = KeyValueMustRetry.TrySetManyKeyValue(request.TrySetManyKeyValue);
                break;

            case GrpcServerBatchType.ServerTryDeleteManyKeyValue:
                response.TryDeleteManyKeyValue = KeyValueMustRetry.TryDeleteManyKeyValue(request.TryDeleteManyKeyValue);
                break;

            case GrpcServerBatchType.ServerTryGetKeyValue:
                response.TryGetKeyValue = KeyValueMustRetry.TryGetKeyValue();
                break;

            case GrpcServerBatchType.ServerTryGetManyValues:
                response.TryGetManyValues = KeyValueMustRetry.TryGetManyValues(request.TryGetManyValues);
                break;

            case GrpcServerBatchType.ServerTryExistsManyValues:
                response.TryExistsManyValues = KeyValueMustRetry.TryExistsManyValues(request.TryExistsManyValues);
                break;

            case GrpcServerBatchType.ServerTryDeleteKeyValue:
                response.TryDeleteKeyValue = KeyValueMustRetry.TryDeleteKeyValue();
                break;

            case GrpcServerBatchType.ServerTryExtendKeyValue:
                response.TryExtendKeyValue = KeyValueMustRetry.TryExtendKeyValue();
                break;

            case GrpcServerBatchType.ServerTryExistsKeyValue:
                response.TryExistsKeyValue = KeyValueMustRetry.TryExistsKeyValue();
                break;

            case GrpcServerBatchType.ServerTryCheckWriteIntent:
                response.TryCheckWriteIntent = KeyValueMustRetry.TryCheckWriteIntent();
                break;

            case GrpcServerBatchType.ServerTryCheckManyWriteIntents:
                response.TryCheckManyWriteIntents = KeyValueMustRetry.TryCheckManyWriteIntents(request.TryCheckManyWriteIntents);
                break;

            case GrpcServerBatchType.ServerTryExecuteTransactionScript:
                response.TryExecuteTransactionScript = KeyValueMustRetry.TryExecuteTransactionScript();
                break;

            case GrpcServerBatchType.ServerTryAcquireExclusiveLock:
                response.TryAcquireExclusiveLock = KeyValueMustRetry.TryAcquireExclusiveLock();
                break;

            case GrpcServerBatchType.ServerTryAcquireExclusivePrefixLock:
                response.TryAcquireExclusivePrefixLock = KeyValueMustRetry.TryAcquireExclusivePrefixLock();
                break;

            case GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks:
                response.TryAcquireManyExclusiveLocks = KeyValueMustRetry.TryAcquireManyExclusiveLocks(request.TryAcquireManyExclusiveLocks);
                break;

            case GrpcServerBatchType.ServerTryReleaseExclusiveLock:
                response.TryReleaseExclusiveLock = KeyValueMustRetry.TryReleaseExclusiveLock();
                break;

            case GrpcServerBatchType.ServerTryReleaseExclusivePrefixLock:
                response.TryReleaseExclusivePrefixLock = KeyValueMustRetry.TryReleaseExclusivePrefixLock();
                break;

            case GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks:
                response.TryReleaseManyExclusiveLocks = KeyValueMustRetry.TryReleaseManyExclusiveLocks(request.TryReleaseManyExclusiveLocks);
                break;

            case GrpcServerBatchType.ServerTryAcquireExclusiveRangeLock:
                response.TryAcquireExclusiveRangeLock = KeyValueMustRetry.TryAcquireExclusiveRangeLock();
                break;

            case GrpcServerBatchType.ServerTryReleaseExclusiveRangeLock:
                response.TryReleaseExclusiveRangeLock = KeyValueMustRetry.TryReleaseExclusiveRangeLock();
                break;

            case GrpcServerBatchType.ServerTryPrepareMutations:
                response.TryPrepareMutations = KeyValueMustRetry.TryPrepareMutations();
                break;

            case GrpcServerBatchType.ServerTryPrepareManyMutations:
                response.TryPrepareManyMutations = KeyValueMustRetry.TryPrepareManyMutations(request.TryPrepareManyMutations);
                break;

            case GrpcServerBatchType.ServerTryCommitMutations:
                response.TryCommitMutations = KeyValueMustRetry.TryCommitMutations();
                break;

            case GrpcServerBatchType.ServerTryCommitManyMutations:
                response.TryCommitManyMutations = KeyValueMustRetry.TryCommitManyMutations(request.TryCommitManyMutations);
                break;

            case GrpcServerBatchType.ServerTryRollbackMutations:
                response.TryRollbackMutations = KeyValueMustRetry.TryRollbackMutations();
                break;

            case GrpcServerBatchType.ServerTryRollbackManyMutations:
                response.TryRollbackManyMutations = KeyValueMustRetry.TryRollbackManyMutations(request.TryRollbackManyMutations);
                break;

            case GrpcServerBatchType.ServerTryGetByBucket:
                response.GetByBucket = KeyValueMustRetry.GetByBucket();
                break;

            case GrpcServerBatchType.ServerTryGetByRange:
                response.GetByRange = KeyValueMustRetry.GetByRange();
                break;

            case GrpcServerBatchType.ServerTryScanByPrefix:
                response.ScanByPrefix = KeyValueMustRetry.ScanByPrefix();
                break;

            case GrpcServerBatchType.ServerTryStartTransaction:
                response.StartTransaction = KeyValueMustRetry.StartTransaction();
                break;

            case GrpcServerBatchType.ServerTryCommitTransaction:
                response.CommitTransaction = KeyValueMustRetry.CommitTransaction();
                break;

            case GrpcServerBatchType.ServerTryRollbackTransaction:
                response.RollbackTransaction = KeyValueMustRetry.RollbackTransaction();
                break;

            case GrpcServerBatchType.ServerCloseTransaction:
                response.CloseTransaction = KeyValueMustRetry.CloseTransaction();
                break;

            case GrpcServerBatchType.ServerTryAcquireSnapshotHold:
                response.AcquireSnapshotHold = KeyValueMustRetry.AcquireSnapshotHold();
                break;

            case GrpcServerBatchType.ServerTryRenewSnapshotHold:
                response.RenewSnapshotHold = KeyValueMustRetry.RenewSnapshotHold();
                break;

            case GrpcServerBatchType.ServerTryReleaseSnapshotHold:
                response.ReleaseSnapshotHold = KeyValueMustRetry.ReleaseSnapshotHold();
                break;

            case GrpcServerBatchType.ServerTryGetSnapshotFloor:
                response.GetSnapshotFloor = KeyValueMustRetry.GetSnapshotFloor();
                break;

            // Payloads with no response-type field: a fabricated Success=false / Found=false / empty
            // collection is indistinguishable from a real negative answer, so refuse explicitly
            // instead. The reader maps the None type to a retryable transport failure.
            case GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded:
            case GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved:
            case GrpcServerBatchType.ServerTryGetRangeLocks:
            case GrpcServerBatchType.ServerTryImportRangeLocks:
            case GrpcServerBatchType.ServerImportCompletionReceipts:
            case GrpcServerBatchType.ServerImportCoordinatorDecisions:
            case GrpcServerBatchType.ServerDurableOperation:
            case GrpcServerBatchType.ServerLookupTransactionRecord:
            case GrpcServerBatchType.ServerGetStagedBaseVerdicts:
            case GrpcServerBatchType.ServerBeginOperation:
            case GrpcServerBatchType.ServerCompleteOperation:
            case GrpcServerBatchType.ServerGetTransactionWorkingSet:
            case GrpcServerBatchType.ServerTypeNone:
            default:
                response.Type = GrpcServerBatchType.ServerTypeNone;
                break;
        }

        return response;
    }

    /// <summary>Answers a client-facing key-value batch request. Every client-facing payload can
    /// carry MustRetry, so the SDK always gets a classifiable answer here.</summary>
    public static GrpcBatchClientKeyValueResponse ForClientKeyValue(GrpcBatchClientKeyValueRequest request)
    {
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = request.Type,
            RequestId = request.RequestId
        };

        switch (request.Type)
        {
            case GrpcClientBatchType.TrySetKeyValue:
                response.TrySetKeyValue = KeyValueMustRetry.TrySetKeyValue();
                break;

            case GrpcClientBatchType.TrySetManyKeyValue:
                response.TrySetManyKeyValue = KeyValueMustRetry.TrySetManyKeyValue(request.TrySetManyKeyValue);
                break;

            case GrpcClientBatchType.TryDeleteManyKeyValue:
                response.TryDeleteManyKeyValue = KeyValueMustRetry.TryDeleteManyKeyValue(request.TryDeleteManyKeyValue);
                break;

            case GrpcClientBatchType.TryGetKeyValue:
                response.TryGetKeyValue = KeyValueMustRetry.TryGetKeyValue();
                break;

            case GrpcClientBatchType.TryDeleteKeyValue:
                response.TryDeleteKeyValue = KeyValueMustRetry.TryDeleteKeyValue();
                break;

            case GrpcClientBatchType.TryExtendKeyValue:
                response.TryExtendKeyValue = KeyValueMustRetry.TryExtendKeyValue();
                break;

            case GrpcClientBatchType.TryExistsKeyValue:
                response.TryExistsKeyValue = KeyValueMustRetry.TryExistsKeyValue();
                break;

            case GrpcClientBatchType.TryAcquireExclusiveLock:
                response.TryAcquireExclusiveLock = KeyValueMustRetry.TryAcquireExclusiveLock();
                break;

            case GrpcClientBatchType.TryExecuteTransactionScript:
                response.TryExecuteTransactionScript = KeyValueMustRetry.TryExecuteTransactionScript();
                break;

            case GrpcClientBatchType.TryGetByBucket:
                response.GetByBucket = KeyValueMustRetry.GetByBucket();
                break;

            case GrpcClientBatchType.TryScanByPrefix:
                response.ScanByPrefix = KeyValueMustRetry.ScanAllByPrefix();
                break;

            case GrpcClientBatchType.TryStartTransaction:
                response.StartTransaction = KeyValueMustRetry.StartTransaction();
                break;

            case GrpcClientBatchType.TryCommitTransaction:
                response.CommitTransaction = KeyValueMustRetry.CommitTransaction();
                break;

            case GrpcClientBatchType.TryRollbackTransaction:
                response.RollbackTransaction = KeyValueMustRetry.RollbackTransaction();
                break;

            case GrpcClientBatchType.TypeNone:
            default:
                response.Type = GrpcClientBatchType.TypeNone;
                break;
        }

        return response;
    }

    /// <summary>Answers an inter-node lock batch request.</summary>
    public static GrpcBatchServerLockResponse ForServerLock(GrpcBatchServerLockRequest request)
    {
        GrpcBatchServerLockResponse response = new()
        {
            Type = request.Type,
            RequestId = request.RequestId
        };

        switch (request.Type)
        {
            case GrpcLockServerBatchType.ServerTypeTryLock:
                response.TryLock = LockMustRetry.TryLock();
                break;

            case GrpcLockServerBatchType.ServerTypeExtendLock:
                response.ExtendLock = LockMustRetry.ExtendLock();
                break;

            case GrpcLockServerBatchType.ServerTypeUnlock:
                response.Unlock = LockMustRetry.Unlock();
                break;

            case GrpcLockServerBatchType.ServerTypeGetLock:
                response.GetLock = LockMustRetry.GetLock();
                break;

            case GrpcLockServerBatchType.ServerTypeNone:
            default:
                response.Type = GrpcLockServerBatchType.ServerTypeNone;
                break;
        }

        return response;
    }

    /// <summary>Answers a client-facing lock batch request.</summary>
    public static GrpcBatchClientLockResponse ForClientLock(GrpcBatchClientLockRequest request)
    {
        GrpcBatchClientLockResponse response = new()
        {
            Type = request.Type,
            RequestId = request.RequestId
        };

        switch (request.Type)
        {
            case GrpcLockClientBatchType.TypeTryLock:
                response.TryLock = LockMustRetry.TryLock();
                break;

            case GrpcLockClientBatchType.TypeExtendLock:
                response.ExtendLock = LockMustRetry.ExtendLock();
                break;

            case GrpcLockClientBatchType.TypeUnlock:
                response.Unlock = LockMustRetry.Unlock();
                break;

            case GrpcLockClientBatchType.TypeGetLock:
                response.GetLock = LockMustRetry.GetLock();
                break;

            case GrpcLockClientBatchType.TypeNone:
            default:
                response.Type = GrpcLockClientBatchType.TypeNone;
                break;
        }

        return response;
    }
}
