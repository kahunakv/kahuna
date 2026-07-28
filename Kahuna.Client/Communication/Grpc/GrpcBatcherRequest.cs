
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

/// <summary>
/// A single queued batch operation's request. Exactly one operation payload is ever populated, so
/// rather than a wide class with one nullable field per operation type (an allocation per queued
/// request) the single payload is held as one reference in a value type. Each constructor also
/// records the wire batch type, so dispatch reads one discriminator and performs a single cast
/// instead of probing every payload accessor in turn. The per-operation accessors preserve the
/// original API by narrowing that reference, so callers can still read <c>request.TryLock</c>.
/// </summary>
internal readonly struct GrpcBatcherRequest
{
    private readonly object? payload;

    // Holds either a GrpcLockClientBatchType or a GrpcClientBatchType value; the owning
    // GrpcBatcherItem.Type decides which enum space applies.
    private readonly int batchType;

    /// <summary>The raw request message; cast according to the batch type discriminator.</summary>
    public object? Payload => payload;

    /// <summary>Wire batch type when this request carries a lock operation.</summary>
    public GrpcLockClientBatchType LockBatchType => (GrpcLockClientBatchType)batchType;

    /// <summary>Wire batch type when this request carries a key-value operation.</summary>
    public GrpcClientBatchType KeyValueBatchType => (GrpcClientBatchType)batchType;

    public GrpcBatcherRequest(GrpcTryLockRequest tryLock)
    {
        payload = tryLock;
        batchType = (int)GrpcLockClientBatchType.TypeTryLock;
    }

    public GrpcBatcherRequest(GrpcUnlockRequest unlock)
    {
        payload = unlock;
        batchType = (int)GrpcLockClientBatchType.TypeUnlock;
    }

    public GrpcBatcherRequest(GrpcExtendLockRequest extendLock)
    {
        payload = extendLock;
        batchType = (int)GrpcLockClientBatchType.TypeExtendLock;
    }

    public GrpcBatcherRequest(GrpcGetLockRequest getLock)
    {
        payload = getLock;
        batchType = (int)GrpcLockClientBatchType.TypeGetLock;
    }

    public GrpcBatcherRequest(GrpcTrySetKeyValueRequest trySetKeyValue)
    {
        payload = trySetKeyValue;
        batchType = (int)GrpcClientBatchType.TrySetKeyValue;
    }

    public GrpcBatcherRequest(GrpcTrySetManyKeyValueRequest trySetManyKeyValues)
    {
        payload = trySetManyKeyValues;
        batchType = (int)GrpcClientBatchType.TrySetManyKeyValue;
    }

    public GrpcBatcherRequest(GrpcTryDeleteManyKeyValueRequest tryDeleteManyKeyValues)
    {
        payload = tryDeleteManyKeyValues;
        batchType = (int)GrpcClientBatchType.TryDeleteManyKeyValue;
    }

    public GrpcBatcherRequest(GrpcTryGetKeyValueRequest tryGetKeyValue)
    {
        payload = tryGetKeyValue;
        batchType = (int)GrpcClientBatchType.TryGetKeyValue;
    }

    public GrpcBatcherRequest(GrpcTryDeleteKeyValueRequest tryDeleteKeyValue)
    {
        payload = tryDeleteKeyValue;
        batchType = (int)GrpcClientBatchType.TryDeleteKeyValue;
    }

    public GrpcBatcherRequest(GrpcTryExtendKeyValueRequest tryExtendKeyValue)
    {
        payload = tryExtendKeyValue;
        batchType = (int)GrpcClientBatchType.TryExtendKeyValue;
    }

    public GrpcBatcherRequest(GrpcTryExistsKeyValueRequest tryExistsKeyValue)
    {
        payload = tryExistsKeyValue;
        batchType = (int)GrpcClientBatchType.TryExistsKeyValue;
    }

    public GrpcBatcherRequest(GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionScript)
    {
        payload = tryExecuteTransactionScript;
        batchType = (int)GrpcClientBatchType.TryExecuteTransactionScript;
    }

    public GrpcBatcherRequest(GrpcTryAcquireExclusiveLockRequest tryAcquireExclusiveLock)
    {
        payload = tryAcquireExclusiveLock;
        batchType = (int)GrpcClientBatchType.TryAcquireExclusiveLock;
    }

    public GrpcBatcherRequest(GrpcGetByBucketRequest getByBucket)
    {
        payload = getByBucket;
        batchType = (int)GrpcClientBatchType.TryGetByBucket;
    }

    public GrpcBatcherRequest(GrpcScanAllByPrefixRequest scanByPrefix)
    {
        payload = scanByPrefix;
        batchType = (int)GrpcClientBatchType.TryScanByPrefix;
    }

    public GrpcBatcherRequest(GrpcStartTransactionRequest startTransaction)
    {
        payload = startTransaction;
        batchType = (int)GrpcClientBatchType.TryStartTransaction;
    }

    public GrpcBatcherRequest(GrpcCommitTransactionRequest commitTransaction)
    {
        payload = commitTransaction;
        batchType = (int)GrpcClientBatchType.TryCommitTransaction;
    }

    public GrpcBatcherRequest(GrpcRollbackTransactionRequest rollbackTransaction)
    {
        payload = rollbackTransaction;
        batchType = (int)GrpcClientBatchType.TryRollbackTransaction;
    }

    public GrpcTryLockRequest? TryLock => payload as GrpcTryLockRequest;

    public GrpcUnlockRequest? Unlock => payload as GrpcUnlockRequest;

    public GrpcExtendLockRequest? ExtendLock => payload as GrpcExtendLockRequest;

    public GrpcGetLockRequest? GetLock => payload as GrpcGetLockRequest;

    public GrpcTrySetKeyValueRequest? TrySetKeyValue => payload as GrpcTrySetKeyValueRequest;

    public GrpcTrySetManyKeyValueRequest? TrySetManyKeyValues => payload as GrpcTrySetManyKeyValueRequest;

    public GrpcTryDeleteManyKeyValueRequest? TryDeleteManyKeyValues => payload as GrpcTryDeleteManyKeyValueRequest;

    public GrpcTryGetKeyValueRequest? TryGetKeyValue => payload as GrpcTryGetKeyValueRequest;

    public GrpcTryDeleteKeyValueRequest? TryDeleteKeyValue => payload as GrpcTryDeleteKeyValueRequest;

    public GrpcTryExtendKeyValueRequest? TryExtendKeyValue => payload as GrpcTryExtendKeyValueRequest;

    public GrpcTryExistsKeyValueRequest? TryExistsKeyValue => payload as GrpcTryExistsKeyValueRequest;

    public GrpcTryExecuteTransactionScriptRequest? TryExecuteTransactionScript => payload as GrpcTryExecuteTransactionScriptRequest;

    public GrpcTryAcquireExclusiveLockRequest? TryAcquireExclusiveLock => payload as GrpcTryAcquireExclusiveLockRequest;

    public GrpcGetByBucketRequest? GetByBucket => payload as GrpcGetByBucketRequest;

    public GrpcScanAllByPrefixRequest? ScanByPrefix => payload as GrpcScanAllByPrefixRequest;

    public GrpcStartTransactionRequest? StartTransaction => payload as GrpcStartTransactionRequest;

    public GrpcCommitTransactionRequest? CommitTransaction => payload as GrpcCommitTransactionRequest;

    public GrpcRollbackTransactionRequest? RollbackTransaction => payload as GrpcRollbackTransactionRequest;
}
