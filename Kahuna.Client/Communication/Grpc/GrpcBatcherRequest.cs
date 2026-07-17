
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
/// request) the single payload is held as one reference in a value type. The per-operation accessors
/// preserve the original API by narrowing that reference, so the batch dispatch reads
/// <c>request.TryLock</c> exactly as before.
/// </summary>
internal readonly struct GrpcBatcherRequest
{
    private readonly object? payload;

    public GrpcBatcherRequest(GrpcTryLockRequest tryLock) => payload = tryLock;

    public GrpcBatcherRequest(GrpcUnlockRequest unlock) => payload = unlock;

    public GrpcBatcherRequest(GrpcExtendLockRequest extendLock) => payload = extendLock;

    public GrpcBatcherRequest(GrpcGetLockRequest getLock) => payload = getLock;

    public GrpcBatcherRequest(GrpcTrySetKeyValueRequest trySetKeyValue) => payload = trySetKeyValue;

    public GrpcBatcherRequest(GrpcTrySetManyKeyValueRequest trySetManyKeyValues) => payload = trySetManyKeyValues;

    public GrpcBatcherRequest(GrpcTryDeleteManyKeyValueRequest tryDeleteManyKeyValues) => payload = tryDeleteManyKeyValues;

    public GrpcBatcherRequest(GrpcTryGetKeyValueRequest tryGetKeyValue) => payload = tryGetKeyValue;

    public GrpcBatcherRequest(GrpcTryDeleteKeyValueRequest tryDeleteKeyValue) => payload = tryDeleteKeyValue;

    public GrpcBatcherRequest(GrpcTryExtendKeyValueRequest tryExtendKeyValue) => payload = tryExtendKeyValue;

    public GrpcBatcherRequest(GrpcTryExistsKeyValueRequest tryExistsKeyValue) => payload = tryExistsKeyValue;

    public GrpcBatcherRequest(GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionScript) => payload = tryExecuteTransactionScript;

    public GrpcBatcherRequest(GrpcTryAcquireExclusiveLockRequest tryAcquireExclusiveLock) => payload = tryAcquireExclusiveLock;

    public GrpcBatcherRequest(GrpcGetByBucketRequest getByBucket) => payload = getByBucket;

    public GrpcBatcherRequest(GrpcScanAllByPrefixRequest scanByPrefix) => payload = scanByPrefix;

    public GrpcBatcherRequest(GrpcStartTransactionRequest startTransaction) => payload = startTransaction;

    public GrpcBatcherRequest(GrpcCommitTransactionRequest commitTransaction) => payload = commitTransaction;

    public GrpcBatcherRequest(GrpcRollbackTransactionRequest rollbackTransaction) => payload = rollbackTransaction;

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
