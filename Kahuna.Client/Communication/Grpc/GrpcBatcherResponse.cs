
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

/// <summary>
/// A batched operation's reply. Exactly one operation payload is ever populated, so rather than a
/// wide class with one nullable field per operation type (an allocation per completed request) the
/// single payload is held as one reference in a value type. The per-operation accessors preserve the
/// original API by narrowing that reference, so callers read <c>response.TryLock</c> exactly as before.
/// </summary>
internal readonly struct GrpcBatcherResponse
{
    private readonly object? payload;

    public GrpcBatcherResponse(GrpcTryLockResponse tryLock) => payload = tryLock;

    public GrpcBatcherResponse(GrpcUnlockResponse unlock) => payload = unlock;

    public GrpcBatcherResponse(GrpcExtendLockResponse extendLock) => payload = extendLock;

    public GrpcBatcherResponse(GrpcGetLockResponse getLock) => payload = getLock;

    public GrpcBatcherResponse(GrpcTrySetKeyValueResponse trySetKeyValue) => payload = trySetKeyValue;

    public GrpcBatcherResponse(GrpcTrySetManyKeyValueResponse trySetManyKeyValues) => payload = trySetManyKeyValues;

    public GrpcBatcherResponse(GrpcTryDeleteManyKeyValueResponse tryDeleteManyKeyValues) => payload = tryDeleteManyKeyValues;

    public GrpcBatcherResponse(GrpcTryGetKeyValueResponse tryGetKeyValue) => payload = tryGetKeyValue;

    public GrpcBatcherResponse(GrpcTryDeleteKeyValueResponse tryDeleteKeyValue) => payload = tryDeleteKeyValue;

    public GrpcBatcherResponse(GrpcTryExtendKeyValueResponse tryExtendKeyValue) => payload = tryExtendKeyValue;

    public GrpcBatcherResponse(GrpcTryExistsKeyValueResponse tryExistsKeyValue) => payload = tryExistsKeyValue;

    public GrpcBatcherResponse(GrpcTryAcquireExclusiveLockResponse tryAcquireExclusiveLock) => payload = tryAcquireExclusiveLock;

    public GrpcBatcherResponse(GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScript) => payload = tryExecuteTransactionScript;

    public GrpcBatcherResponse(GrpcGetByBucketResponse getByBucket) => payload = getByBucket;

    public GrpcBatcherResponse(GrpcScanAllByPrefixResponse scanByPrefix) => payload = scanByPrefix;

    public GrpcBatcherResponse(GrpcStartTransactionResponse startTransaction) => payload = startTransaction;

    public GrpcBatcherResponse(GrpcCommitTransactionResponse commitTransaction) => payload = commitTransaction;

    public GrpcBatcherResponse(GrpcRollbackTransactionResponse rollbackTransaction) => payload = rollbackTransaction;

    public GrpcTryLockResponse? TryLock => payload as GrpcTryLockResponse;

    public GrpcUnlockResponse? Unlock => payload as GrpcUnlockResponse;

    public GrpcExtendLockResponse? ExtendLock => payload as GrpcExtendLockResponse;

    public GrpcGetLockResponse? GetLock => payload as GrpcGetLockResponse;

    public GrpcTrySetKeyValueResponse? TrySetKeyValue => payload as GrpcTrySetKeyValueResponse;

    public GrpcTrySetManyKeyValueResponse? TrySetManyKeyValues => payload as GrpcTrySetManyKeyValueResponse;

    public GrpcTryDeleteManyKeyValueResponse? TryDeleteManyKeyValues => payload as GrpcTryDeleteManyKeyValueResponse;

    public GrpcTryGetKeyValueResponse? TryGetKeyValue => payload as GrpcTryGetKeyValueResponse;

    public GrpcTryDeleteKeyValueResponse? TryDeleteKeyValue => payload as GrpcTryDeleteKeyValueResponse;

    public GrpcTryExtendKeyValueResponse? TryExtendKeyValue => payload as GrpcTryExtendKeyValueResponse;

    public GrpcTryExistsKeyValueResponse? TryExistsKeyValue => payload as GrpcTryExistsKeyValueResponse;

    public GrpcTryAcquireExclusiveLockResponse? TryAcquireExclusiveLock => payload as GrpcTryAcquireExclusiveLockResponse;

    public GrpcTryExecuteTransactionScriptResponse? TryExecuteTransactionScript => payload as GrpcTryExecuteTransactionScriptResponse;

    public GrpcGetByBucketResponse? GetByBucket => payload as GrpcGetByBucketResponse;

    public GrpcScanAllByPrefixResponse? ScanByPrefix => payload as GrpcScanAllByPrefixResponse;

    public GrpcStartTransactionResponse? StartTransaction => payload as GrpcStartTransactionResponse;

    public GrpcCommitTransactionResponse? CommitTransaction => payload as GrpcCommitTransactionResponse;

    public GrpcRollbackTransactionResponse? RollbackTransaction => payload as GrpcRollbackTransactionResponse;
}
