
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

internal sealed class GrpcBatcherResponse
{
    public GrpcTryLockResponse? TryLock { get; }
    
    public GrpcUnlockResponse? Unlock { get; }
    
    public GrpcExtendLockResponse? ExtendLock { get; }
    
    public GrpcGetLockResponse? GetLock { get; }
    
    public GrpcTrySetKeyValueResponse? TrySetKeyValue { get; }
    
    public GrpcTrySetManyKeyValueResponse? TrySetManyKeyValues { get; }
    
    public GrpcTryGetKeyValueResponse? TryGetKeyValue { get; }
    
    public GrpcTryDeleteKeyValueResponse? TryDeleteKeyValue { get; }
    
    public GrpcTryExtendKeyValueResponse? TryExtendKeyValue { get; }

    public GrpcTryExistsKeyValueResponse? TryExistsKeyValue { get; }
    
    public GrpcTryAcquireExclusiveLockResponse? TryAcquireExclusiveLock { get; }
    
    public GrpcTryExecuteTransactionScriptResponse? TryExecuteTransactionScript { get; }     
    
    public GrpcGetByBucketResponse? GetByBucket { get; }
    
    public GrpcScanAllByPrefixResponse? ScanByPrefix { get; }
    
    public GrpcStartTransactionResponse? StartTransaction { get; }
    
    public GrpcCommitTransactionResponse? CommitTransaction { get; }
    
    public GrpcRollbackTransactionResponse? RollbackTransaction { get; }
    
    public GrpcBatcherResponse(GrpcTryLockResponse tryLock)
    {
        TryLock = tryLock;
    }
    
    public GrpcBatcherResponse(GrpcUnlockResponse unlock)
    {
        Unlock = unlock;
    }
    
    public GrpcBatcherResponse(GrpcExtendLockResponse extendLock)
    {
        ExtendLock = extendLock;
    }
    
    public GrpcBatcherResponse(GrpcGetLockResponse getLock)
    {
        GetLock = getLock;
    }

    public GrpcBatcherResponse(GrpcTrySetKeyValueResponse trySetKeyValue)
    {
        TrySetKeyValue = trySetKeyValue;
    }

    public GrpcBatcherResponse(GrpcTrySetManyKeyValueResponse trySetManyKeyValues)
    {
        TrySetManyKeyValues = trySetManyKeyValues;
    }
    
    public GrpcBatcherResponse(GrpcTryGetKeyValueResponse tryGetKeyValue)
    {
        TryGetKeyValue = tryGetKeyValue;
    }
    
    public GrpcBatcherResponse(GrpcTryDeleteKeyValueResponse tryDeleteKeyValue)
    {
        TryDeleteKeyValue = tryDeleteKeyValue;
    }
    
    public GrpcBatcherResponse(GrpcTryExtendKeyValueResponse tryExtendKeyValue)
    {
        TryExtendKeyValue = tryExtendKeyValue;
    }
    
    public GrpcBatcherResponse(GrpcTryExistsKeyValueResponse tryExistsKeyValue)
    {
        TryExistsKeyValue = tryExistsKeyValue;
    }
    
    public GrpcBatcherResponse(GrpcTryAcquireExclusiveLockResponse tryAcquireExclusiveLock)
    {
        TryAcquireExclusiveLock = tryAcquireExclusiveLock;
    }       
    
    public GrpcBatcherResponse(GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScript)
    {
        TryExecuteTransactionScript = tryExecuteTransactionScript;
    }
    
    public GrpcBatcherResponse(GrpcGetByBucketResponse getByBucket)
    {
        GetByBucket = getByBucket;
    }
    
    public GrpcBatcherResponse(GrpcScanAllByPrefixResponse scanByPrefix)
    {
        ScanByPrefix = scanByPrefix;
    }
    
    public GrpcBatcherResponse(GrpcStartTransactionResponse startTransaction)
    {
        StartTransaction = startTransaction;
    }
    
    public GrpcBatcherResponse(GrpcCommitTransactionResponse commitTransaction)
    {
        CommitTransaction = commitTransaction;
    }
    
    public GrpcBatcherResponse(GrpcRollbackTransactionResponse rollbackTransaction)
    {
        RollbackTransaction = rollbackTransaction;
    }
}