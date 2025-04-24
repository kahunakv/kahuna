
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
    
    public GrpcTryGetKeyValueResponse? TryGetKeyValue { get; }
    
    public GrpcTryDeleteKeyValueResponse? TryDeleteKeyValue { get; }
    
    public GrpcTryExtendKeyValueResponse? TryExtendKeyValue { get; }

    public GrpcTryExistsKeyValueResponse? TryExistsKeyValue { get; }
    
    public GrpcTryExecuteTransactionScriptResponse? TryExecuteTransactionScript { get; }
    
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
    
    public GrpcBatcherResponse(GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScript)
    {
        TryExecuteTransactionScript = tryExecuteTransactionScript;
    }
}