
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

internal sealed class GrpcBatcherRequest
{
    public GrpcTryLockRequest? TryLock { get; }
    
    public GrpcUnlockRequest? Unlock { get; }
    
    public GrpcExtendLockRequest? ExtendLock { get; }
    
    public GrpcGetLockRequest? GetLock { get; }
    
    public GrpcTrySetKeyValueRequest? TrySetKeyValue { get; }
    
    public GrpcTryGetKeyValueRequest? TryGetKeyValue { get; }
    
    public GrpcTryDeleteKeyValueRequest? TryDeleteKeyValue { get; }
    
    public GrpcTryExtendKeyValueRequest? TryExtendKeyValue { get; }
    
    public GrpcTryExistsKeyValueRequest? TryExistsKeyValue { get; }
    
    public GrpcTryExecuteTransactionScriptRequest? TryExecuteTransactionScript { get; }
    
    public GrpcGetByPrefixRequest? GetByPrefix { get; }
    
    public GrpcScanAllByPrefixRequest? ScanByPrefix { get; }
    
    public GrpcBatcherRequest(GrpcTryLockRequest tryLock)
    {
        TryLock = tryLock;
    }
    
    public GrpcBatcherRequest(GrpcUnlockRequest unlock)
    {
        Unlock = unlock;
    }
    
    public GrpcBatcherRequest(GrpcExtendLockRequest extendLock)
    {
        ExtendLock = extendLock;
    }
    
    public GrpcBatcherRequest(GrpcGetLockRequest getLock)
    {
        GetLock = getLock;
    }

    public GrpcBatcherRequest(GrpcTrySetKeyValueRequest trySetKeyValue)
    {
        TrySetKeyValue = trySetKeyValue;
    }
    
    public GrpcBatcherRequest(GrpcTryGetKeyValueRequest tryGetKeyValue)
    {
        TryGetKeyValue = tryGetKeyValue;
    }
    
    public GrpcBatcherRequest(GrpcTryDeleteKeyValueRequest tryDeleteKeyValue)
    {
        TryDeleteKeyValue = tryDeleteKeyValue;
    }
    
    public GrpcBatcherRequest(GrpcTryExtendKeyValueRequest tryExtendKeyValue)
    {
        TryExtendKeyValue = tryExtendKeyValue;
    }
    
    public GrpcBatcherRequest(GrpcTryExistsKeyValueRequest tryExistsKeyValue)
    {
        TryExistsKeyValue = tryExistsKeyValue;
    }
    
    public GrpcBatcherRequest(GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionScript)
    {
        TryExecuteTransactionScript = tryExecuteTransactionScript;
    }
    
    public GrpcBatcherRequest(GrpcGetByPrefixRequest getByPrefix)
    {
        GetByPrefix = getByPrefix;
    }
    
    public GrpcBatcherRequest(GrpcScanAllByPrefixRequest scanByPrefix)
    {
        ScanByPrefix = scanByPrefix;
    }
}