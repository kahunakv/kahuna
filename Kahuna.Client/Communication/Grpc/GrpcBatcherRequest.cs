
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

internal sealed class GrpcBatcherRequest
{        
    public GrpcTrySetKeyValueRequest? TrySetKeyValue { get; }
    
    public GrpcTryGetKeyValueRequest? TryGetKeyValue { get; }
    
    public GrpcTryDeleteKeyValueRequest? TryDeleteKeyValue { get; }
    
    public GrpcTryExtendKeyValueRequest? TryExtendKeyValue { get; }
    
    public GrpcTryExistsKeyValueRequest? TryExistsKeyValue { get; }
    
    public GrpcTryExecuteTransactionRequest? TryExecuteTransaction { get; }

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
    
    public GrpcBatcherRequest(GrpcTryExecuteTransactionRequest tryExecuteTransaction)
    {
        TryExecuteTransaction = tryExecuteTransaction;
    }
}