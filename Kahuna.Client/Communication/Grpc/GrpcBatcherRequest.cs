
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

    public GrpcBatcherRequest(GrpcTrySetKeyValueRequest trySetKeyValue)
    {
        TrySetKeyValue = trySetKeyValue;
    }
    
    public GrpcBatcherRequest(GrpcTryGetKeyValueRequest tryGetKeyValue)
    {
        TryGetKeyValue = tryGetKeyValue;
    }
}