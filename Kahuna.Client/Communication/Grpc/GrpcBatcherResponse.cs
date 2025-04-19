
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

internal sealed class GrpcBatcherResponse
{
    public GrpcTrySetKeyValueResponse? TrySetKeyValue { get; }
    
    public GrpcTryGetKeyValueResponse? TryGetKeyValue { get; }

    public GrpcBatcherResponse(GrpcTrySetKeyValueResponse trySetKeyValue)
    {
        TrySetKeyValue = trySetKeyValue;
    }
    
    public GrpcBatcherResponse(GrpcTryGetKeyValueResponse tryGetKeyValue)
    {
        TryGetKeyValue = tryGetKeyValue;
    }
}