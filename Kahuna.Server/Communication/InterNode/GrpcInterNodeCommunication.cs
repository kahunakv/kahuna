
using Google.Protobuf;
using Grpc.Net.Client;
using Kahuna.Communication.Common.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Communication.Internode;

public class GrpcInterNodeCommunication : IInterNodeCommunication
{
    private readonly KahunaConfiguration configuration;
    
    public GrpcInterNodeCommunication(KahunaConfiguration configuration)
    {
        this.configuration = configuration;
    }
    
    public async Task<(LockResponseType, long)> TryLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),
            ExpiresMs = expiresMs,
            Durability = (GrpcLockDurability)durability
        };
        
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        Locker.LockerClient client = new(channel);
        
        GrpcTryLockResponse? remoteResponse = await client.TryLockAsync(request, cancellationToken: cancellationToken);
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((LockResponseType)remoteResponse.Type, remoteResponse.FencingToken);
    }
}