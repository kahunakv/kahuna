
using Google.Protobuf;
using Grpc.Net.Client;
using Kahuna.Communication.Common.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Server.Locks;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;

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

    public async Task<(LockResponseType, long)> TryExtendLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcExtendLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),
            ExpiresMs = expiresMs,
            Durability = (GrpcLockDurability)durability
        };
        
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        Locker.LockerClient client = new(channel);
        
        GrpcExtendLockResponse? remoteResponse = await client.TryExtendLockAsync(request, cancellationToken: cancellationToken);
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((LockResponseType)remoteResponse.Type, remoteResponse.FencingToken);
    }

    public async Task<LockResponseType> TryUnlock(string node, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcUnlockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),           
            Durability = (GrpcLockDurability)durability
        };
        
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        Locker.LockerClient client = new(channel);
        
        GrpcUnlockResponse? remoteResponse = await client.UnlockAsync(request, cancellationToken: cancellationToken);
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (LockResponseType)remoteResponse.Type;
    }

    public async Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string node, string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcGetLockRequest request = new()
        {
            Resource = resource,
            Durability = (GrpcLockDurability)durability
        };
        
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        Locker.LockerClient client = new(channel);
        
        GrpcGetLockResponse? remoteResponse = await client.GetLockAsync(request, cancellationToken: cancellationToken);
        
        if (remoteResponse.Type != GrpcLockResponseType.LockResponseTypeGot)
            return ((LockResponseType)remoteResponse.Type, null);

        return ((LockResponseType)remoteResponse.Type,
            new(
                remoteResponse.Owner?.ToByteArray(), 
                remoteResponse.FencingToken,
                new(remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter)
            )
        );
    }

    public async Task<(KeyValueResponseType, long)> TrySetKeyValue(string node, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);

        GrpcTrySetKeyValueRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            CompareRevision = compareRevision,
            Flags = (GrpcKeyValueFlags) flags,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability) durability,
        };
        
        if (value is not null)
            request.Value = UnsafeByteOperations.UnsafeWrap(value);
        
        if (compareValue is not null)
            request.CompareValue = UnsafeByteOperations.UnsafeWrap(compareValue);
        
        GrpcTrySetKeyValueResponse? remoteResponse = await client.TrySetKeyValueAsync(request, cancellationToken: cancellationToken);
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision);
    }

    public async Task<(KeyValueResponseType, long)> TryDeleteKeyValue(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryDeleteKeyValueRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryDeleteKeyValueResponse? remoteResponse = await client.TryDeleteKeyValueAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision);
    }
}