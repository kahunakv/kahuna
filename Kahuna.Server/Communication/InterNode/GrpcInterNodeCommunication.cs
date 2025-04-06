
using Google.Protobuf;
using Grpc.Net.Client;
using Kahuna.Communication.Common.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
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

    public async Task<(KeyValueResponseType, long)> TryExtendKeyValue(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryExtendKeyValueRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryExtendKeyValueResponse? remoteResponse = await client.TryExtendKeyValueAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision);
    }

    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(string node, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryGetKeyValueRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Revision = revision,
            Durability = (GrpcKeyValueDurability) durability,
        };
        
        GrpcTryGetKeyValueResponse? remoteResponse = await client.TryGetKeyValueAsync(request, cancellationToken: cancellationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, new(
            remoteResponse.Value?.ToByteArray(),
            remoteResponse.Revision,
            new(remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter),
            (KeyValueState)remoteResponse.State
        ));
    }

    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryExistsValue(string node, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryExistsKeyValueRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Revision = revision,
            Durability = (GrpcKeyValueDurability) durability,
        };
        
        GrpcTryExistsKeyValueResponse? remoteResponse = await client.TryExistsKeyValueAsync(request, cancellationToken: cancellationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, new(
            null,
            remoteResponse.Revision,
            new(remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter),
            (KeyValueState)remoteResponse.State
        ));
    }

    public async Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryAcquireExclusiveLockRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryAcquireExclusiveLockResponse? remoteResponse = await client.TryAcquireExclusiveLockAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, key, durability);
    }

    public async Task TryAcquireNodeExclusiveLocks(
        string node, 
        HLCTimestamp transactionId, 
        List<(string key, int expiresMs, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses,
        CancellationToken cancelationToken
    )
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
            
        KeyValuer.KeyValuerClient client = new(channel);
            
        GrpcTryAcquireManyExclusiveLocksRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        request.Items.Add(GetAcquireLockRequestItems(xkeys));
            
        GrpcTryAcquireManyExclusiveLocksResponse? remoteResponse = await client.TryAcquireManyExclusiveLocksAsync(request, cancellationToken: cancelationToken);

        lock (lockSync)
        {
            foreach (GrpcTryAcquireManyExclusiveLocksResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability));
        }
    }
    
    private static IEnumerable<GrpcTryAcquireManyExclusiveLocksRequestItem> GetAcquireLockRequestItems(List<(string key, int expiresMs, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, int expiresMs, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                ExpiresMs = key.expiresMs,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }

    public async Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryReleaseExclusiveLockRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryReleaseExclusiveLockResponse? remoteResponse = await client.TryReleaseExclusiveLockAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, key);
    }

    public async Task TryReleaseNodeExclusiveLocks(string node, HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
            
        KeyValuer.KeyValuerClient client = new(channel);
            
        GrpcTryReleaseManyExclusiveLocksRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        request.Items.Add(GetReleaseLockRequestItems(xkeys));
            
        GrpcTryReleaseManyExclusiveLocksResponse? remoteResponse = await client.TryReleaseManyExclusiveLocksAsync(request, cancellationToken: cancellationToken);

        lock (lockSync)
        {
            foreach (GrpcTryReleaseManyExclusiveLocksResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability));
        }
    }
    
    private static IEnumerable<GrpcTryReleaseManyExclusiveLocksRequestItem> GetReleaseLockRequestItems(List<(string key, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }
    
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryPrepareMutationsRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryPrepareMutationsResponse? remoteResponse = await client.TryPrepareMutationsAsync(request, cancellationToken: cancellationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, new(remoteResponse.ProposalTicketPhysical, remoteResponse.ProposalTicketCounter), key, durability);
    }

    public async Task TryPrepareNodeMutations(string node, HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
            
        KeyValuer.KeyValuerClient client = new(channel);
            
        GrpcTryPrepareManyMutationsRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        request.Items.Add(GetPrepareRequestItems(xkeys));
            
        GrpcTryPrepareManyMutationsResponse? remoteResponse = await client.TryPrepareManyMutationsAsync(request, cancellationToken: cancellationToken);

        lock (lockSync)
        {
            foreach (GrpcTryPrepareManyMutationsResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, new(item.ProposalTicketPhysical, item.ProposalTicketCounter), item.Key, (KeyValueDurability)item.Durability));
        }
    }
    
    private static IEnumerable<GrpcTryPrepareManyMutationsRequestItem> GetPrepareRequestItems(List<(string key, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }

    public async Task<(KeyValueResponseType, long)> TryCommitMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryCommitMutationsRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ProposalTicketPhysical = ticketId.L,
            ProposalTicketCounter = ticketId.C,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryCommitMutationsResponse? remoteResponse = await client.TryCommitMutationsAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.ProposalIndex);
    }
}