
using Kommander.Time;
using Kommander.Communication.Grpc;

using Google.Protobuf;
using Grpc.Net.Client;
using System.Runtime.InteropServices;
using Google.Protobuf.Collections;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Shared.KeyValue;
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
        
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
        
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
        
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
        
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
        Locker.LockerClient client = new(channel);
        
        GrpcGetLockResponse? remoteResponse = await client.GetLockAsync(request, cancellationToken: cancellationToken);
        
        if (remoteResponse.Type != GrpcLockResponseType.LockResponseTypeGot)
            return ((LockResponseType)remoteResponse.Type, null);
        
        byte[]? owner;
            
        if (MemoryMarshal.TryGetArray(remoteResponse.Owner.Memory, out ArraySegment<byte> segment))
            owner = segment.Array;
        else
            owner = remoteResponse.Owner.ToByteArray();

        return ((LockResponseType)remoteResponse.Type,
            new(
                owner, 
                remoteResponse.FencingToken,
                new(remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter)
            )
        );
    }

    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(string node, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision, new(remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter));
    }

    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision, new(remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter));
    }

    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision, new(remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter));
    }

    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(string node, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
        
        byte[]? value;
            
        if (MemoryMarshal.TryGetArray(remoteResponse.Value.Memory, out ArraySegment<byte> segment))
            value = segment.Array;
        else
            value = remoteResponse.Value.ToByteArray();
        
        return ((KeyValueResponseType)remoteResponse.Type, new(
            value,
            remoteResponse.Revision,
            new(remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter),
            new(remoteResponse.LastUsedPhysical, remoteResponse.LastUsedCounter),
            new(remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter),
            (KeyValueState)remoteResponse.State
        ));
    }

    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryExistsValue(string node, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
            new(remoteResponse.LastUsedPhysical, remoteResponse.LastUsedCounter),
            new(remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter),
            (KeyValueState)remoteResponse.State
        ));
    }

    public async Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
        GrpcChannel channel = SharedChannels.GetChannel(node);
            
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
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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
        GrpcChannel channel = SharedChannels.GetChannel(node);
            
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
    
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        string node, HLCTimestamp transactionId, 
        HLCTimestamp commitId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryPrepareMutationsRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            CommitIdPhysical = commitId.L,
            CommitIdCounter = commitId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability
        };
        
        GrpcTryPrepareMutationsResponse? remoteResponse = await client.TryPrepareMutationsAsync(request, cancellationToken: cancellationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (
            (KeyValueResponseType)remoteResponse.Type, 
            new(remoteResponse.ProposalTicketPhysical, remoteResponse.ProposalTicketCounter), 
            key, 
            durability
        );
    }

    public async Task TryPrepareNodeMutations(
        string node, 
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses, 
        CancellationToken cancellationToken
    )
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
            
        KeyValuer.KeyValuerClient client = new(channel);
            
        GrpcTryPrepareManyMutationsRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            CommitIdPhysical = commitId.L,
            CommitIdCounter = commitId.C,
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
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
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

    public async Task TryCommitNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
            
        KeyValuer.KeyValuerClient client = new(channel);
            
        GrpcTryCommitManyMutationsRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        request.Items.Add(GetCommitRequestItems(xkeys));
            
        GrpcTryCommitManyMutationsResponse? remoteResponse = await client.TryCommitManyMutationsAsync(request, cancellationToken: cancellationToken);

        lock (lockSync)
        {
            foreach (GrpcTryCommitManyMutationsResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, item.ProposalIndex, (KeyValueDurability)item.Durability));
        }
    }
    
    private static IEnumerable<GrpcTryCommitManyMutationsRequestItem> GetCommitRequestItems(List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                ProposalTicketPhysical = key.ticketId.L,
                ProposalTicketCounter = key.ticketId.C,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }
    
    public async Task<KeyValueGetByPrefixResult> GetByPrefix(string node, HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcGetByPrefixRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            PrefixKey = prefixedKey,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcGetByPrefixResponse? remoteResponse = await client.GetByPrefixAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return new((KeyValueResponseType)remoteResponse.Type, GetReadOnlyItem(remoteResponse.Items));
    }
    
    private static List<(string, ReadOnlyKeyValueContext)> GetReadOnlyItem(RepeatedField<GrpcKeyValueByPrefixItemResponse> remoteResponseItems)
    {
        List<(string, ReadOnlyKeyValueContext)> responses = new(remoteResponseItems.Count);
        
        foreach (GrpcKeyValueByPrefixItemResponse? kv in remoteResponseItems)
        {
            byte[]? value;
            
            if (MemoryMarshal.TryGetArray(kv.Value.Memory, out ArraySegment<byte> segment))
                value = segment.Array;
            else
                value = kv.Value.ToByteArray();
            
            responses.Add((kv.Key, new(
                value, 
                kv.Revision, 
                new(kv.ExpiresPhysical, kv.ExpiresCounter),
                new(kv.LastUsedPhysical, kv.LastUsedCounter),
                new(kv.LastModifiedPhysical, kv.LastModifiedCounter),
                (KeyValueState)kv.State
            )));
        }

        return responses;
    }
}