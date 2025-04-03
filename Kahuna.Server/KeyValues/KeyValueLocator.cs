
using System.Collections.Concurrent;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Net.Client;

using Kahuna.Communication.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Shared.KeyValue;

using Kommander;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueLocator
{
    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;
    
    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;
    
    public KeyValueLocator(KeyValuesManager manager, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger)
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TrySet request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0);
        
        if (expiresMs < 0)
            return (KeyValueResponseType.InvalidInput, 0);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
        {
            return await manager.TrySetKeyValue(
                transactionId,
                key, 
                value,
                compareValue,
                compareRevision,
                flags,
                expiresMs, 
                durability
            );
        }
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0);
        
        logger.LogDebug("SET-KEYVALUE Redirect {Key} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
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
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryDelete request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryDeleteKeyValue(transactionId, key, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0);
        
        logger.LogDebug("DELETE-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryDeleteKeyValueRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryDeleteKeyValueResponse? remoteResponse = await client.TryDeleteKeyValueAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExtend request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryExtendKeyValue(transactionId, key, expiresMs, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0);
        
        logger.LogDebug("EXTEND-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
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
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, null);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryGetValue(transactionId, key, revision, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);
        
        logger.LogDebug("GET-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
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
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, new(
            remoteResponse.Value?.ToByteArray(),
            remoteResponse.Revision,
            new(remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter)
        ));
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExistsValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, null);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryExistsValue(transactionId, key, revision, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);
        
        logger.LogDebug("EXISTS-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
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
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, new(
            null,
            remoteResponse.Revision,
            new(remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter)
        ));
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string, KeyValueDurability)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, key, durability);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, key, durability);
        
        logger.LogDebug("ACQUIRE-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
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
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, key, durability);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryAcquireManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, int expiresMs, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, int expiresMs, KeyValueDurability durability)>> acquisitionPlan = [];

        foreach ((string key, int expiresMs, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, key.durability)];

            int partitionId = raft.GetPartitionKey(key.key);
            string leader = await raft.WaitForLeader(partitionId, cancelationToken);
            
            if (leader != localNode)
                logger.LogDebug("ACQUIRE-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
            
            if (acquisitionPlan.TryGetValue(leader, out List<(string key, int expiresMs, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = [];
        
        foreach ((string? leader, List<(string key, int expiresMs, KeyValueDurability durability)>? xkeys) in acquisitionPlan)
        {
            if (leader == localNode)
            {
                List<(KeyValueResponseType type, string key, KeyValueDurability durability)> acquireResponses = await manager.TryAcquireManyExclusiveLock(transactionId, xkeys);

                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) item in acquireResponses)
                    responses.Add((item.type, item.key, item.durability));
                
                continue;
            }
            
            GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
            
            KeyValuer.KeyValuerClient client = new(channel);
            
            GrpcTryAcquireManyExclusiveLocksRequest request = new()
            {
                TransactionIdPhysical = transactionId.L,
                TransactionIdCounter = transactionId.C
            };
            
            request.Items.Add(GetRequestItems(xkeys));
            
            GrpcTryAcquireManyExclusiveLocksResponse? remoteResponse = await client.TryAcquireManyExclusiveLocksAsync(request, cancellationToken: cancelationToken);

            foreach (GrpcTryAcquireManyExclusiveLocksResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability));
        }

        return responses;
    }

    private static IEnumerable<GrpcTryAcquireManyExclusiveLocksRequestItem> GetRequestItems(List<(string key, int expiresMs, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, int expiresMs, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                ExpiresMs = key.expiresMs,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, key);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryReleaseExclusiveLock(transactionId, key, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, key);
        
        logger.LogDebug("RELEASE-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryReleaseExclusiveLockRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryReleaseExclusiveLockResponse? remoteResponse = await client.TryReleaseExclusiveLockAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, key);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, HLCTimestamp.Zero, key, durability);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryPrepareMutations(transactionId, key, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, key, durability);
        
        logger.LogDebug("PREPARE-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryPrepareMutationsRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryPrepareMutationsResponse? remoteResponse = await client.TryPrepareMutationsAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, new(remoteResponse.ProposalTicketPhysical, remoteResponse.ProposalTicketCounter), key, durability);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryCommitMutations(transactionId, key, ticketId, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0);
        
        logger.LogDebug("COMMIT-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
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
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.ProposalIndex);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryRollbackMutations(transactionId, key, ticketId, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0);
        
        logger.LogDebug("ROLLBACK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryRollbackMutationsRequest request = new()
        {
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ProposalTicketPhysical = ticketId.L,
            ProposalTicketCounter = ticketId.C,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcTryRollbackMutationsResponse? remoteResponse = await client.TryRollbackMutationsAsync(request, cancellationToken: cancelationToken);
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.ProposalIndex);
    }

    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix 
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByPrefixResult> ScanAllByPrefix(string prefixKeyName, KeyValueDurability durability)
    {
        ConcurrentBag<(string, ReadOnlyKeyValueContext)> unionItems = [];
        
        KeyValueGetByPrefixResult items = await manager.ScanByPrefix(prefixKeyName, durability);

        IList<RaftNode> nodes = raft.GetNodes();
        
        List<Task> tasks = new(nodes.Count);
        
        foreach (RaftNode node in nodes)
            tasks.Add(NodeScanByPrefix(unionItems, node, prefixKeyName, durability));
        
        await Task.WhenAll(tasks);
        
        foreach ((string, ReadOnlyKeyValueContext) item in unionItems)
            items.Items.Add(item);

        return items;
    }

    private async Task NodeScanByPrefix(ConcurrentBag<(string, ReadOnlyKeyValueContext)> unionItems, RaftNode node, string prefixKeyName, KeyValueDurability durability)
    {
        GrpcChannel channel = SharedChannels.GetChannel(node.Endpoint, configuration);
            
        GrpcScanByPrefixRequest request = new()
        {
            PrefixKey = prefixKeyName,
            Durability = (GrpcKeyValueDurability)durability,
        };
            
        KeyValuer.KeyValuerClient client = new(channel);

        GrpcScanByPrefixResponse? response = await client.ScanByPrefixAsync(request);

        if (response.Type == GrpcKeyValueResponseType.TypeGot)
        {
            foreach (GrpcKeyValueByPrefixItemResponse item in response.Items)
                unionItems.Add(ScanByPrefixItems(item));
        }
    }

    private static (string, ReadOnlyKeyValueContext) ScanByPrefixItems(GrpcKeyValueByPrefixItemResponse item)
    {
        return (item.Key, new(
            item.Value?.ToByteArray(),
            item.Revision,
            new(item.ExpiresPhysical, item.ExpiresCounter)
        ));
    }
}