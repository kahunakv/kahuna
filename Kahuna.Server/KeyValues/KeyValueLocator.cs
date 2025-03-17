
using Google.Protobuf;
using Grpc.Net.Client;

using Kahuna.Communication.Grpc;
using Kahuna.Configuration;
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
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTrySetKeyValue(
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueConsistency consistency,
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
                key, 
                value,
                compareValue,
                compareRevision,
                flags,
                expiresMs, 
                consistency
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
            Key = key,
            CompareRevision = compareRevision,
            Flags = (GrpcKeyValueFlags) flags,
            ExpiresMs = expiresMs,
            Consistency = (GrpcKeyValueConsistency) consistency,
        };
        
        if (value is not null)
            request.Value = ByteString.CopyFrom(value);
        
        if (compareValue is not null)
            request.CompareValue = ByteString.CopyFrom(compareValue);
        
        GrpcTrySetKeyValueResponse? remoteResponse = await client.TrySetKeyValueAsync(request);
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.Revision);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(string key, KeyValueConsistency consistency, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, null);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryGetValue(key, consistency);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);
        
        logger.LogDebug("GET-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryGetKeyValueRequest request = new()
        {
            Key = key,
            Consistency = (GrpcKeyValueConsistency)consistency,
        };
        
        GrpcTryGetKeyValueResponse? remoteResponse = await client.TryGetKeyValueAsync(request);
        
        remoteResponse.ServedFrom = $"https://{leader}";
        
        return ((KeyValueResponseType)remoteResponse.Type, new ReadOnlyKeyValueContext(
            remoteResponse.Value?.ToByteArray(),
            remoteResponse.Revision,
            new HLCTimestamp(remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter)
        ));
    }
}