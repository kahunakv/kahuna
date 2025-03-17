
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;

namespace Kahuna.Communication.Grpc;

public class KeyValuesService : KeyValuer.KeyValuerBase
{
    private readonly IKahuna keyValues;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft; 
    
    private readonly ILogger<IKahuna> logger;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="keyValues"></param>
    /// <param name="configuration"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public KeyValuesService(IKahuna keyValues, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger)
    {
        this.keyValues = keyValues;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
    }
    
    /// <summary>
    /// Receives requests the key/value "set" service
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTrySetKeyValueResponse> TrySetKeyValue(GrpcTrySetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        if (request.ExpiresMs < 0)
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        (KeyValueResponseType response, long revision) = await keyValues.LocateAndTrySetKeyValue(
            request.Key, 
            request.Value?.ToByteArray(),
            request.CompareValue?.ToByteArray(),
            request.CompareRevision,
            (KeyValueFlags)request.Flags,
            request.ExpiresMs, 
            (KeyValueConsistency)request.Consistency,
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)response,
            Revision = revision
        };
    }
    
    /// <summary>
    /// Receives requests the key/value "extend" service
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValue(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        if (request.ExpiresMs < 0)
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        int partitionId = raft.GetPartitionKey(request.Key);

        if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
        {
            (KeyValueResponseType response, long revision) = await keyValues.TryExtendKeyValue(request.Key, request.ExpiresMs, (KeyValueConsistency)request.Consistency);

            return new()
            {
                Type = (GrpcKeyValueResponseType)response,
                Revision = revision
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        logger.LogDebug("EXTEND-KEYVALUE Redirect {Key} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryExtendKeyValueResponse? remoteResponse = await client.TryExtendKeyValueAsync(request);
        remoteResponse.ServedFrom = $"https://{leader}";
        return remoteResponse;
    }
    
    /// <summary>
    /// Receives requests for the key/value "delete" service
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryDeleteKeyValueResponse> TryDeleteKeyValue(GrpcTryDeleteKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        int partitionId = raft.GetPartitionKey(request.Key);

        if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
        {
            (KeyValueResponseType response, long revision) = await keyValues.TryDeleteKeyValue(request.Key, (KeyValueConsistency)request.Consistency);

            return new()
            {
                Type = (GrpcKeyValueResponseType)response,
                Revision = revision
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        logger.LogDebug("DELETE-KEYVALUE Redirect {Key} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryDeleteKeyValueResponse? remoteResponse = await client.TryDeleteKeyValueAsync(request);
        remoteResponse.ServedFrom = $"https://{leader}";
        return remoteResponse;
    }
    
    /// <summary>
    /// Receives requests for the key/value "get" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryGetKeyValueResponse> TryGetKeyValue(GrpcTryGetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? keyValueContext) = await keyValues.LocateAndTryGetValue(request.Key, (KeyValueConsistency)request.Consistency, context.CancellationToken);
        
        if (keyValueContext is not null)
        {
            GrpcTryGetKeyValueResponse response = new()
            {
                ServedFrom = "",
                Type = (GrpcKeyValueResponseType)type,
                Revision = keyValueContext.Revision,
                ExpiresPhysical = keyValueContext.Expires.L,
                ExpiresCounter = keyValueContext.Expires.C,
            };

            if (keyValueContext.Value is not null)
                response.Value = ByteString.CopyFrom(keyValueContext.Value);

            return response;
        }

        return new()
        {
            Type = (GrpcKeyValueResponseType)type
        };
    }
}