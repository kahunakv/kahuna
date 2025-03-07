
using System.Collections.Concurrent;
using System.Net.Security;
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Configuration;
using Kahuna.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;

namespace Kahuna.Communication.Grpc;

public class KeyValuesService : KeyValuer.KeyValuerBase
{
    private static readonly ConcurrentDictionary<string, GrpcChannel> channels = new();
    
    private static HttpClientHandler? httpHandler;
    
    private readonly IKahuna keyValues;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft; 
    
    private readonly ILogger<IKahuna> logger;
    
    public KeyValuesService(IKahuna keyValues, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger)
    {
        this.keyValues = keyValues;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
    }

    private HttpClientHandler GetHandler()
    {
        if (httpHandler is not null)
            return httpHandler;
        
        HttpClientHandler handler = new();

        if (string.IsNullOrEmpty(configuration.HttpsCertificate))
            return handler;
        
        handler.ServerCertificateCustomValidationCallback = (httpRequestMessage, cert, cetChain, policyErrors) =>
        {
            // Optionally, check for other policyErrors
            if (policyErrors == SslPolicyErrors.None)
                return true;

            // Compare the certificate's thumbprint to our trusted thumbprint.
            return cert is not null && cert.Thumbprint.Equals(configuration.HttpsTrustedThumbprint, StringComparison.OrdinalIgnoreCase);
          
            //if (cert is not null)
            //    Console.WriteLine("{0} {1}", cert.Thumbprint, configuration.HttpsTrustedThumbprint);
            //return true;
        };

        httpHandler = handler;
        
        return handler;
    }
    
    public override async Task<GrpcTrySetKeyValueResponse> TrySetKeyValue(GrpcTrySetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        if (request.ExpiresMs <= 0)
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        int partitionId = raft.GetPartitionKey(request.Key);

        if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
        {
            KeyValueResponseType response = await keyValues.TrySetKeyValue(request.Key, request.Value, request.ExpiresMs, (KeyValueConsistency)request.Consistency);

            return new()
            {
                Type = (GrpcKeyValueResponseType)response
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry
            };
        
        logger.LogDebug("SET-KEYVALUE Redirect {Key} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
        
        if (!channels.TryGetValue(leader, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress($"https://{leader}", new() { HttpHandler = GetHandler() });
            channels.TryAdd(leader, channel);
        }
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTrySetKeyValueResponse? remoteResponse = await client.TrySetKeyValueAsync(request);
        remoteResponse.ServedFrom = $"https://{leader}";
        return remoteResponse;
    }
    
    public override async Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValue(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        if (request.ExpiresMs <= 0)
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        int partitionId = raft.GetPartitionKey(request.Key);

        if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
        {
            KeyValueResponseType response = await keyValues.TryExtendKeyValue(request.Key, request.ExpiresMs, (KeyValueConsistency)request.Consistency);

            return new()
            {
                Type = (GrpcKeyValueResponseType)response
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        logger.LogDebug("EXTEND-KEYVALUE Redirect {Key} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
        
        if (!channels.TryGetValue(leader, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress($"https://{leader}", new() { HttpHandler = GetHandler() });
            channels.TryAdd(leader, channel);
        }
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryExtendKeyValueResponse? remoteResponse = await client.TryExtendKeyValueAsync(request);
        remoteResponse.ServedFrom = $"https://{leader}";
        return remoteResponse;
    }
    
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
            KeyValueResponseType response = await keyValues.TryDeleteKeyValue(request.Key, (KeyValueConsistency)request.Consistency);

            return new()
            {
                Type = (GrpcKeyValueResponseType)response
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        logger.LogDebug("UNKEYVALUE Redirect {Key} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
        
        if (!channels.TryGetValue(leader, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress($"https://{leader}", new() { HttpHandler = GetHandler() });
            channels.TryAdd(leader, channel);
        }
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryDeleteKeyValueResponse? remoteResponse = await client.TryDeleteKeyValueAsync(request);
        remoteResponse.ServedFrom = $"https://{leader}";
        return remoteResponse;
    }
    
    public override async Task<GrpcTryGetKeyValueResponse> TryGetKeyValue(GrpcTryGetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeInvalidInput
            };
        
        int partitionId = raft.GetPartitionKey(request.Key);

        if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
        {
            (KeyValueResponseType type, ReadOnlyKeyValueContext? keyValueContext) = await keyValues.TryGetValue(request.Key, (KeyValueConsistency)request.Consistency);
            if (type != KeyValueResponseType.Get)
                return new()
                {
                    Type = (GrpcKeyValueResponseType)type
                };

            return new()
            {
                ServedFrom = "",
                Type = (GrpcKeyValueResponseType)type,
                Value = keyValueContext?.Value ?? "",
                ExpiresPhysical = keyValueContext?.Expires.L ?? 0,
                ExpiresCounter = keyValueContext?.Expires.C ?? 0,
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry
            };
        
        logger.LogDebug("GET-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
        
        if (!channels.TryGetValue(leader, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress($"https://{leader}", new() { HttpHandler = GetHandler() });
            channels.TryAdd(leader, channel);
        }
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcTryGetKeyValueResponse? remoteResponse = await client.TryGetKeyValueAsync(request);
        remoteResponse.ServedFrom = $"https://{leader}";
        return remoteResponse;
    }
}