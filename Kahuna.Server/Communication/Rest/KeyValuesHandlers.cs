
using System.Text.Json;
using Flurl;
using Flurl.Http;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;

namespace Kahuna.Communication.Rest;

public static class KeyValuesHandlers
{
    public static void MapKeyValueRoutes(WebApplication app)
    {
        app.MapPost("/v1/kv/try-set", async (KahunaSetKeyValueRequest request, IKahuna keyValues, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new() { Type = KeyValueResponseType.InvalidInput };

            if (request.Value is null)
                return new() { Type = KeyValueResponseType.InvalidInput };
            
            if (request.ExpiresMs < 0)
                return new() { Type = KeyValueResponseType.InvalidInput };
            
            (KeyValueResponseType response, long revision) = await keyValues.LocateAndTrySetKeyValue(
                request.TransactionId,
                request.Key, 
                request.Value,
                request.CompareValue,
                request.CompareRevision,
                request.Flags,
                request.ExpiresMs, 
                request.Consistency,
                CancellationToken.None
            );

            return new KahunaSetKeyValueResponse
            {
                Type = response,
                Revision = revision
            };
        });

        app.MapPost("/v1/kv/try-extend", async (KahunaExtendKeyValueRequest request, IKahuna keyValues, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new() { Type = KeyValueResponseType.InvalidInput };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = KeyValueResponseType.InvalidInput };
            
            int partitionId = raft.GetPartitionKey(request.Key);
            
            if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
            {
                (KeyValueResponseType response, long revision) = await keyValues.TryExtendKeyValue(request.TransactionId, request.Key, request.ExpiresMs, request.Consistency);

                return new() { Type = response, Revision = revision };    
            }
            
            string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = KeyValueResponseType.MustRetry };
            
            logger.LogDebug("EXTEND-KEYVALUE Redirect {LockName} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaExtendKeyValueRequest);
                
                KahunaExtendKeyValueResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/kv/try-extend")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaExtendKeyValueResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = KeyValueResponseType.Errored };
            }
        });

        app.MapPost("/v1/kv/try-delete", async (KahunaDeleteKeyValueRequest request, IKahuna keyValues, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new() { Type = KeyValueResponseType.InvalidInput };
            
            int partitionId = raft.GetPartitionKey(request.Key);

            if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
            {
                (KeyValueResponseType response, long revision) = await keyValues.TryDeleteKeyValue(request.TransactionId, request.Key, request.Consistency);

                return new() { Type = response, Revision = revision };
            }
            
            string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = KeyValueResponseType.MustRetry };
            
            logger.LogDebug("DELETE-KEYVALUE Redirect {LockName} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaDeleteKeyValueRequest);
                
                KahunaDeleteKeyValueResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/kv/try-delete")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaDeleteKeyValueResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = KeyValueResponseType.Errored };
            }
        });

        app.MapPost("/v1/kv/try-get", async (KahunaGetKeyValueRequest request, IKahuna keyValues, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new()
                {
                    Type = KeyValueResponseType.InvalidInput
                };
        
            if (string.IsNullOrEmpty(request.Key))
                return new()
                {
                    Type = KeyValueResponseType.InvalidInput
                };
        
            (KeyValueResponseType type, ReadOnlyKeyValueContext? keyValueContext) = await keyValues.LocateAndTryGetValue(
                request.TransactionId,
                request.Key, 
                request.Consistency, 
                CancellationToken.None
            );
        
            if (keyValueContext is not null)
            {
                KahunaGetKeyValueResponse response = new()
                {
                    ServedFrom = "",
                    Type = type,
                    Value = keyValueContext.Value,
                    Revision = keyValueContext.Revision,
                    Expires = keyValueContext.Expires
                };
                
                return response;
            }

            return new()
            {
                Type = type
            };
        });
        
        app.MapPost("/v1/kv/try-execute-tx", async (KahunaTxKeyValueRequest request, IKahuna keyValues) =>
        {
            if (request.Script is null)
                return new()
                {
                    Type = KeyValueResponseType.InvalidInput
                };
            
            KeyValueTransactionResult result = await keyValues.TryExecuteTx(request.Script, request.Hash);

            return new KahunaTxKeyValueResponse
            {
                ServedFrom = result.ServedFrom,
                Type = result.Type,
                Value = result.Value,
                Revision = result.Revision,
                Expires = result.Expires,
                Reason = result.Reason
            };
        });
    }
}