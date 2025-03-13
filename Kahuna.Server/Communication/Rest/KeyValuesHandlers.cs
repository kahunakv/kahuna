using System.Text.Json;
using Flurl;
using Flurl.Http;
using Kahuna.KeyValues;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander;
using Kommander.Time;

namespace Kahuna.Communication.Rest;

public static class KeyValuesHandlers
{
    public static void MapKeyValueRoutes(WebApplication app)
    {
        app.MapPost("/v1/kv/try-set", async (KahunaSetKeyValueRequest request, IKahuna keyValues, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new() { Type = KeyValueResponseType.Errored };

            if (string.IsNullOrEmpty(request.Value))
                return new() { Type = KeyValueResponseType.Errored };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = KeyValueResponseType.Errored };
            
            int partitionId = raft.GetPartitionKey(request.Key);

            if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
            {
                (KeyValueResponseType response, long revision) = await keyValues.TrySetKeyValue(
                    request.Key, 
                    request.Value, 
                    null, 0, 
                    request.Flags, 
                    request.ExpiresMs, 
                    request.Consistency
                );

                return new() { Type = response, Revision = revision };    
            }
            
            string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = KeyValueResponseType.MustRetry };
            
            logger.LogDebug("SET-KEYVALUE Redirect {Key} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);

            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);

                KahunaSetKeyValueResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaSetKeyValueResponse>();
                
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

        /*app.MapPost("/v1/kahuna/extend-lock", async (KahunaLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.InvalidInput };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.InvalidInput };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.InvalidInput };
            
            int partitionId = raft.GetPartitionKey(request.LockName);
            
            if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
            {
                (LockResponseType response, long fencingToken) = await locks.TryExtendLock(request.LockName, request.LockId, request.ExpiresMs, request.Consistency);

                return new() { Type = response, FencingToken = fencingToken };    
            }
            
            string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = LockResponseType.MustRetry };
            
            logger.LogDebug("EXTEND-LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
                
                KahunaLockResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/kahuna/extend-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaLockResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }
        });

        app.MapPost("/v1/kahuna/unlock", async (KahunaLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.InvalidInput };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.InvalidInput };
            
            int partitionId = raft.GetPartitionKey(request.LockName);

            if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
            {
                LockResponseType response = await locks.TryUnlock(request.LockName, request.LockId, request.Consistency);

                return new() { Type = response };
            }
            
            string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = LockResponseType.MustRetry };
            
            logger.LogDebug("UNLOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
                
                KahunaLockResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/kahuna/unlock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaLockResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }
        });*/

        app.MapPost("/v1/kv/try-get", async (KahunaGetKeyValueRequest request, IKahuna keyValues, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new()
                {
                    Type = KeyValueResponseType.InvalidInput
                };
        
            int partitionId = raft.GetPartitionKey(request.Key);

            if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
            {
                (KeyValueResponseType type, ReadOnlyKeyValueContext? keyValueContext) = await keyValues.TryGetValue(request.Key, request.Consistency);
                if (type != KeyValueResponseType.Get)
                    return new()
                    {
                        Type = type
                    };

                return new()
                {
                    ServedFrom = "",
                    Type = type,
                    Value = keyValueContext?.Value ?? "",
                    Revision = keyValueContext?.Revision ?? 0,
                    Expires = keyValueContext?.Expires ?? HLCTimestamp.Zero
                };
            }
            
            string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
            if (leader == raft.GetLocalEndpoint())
                return new()
                {
                    Type = KeyValueResponseType.MustRetry
                };
            
            logger.LogDebug("GET-KEYVALUE Redirect {Key} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaGetKeyValueRequest);
                
                KahunaGetKeyValueResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/kv/try-get")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaGetKeyValueResponse>();

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
    }
}