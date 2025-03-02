
using System.Text.Json;
using Flurl;
using Flurl.Http;
using Kahuna.Locks;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Locks;
using Kommander;

namespace Kahuna.Communication.Rest;

public static class MapRoutesExtensions
{
    public static void MapRestKahunaRoutes(this WebApplication app)
    {
        app.MapGet("/", () => "Kahuna.Server");
        
        app.MapPost("/v1/kahuna/lock", async (KahunaLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.Errored };
            
            int partitionId = raft.GetPartitionKey(request.LockName);

            if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
            {
                (LockResponseType response, long fencingToken) = await locks.TryLock(request.LockName, request.LockId, request.ExpiresMs, request.Consistency);

                return new() { Type = response, FencingToken = fencingToken };    
            }
            
            string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = LockResponseType.MustRetry };
            
            logger.LogDebug("LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);

            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);

                KahunaLockResponse? response = await $"http://{leader}"
                    .AppendPathSegments("v1/kahuna/lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaLockResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"http://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }
        });

        app.MapPost("/v1/kahuna/extend-lock", async (KahunaLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger) =>
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
                LockResponseType response = await locks.TryExtendLock(request.LockName, request.LockId, request.ExpiresMs, request.Consistency);

                return new() { Type = response };    
            }
            
            string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = LockResponseType.MustRetry };
            
            logger.LogDebug("EXTEND-LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
                
                KahunaLockResponse? response = await $"http://{leader}"
                    .AppendPathSegments("v1/kahuna/extend-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaLockResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"http://{leader}";

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
                
                KahunaLockResponse? response = await $"http://{leader}"
                    .AppendPathSegments("v1/kahuna/unlock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaLockResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"http://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }
        });

        app.MapPost("/v1/kahuna/get-lock", async (KahunaGetLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.InvalidInput };
            
            int partitionId = raft.GetPartitionKey(request.LockName);

            if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
            {
                (LockResponseType response, ReadOnlyLockContext? context) = await locks.GetLock(request.LockName, request.Consistency);

                if (context is not null)
                    return new()
                    {
                        Type = response, 
                        Owner = context.Owner, 
                        Expires = context.Expires,
                        FencingToken = context.FencingToken
                    };

                return new() { Type = response };
            }
            
            string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = LockResponseType.MustRetry };
            
            logger.LogDebug("GET-LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaGetLockRequest);
                
                KahunaGetLockResponse? response = await $"http://{leader}"
                    .AppendPathSegments("v1/kahuna/get-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaGetLockResponse>();

                if (response is not null)
                    response.ServedFrom = $"http://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                    
                return new() { Type = LockResponseType.Errored };
            }
        });
    }
}