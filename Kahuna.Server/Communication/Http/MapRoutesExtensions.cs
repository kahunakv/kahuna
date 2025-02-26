
using System.Text.Json;
using Flurl;
using Flurl.Http;
using Kahuna.Locks;
using Kommander;

namespace Kahuna.Communication.Http;

public static class MapRoutesExtensions
{
    public static void MapKahunaRoutes(this WebApplication app)
    {
        app.MapPost("/v1/kahuna/lock", async (ExternLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.Errored };
            
            // Console.WriteLine("LOCK {0} {1} {2}", request.LockName, request.LockId, request.ExpiresMs);
            
            int partitionId = (int)raft.GetPartitionKey(request.LockName);

            if (!raft.Joined || await raft.AmILeader(partitionId))
            {
                (LockResponseType response, long fencingToken) = await locks.TryLock(request.LockName, request.LockId, request.ExpiresMs, request.Consistency);

                return new() { Type = response, FencingToken = fencingToken };    
            }
            
            string leader = await raft.WaitForLeader(partitionId);
            if (leader == raft.GetLocalEndpoint())
                throw new RaftException("lol");
            
            logger.LogInformation("LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);

            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.ExternLockRequest);

                return await $"http://{leader}"
                    .AppendPathSegments("v1/kahuna/lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<ExternLockResponse>();
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }
        });

        app.MapPost("/v1/kahuna/extend-lock", async (ExternLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.Errored };
            
            int partitionId = (int)raft.GetPartitionKey(request.LockName);
            
            if (!raft.Joined || await raft.AmILeader(partitionId))
            {
                LockResponseType response = await locks.TryExtendLock(request.LockName, request.LockId, request.ExpiresMs, request.Consistency);

                return new() { Type = response };    
            }
            
            string leader = await raft.WaitForLeader(partitionId);
            if (leader == raft.GetLocalEndpoint())
                throw new RaftException("lol");
            
            logger.LogInformation("EXTEND-LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.ExternLockRequest);
                
                return await $"http://{leader}"
                    .AppendPathSegments("v1/kahuna/extend-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<ExternLockResponse>();
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }
        });

        app.MapPost("/v1/kahuna/unlock", async (ExternLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            int partitionId = (int)raft.GetPartitionKey(request.LockName);

            if (!raft.Joined || await raft.AmILeader(partitionId))
            {
                LockResponseType response = await locks.TryUnlock(request.LockName, request.LockId, request.Consistency);

                return new() { Type = response };
            }
            
            string leader = await raft.WaitForLeader(partitionId);
            if (leader == raft.GetLocalEndpoint())
                throw new RaftException("lol");
            
            logger.LogInformation("UNLOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.ExternLockRequest);
                
                return await $"http://{leader}"
                    .AppendPathSegments("v1/kahuna/unlock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<ExternLockResponse>();
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }
        });

        app.MapPost("/v1/kahuna/get-lock", async (ExternGetLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };
            
            int partitionId = (int)raft.GetPartitionKey(request.LockName);

            if (!raft.Joined || await raft.AmILeader(partitionId))
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
            
            string leader = await raft.WaitForLeader(partitionId);
            if (leader == raft.GetLocalEndpoint())
                throw new RaftException("lol");
            
            logger.LogInformation("GET-LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.ExternGetLockRequest);
                
                return await $"http://{leader}"
                    .AppendPathSegments("v1/kahuna/get-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<ExternGetLockResponse>();
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                    
                return new() { Type = LockResponseType.Errored };
            }
        });
    }
}