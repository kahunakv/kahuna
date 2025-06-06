
using Kommander;

using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Locks;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Provides methods for mapping routes related to lock operations
/// in a RESTful web application environment.
/// </summary>
public static class LocksHandlers
{
    public static void MapLocksRoutes(WebApplication app)
    {
        app.MapPost("/v1/locks/try-lock", async (KahunaLockRequest request, IKahuna locks, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Resource) || request.Owner is null || request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.InvalidInput };

            (LockResponseType response, long fencingToken)  = await locks.LocateAndTryLock(
                request.Resource, 
                request.Owner, 
                request.ExpiresMs, 
                request.Durability, 
                cancellationToken
            );

            return new KahunaLockResponse
            {
                Type = response,
                FencingToken = fencingToken,
                ServedFrom = ""
            };
            
            /*if (string.IsNullOrEmpty(request.Resource))
                return new() { Type = LockResponseType.Errored };

            if (request.Owner is null)
                return new() { Type = LockResponseType.Errored };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.Errored };
            
            int partitionId = raft.GetPartitionKey(request.Resource);

            if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            {
                (LockResponseType response, long fencingToken) = await locks.TryLock(request.Resource, request.Owner, request.ExpiresMs, request.Durability);

                return new() { Type = response, FencingToken = fencingToken };    
            }
            
            string leader = await raft.WaitForLeader(partitionId, cancellationToken);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = LockResponseType.MustRetry };
            
            logger.LogDebug("LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.Resource, partitionId, leader);

            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);

                KahunaLockResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/locks/try-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaLockResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }*/
        });

        app.MapPost("/v1/locks/try-extend", async (KahunaLockRequest request, IKahuna locks, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Resource) || request.Owner is null || request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.InvalidInput };

            (LockResponseType response, long fencingToken) = await locks.LocateAndTryExtendLock(
                request.Resource, 
                request.Owner, 
                request.ExpiresMs, 
                request.Durability, 
                cancellationToken
            );

            return new KahunaLockResponse
            {
                Type = response,
                FencingToken = fencingToken,
                ServedFrom = ""
            };
            
            /*if (string.IsNullOrEmpty(request.Resource))
                return new() { Type = LockResponseType.InvalidInput };

            if (request.Owner is null)
                return new() { Type = LockResponseType.InvalidInput };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.InvalidInput };
            
            int partitionId = raft.GetPartitionKey(request.Resource);
            
            if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            {
                (LockResponseType response, long fencingToken) = await locks.TryExtendLock(request.Resource, request.Owner, request.ExpiresMs, request.Durability);

                return new() { Type = response, FencingToken = fencingToken };    
            }
            
            string leader = await raft.WaitForLeader(partitionId, cancellationToken);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = LockResponseType.MustRetry };
            
            logger.LogDebug("EXTEND-LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.Resource, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
                
                KahunaLockResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/locks/try-extend")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaLockResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }*/
        });

        app.MapPost("/v1/locks/try-unlock", async (KahunaLockRequest request, IKahuna locks, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Resource) || request.Owner is null)
                return new() { Type = LockResponseType.InvalidInput };

            LockResponseType response = await locks.LocateAndTryUnlock(
                request.Resource, 
                request.Owner, 
                request.Durability, 
                cancellationToken
            );

            return new KahunaLockResponse()
            {
                Type = response,
                ServedFrom = ""
            };
            
            /*int partitionId = raft.GetPartitionKey(request.Resource);

            if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            {
                LockResponseType response = await locks.TryUnlock(request.Resource, request.Owner, request.Durability);

                return new() { Type = response };
            }
            
            string leader = await raft.WaitForLeader(partitionId, cancellationToken);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = LockResponseType.MustRetry };
            
            logger.LogDebug("UNLOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.Resource, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
                
                KahunaLockResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/locks/try-unlock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaLockResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = LockResponseType.Errored };
            }*/
        });

        app.MapPost("/v1/locks/get-info", async (KahunaGetLockRequest request, IKahuna locks, IRaft raft, ILogger<IKahuna> logger, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Resource))
                return new() { Type = LockResponseType.InvalidInput };
            
            (LockResponseType type, ReadOnlyLockEntry? lockContext) = await locks.LocateAndGetLock(
                request.Resource, 
                request.Durability, 
                cancellationToken
            );
        
            if (type != LockResponseType.Got)
                return new()
                {
                    Type = type
                };

            return new KahunaGetLockResponse
            {
                Type = type,
                Owner = lockContext?.Owner,
                FencingToken = lockContext?.FencingToken ?? 0,
                Expires = new(lockContext?.Expires.N ?? 0, lockContext?.Expires.L ?? 0, lockContext?.Expires.C ?? 0),
                ServedFrom = ""
            };
            
            /*int partitionId = raft.GetPartitionKey(request.LockName);

            if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
            {
                (LockResponseType response, ReadOnlyLockContext? context) = await locks.GetLock(request.LockName, request.Durability);

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
                
                KahunaGetLockResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/locks/get-info")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaGetLockResponse>();

                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                    
                return new() { Type = LockResponseType.Errored };
            }*/
        });
    }
}