using System.Text.Json;
using Flurl;
using Flurl.Http;
using Kommander;

namespace Kahuna;

public static class MapRoutesExtensions
{
    public static void MapKahunaRoutes(this WebApplication app)
    {
        app.MapPost("/v1/kahuna/lock", async (ExternLockRequest request, IKahuna locks, IRaft raft) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.Errored };
            
            // Console.WriteLine("LOCK {0} {1} {2}", request.LockName, request.LockId, request.ExpiresMs);
            
            int partitionId = Math.Abs(request.LockName.GetHashCode()) % 3;

            if (await raft.AmILeader(partitionId))
            {
                (LockResponseType response, long fencingToken) = await locks.TryLock(request.LockName, request.LockId, request.ExpiresMs, request.Consistency);

                return new() { Type = response, FencingToken = fencingToken };    
            }

            string leader = await raft.WaitForLeader(partitionId);
            if (leader == raft.GetLocalEndpoint())
                throw new RaftException("lol");
            
            string payload = JsonSerializer.Serialize(request);
            
            Console.WriteLine("{0} {1}", leader, payload);

            return await $"http://{leader}"
                .AppendPathSegments("v1/kahuna/lock")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(5)
                //.WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload)
                .ReceiveJson<ExternLockResponse>();
        });

        app.MapPost("/v1/kahuna/extend-lock", async (ExternLockRequest request, IKahuna locks, IRaft raft) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            if (request.ExpiresMs <= 0)
                return new() { Type = LockResponseType.Errored };
            
            // Console.WriteLine("EXTEND-LOCK {0} {1} {2}", request.LockName, request.LockId, request.ExpiresMs);
            
            int partitionId = Math.Abs(request.LockName.GetHashCode()) % raft.Configuration.MaxPartitions;
            
            if (await raft.AmILeader(partitionId))
            {
                LockResponseType response = await locks.TryExtendLock(request.LockName, request.LockId, request.ExpiresMs, request.Consistency);

                return new() { Type = response };    
            }
            
            string leader = await raft.WaitForLeader(partitionId);
            string payload = JsonSerializer.Serialize(request);
            
            return await $"http://{leader}"
                .AppendPathSegments("v1/kahuna/extend-lock")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(5)
                //.WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload)
                .ReceiveJson<ExternLockResponse>();
        });

        app.MapPost("/v1/kahuna/unlock", async (ExternLockRequest request, IKahuna locks, IRaft raft) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            // Console.WriteLine("UNLOCK {0} {1} {2}", request.LockName, request.LockId, request.ExpiresMs);
            
            int partitionId = Math.Abs(request.LockName.GetHashCode()) % raft.Configuration.MaxPartitions;

            if (await raft.AmILeader(partitionId))
            {
                LockResponseType response = await locks.TryUnlock(request.LockName, request.LockId, request.Consistency);

                return new() { Type = response };
            }
            
            string leader = await raft.WaitForLeader(partitionId);
            if (leader == raft.GetLocalEndpoint())
                throw new RaftException("lol");
            
            string payload = JsonSerializer.Serialize(request);
            
            return await $"http://{leader}"
                .AppendPathSegments("v1/kahuna/unlock")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(5)
                //.WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload)
                .ReceiveJson<ExternLockResponse>();
        });

        app.MapPost("/v1/kahuna/get-lock", async (ExternGetLockRequest request, IKahuna locks, IRaft raft) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };
            
            // Console.WriteLine("UNLOCK {0} {1} {2}", request.LockName, request.LockId, request.ExpiresMs);
            
            int partitionId = Math.Abs(request.LockName.GetHashCode()) % raft.Configuration.MaxPartitions;

            if (await raft.AmILeader(partitionId))
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
            
            string payload = JsonSerializer.Serialize(request);
            
            return await $"http://{leader}"
                .AppendPathSegments("v1/kahuna/get-lock")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(5)
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload)
                .ReceiveJson<ExternGetLockResponse>();
        });
    }
}