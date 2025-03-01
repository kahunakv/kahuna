
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Shared.Locks;
using Kommander;

namespace Kahuna.Communication.Grpc;

public class LocksService : Locker.LockerBase
{
    private readonly IKahuna locks;

    private readonly IRaft raft; 
    
    private readonly ILogger<IKahuna> logger;
    
    public LocksService(IKahuna locks, IRaft raft, ILogger<IKahuna> logger)
    {
        this.locks = locks;
        this.raft = raft;
        this.logger = logger;
    }

    public HttpClientHandler GetHandler()
    {
        HttpClientHandler handler = new();
        
        handler.ServerCertificateCustomValidationCallback = (httpRequestMessage, cert, cetChain, policyErrors) =>
        {
            // Optionally, check for other policyErrors
            if (policyErrors == System.Net.Security.SslPolicyErrors.None)
                return true;

            // Compare the certificate's thumbprint to our trusted thumbprint.
            //if (cert is X509Certificate2 certificate && certificate.Thumbprint.Equals(trustedThumbprint, StringComparison.OrdinalIgnoreCase))
            //{
                return true;
            //}

            //return false;
        };
        
        return handler;
    }
    
    public override async Task<TryLockResponse> TryLock(TryLockRequest request, ServerCallContext context)
    {
        int partitionId = raft.GetPartitionKey(request.LockName);

        if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
        {
            (LockResponseType response, long fencingToken) = await locks.TryLock(request.LockName, request.LockId, request.ExpiresMs, (LockConsistency)request.Consistency);

            return new()
            {
                Type = (GrpcLockResponseType)response,
                FencingToken = fencingToken
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeMustRetry
            };
        
        logger.LogInformation("LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);
        
        GrpcChannel channel = GrpcChannel.ForAddress($"https://{leader}", new() { HttpHandler = GetHandler() });
        
        Locker.LockerClient client = new(channel);
        
        return await client.TryLockAsync(request);
    }
}
