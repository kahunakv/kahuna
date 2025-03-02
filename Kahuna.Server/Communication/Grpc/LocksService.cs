
using System.Net.Security;
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Configuration;
using Kahuna.Shared.Locks;
using Kommander;

namespace Kahuna.Communication.Grpc;

public class LocksService : Locker.LockerBase
{
    private static HttpClientHandler? httpHandler;
    
    private readonly IKahuna locks;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft; 
    
    private readonly ILogger<IKahuna> logger;
    
    public LocksService(IKahuna locks, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger)
    {
        this.locks = locks;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
    }

    public HttpClientHandler GetHandler()
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
        };

        httpHandler = handler;
        
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
        
        logger.LogDebug("LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);
        
        GrpcChannel channel = GrpcChannel.ForAddress($"https://{leader}", new() { HttpHandler = GetHandler() });
        
        Locker.LockerClient client = new(channel);
        
        return await client.TryLockAsync(request);
    }
}
