
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Tests.Client;

public class TestLocks
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];
    
    private int total;

    private static string GetRandomLockName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireSingleLock(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(LockConsistency.Ephemeral, LockConsistency.Linearizable)] LockConsistency consistency
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await client.GetOrCreateLock(lockName, 1000, consistency: consistency);

        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestLockAcquisitionAndExpiresWithMilliseconds(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(LockConsistency.Ephemeral, LockConsistency.Linearizable)] LockConsistency consistency
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string lockName = GetRandomLockName();

        // Attempt to acquire the same lock twice:
        KahunaLock kLock1 = await client.GetOrCreateLock(lockName, 10000, consistency: consistency);
        KahunaLock kLock2 = await client.GetOrCreateLock(lockName, 10000, consistency: consistency);

        Assert.True(kLock1.IsAcquired);
        Assert.Equal(0, kLock1.FencingToken);
        
        Assert.False(kLock2.IsAcquired);

        // Dispose the first lock so that the lock becomes available:
        await kLock1.DisposeAsync();
        await kLock2.DisposeAsync();

        // Now try acquiring the lock again:
        await using KahunaLock kLock3 = await client.GetOrCreateLock(lockName, 1000, consistency: consistency);
        Assert.True(kLock3.IsAcquired);
        Assert.Equal(1, kLock3.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireLockExpiresWithTimeSpan(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(LockConsistency.Ephemeral, LockConsistency.Linearizable)] LockConsistency consistency
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string lockName = GetRandomLockName();

        KahunaLock kLock = await client.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await client.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock2.IsAcquired);
        Assert.Equal(1, kLock2.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireLockExpires4(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(LockConsistency.Ephemeral, LockConsistency.Linearizable)] LockConsistency consistency
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string lockName = GetRandomLockName();

        KahunaLock kLock = await client.GetOrCreateLock(
            lockName, 
            expiryTime: 1000, 
            waitTime: 1000, 
            retryTime: 500,
            consistency: consistency
        );
        
        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await client.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock2.IsAcquired);
        Assert.Equal(1, kLock2.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireLockExpiresRace(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(LockConsistency.Ephemeral, LockConsistency.Linearizable)] LockConsistency consistency
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        List<Task> tasks = new(50);

        for (int i = 0; i < 50; i++)
            tasks.Add(AcquireLockConcurrently(client, consistency));

        await Task.WhenAll(tasks);
    }
    
    private async Task AcquireLockConcurrently(KahunaClient client, LockConsistency consistency) 
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await client.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(5), 
            wait: TimeSpan.FromSeconds(5), 
            retry: TimeSpan.FromMilliseconds(500),
            consistency: consistency
        );
        
        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await client.GetOrCreateLock(lockName, TimeSpan.FromSeconds(5), consistency: consistency);
        Assert.True(kLock2.IsAcquired);
        Assert.Equal(1, kLock2.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireSameLockExpiresRace(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.Single, KahunaClientType.Pool)] KahunaClientType clientType, 
        [CombinatorialValues(LockConsistency.Ephemeral, LockConsistency.Linearizable)] LockConsistency consistency
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string lockName = GetRandomLockName();
        
        List<Task> tasks = new(10);

        for (int i = 0; i < 10; i++)
            tasks.Add(AcquireSameLockConcurrently(lockName, client, consistency));

        await Task.WhenAll(tasks);
        
        Assert.Equal(10, total);
    }
    
    private async Task AcquireSameLockConcurrently(string lockName, KahunaClient client, LockConsistency consistency)
    {
        await using KahunaLock kLock = await client.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(10), 
            wait: TimeSpan.FromSeconds(11),
            retry: TimeSpan.FromMilliseconds(500),
            consistency: consistency
        );
        
        if (!kLock.IsAcquired)
            return;

        total++;
    }
    
    /*[Theory]
    [InlineData(KahunaCommunicationType.Grpc, KahunaClientType.Single, LockConsistency.Linearizable)]
    [InlineData(KahunaCommunicationType.Rest, KahunaClientType.Single, LockConsistency.Linearizable)]
    [InlineData(KahunaCommunicationType.Grpc, KahunaClientType.Single, LockConsistency.Ephemeral)]
    [InlineData(KahunaCommunicationType.Rest, KahunaClientType.Single, LockConsistency.Ephemeral)]
    [InlineData(KahunaCommunicationType.Grpc, KahunaClientType.Pool, LockConsistency.Linearizable)]
    [InlineData(KahunaCommunicationType.Rest, KahunaClientType.Pool, LockConsistency.Linearizable)]
    [InlineData(KahunaCommunicationType.Grpc, KahunaClientType.Pool, LockConsistency.Ephemeral)]
    [InlineData(KahunaCommunicationType.Rest, KahunaClientType.Pool, LockConsistency.Ephemeral)]
    public async Task TestValidateAcquireAndExtendLock(KahunaCommunicationType communicationType, KahunaClientType clientType, LockConsistency consistency) 
    {
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await locks.GetOrCreateLock(lockName, 10000, consistency: consistency);

        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);
        
        (bool extended, long fencingToken) = await kLock.TryExtend(TimeSpan.FromSeconds(10));
        Assert.True(extended);
        Assert.Equal(kLock.FencingToken, fencingToken);

        KahunaLockInfo? lockInfo = await locks.GetLockInfo(lockName, consistency);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.LockId);
        HLCTimestamp expires = lockInfo.Expires;
        
        (extended, fencingToken) = await kLock.TryExtend(TimeSpan.FromSeconds(10));
        Assert.True(extended);
        Assert.Equal(kLock.FencingToken, fencingToken);
        
        lockInfo = await locks.GetLockInfo(lockName, consistency);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.LockId);
        Assert.True(lockInfo.Expires - expires > TimeSpan.Zero);
        
        //Assert.Equal(lockInfo.Expires > DateTime.UtcNow, true);
    }
    
    [Theory]
    [InlineData(KahunaCommunicationType.Grpc, KahunaClientType.Single, LockConsistency.Linearizable)]
    [InlineData(KahunaCommunicationType.Rest, KahunaClientType.Single, LockConsistency.Linearizable)]
    [InlineData(KahunaCommunicationType.Grpc, KahunaClientType.Single, LockConsistency.Ephemeral)]
    [InlineData(KahunaCommunicationType.Rest, KahunaClientType.Single, LockConsistency.Ephemeral)]
    [InlineData(KahunaCommunicationType.Grpc, KahunaClientType.Pool, LockConsistency.Linearizable)]
    [InlineData(KahunaCommunicationType.Rest, KahunaClientType.Pool, LockConsistency.Linearizable)]
    [InlineData(KahunaCommunicationType.Grpc, KahunaClientType.Pool, LockConsistency.Ephemeral)]
    [InlineData(KahunaCommunicationType.Rest, KahunaClientType.Pool, LockConsistency.Ephemeral)]
    public async Task TestAdquireLockAndGetInfo(KahunaCommunicationType communicationType, KahunaClientType clientType, LockConsistency consistency) 
    {
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await locks.GetOrCreateLock(lockName, 10000, consistency: consistency);

        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);
        
        (bool extended, long fencingToken) = await kLock.TryExtend(TimeSpan.FromSeconds(10));
        Assert.True(extended);
        Assert.Equal(kLock.FencingToken, fencingToken);

        KahunaLockInfo? lockInfo = await kLock.GetInfo();
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.LockId);
        HLCTimestamp expires = lockInfo.Expires;
        
        (extended, fencingToken) = await kLock.TryExtend(TimeSpan.FromSeconds(10));
        Assert.True(extended);
        Assert.Equal(kLock.FencingToken, fencingToken);
        
        lockInfo = await kLock.GetInfo();
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.LockId);
        Assert.True(lockInfo.Expires - expires > TimeSpan.Zero);
        
        //Assert.Equal(lockInfo.Expires > DateTime.UtcNow, true);
    }*/
    
    private KahunaClient GetClientByType(KahunaCommunicationType communicationType, KahunaClientType clientType)
    {
        return clientType switch
        {
            KahunaClientType.Single => new(url, null, GetCommunicationByType(communicationType)),
            KahunaClientType.Pool => new(urls, null, GetCommunicationByType(communicationType)),
            _ => throw new ArgumentOutOfRangeException(nameof(clientType), clientType, null)
        };
    }

    private IKahunaCommunication GetCommunicationByType(KahunaCommunicationType communicationType)
    {
        return communicationType switch
        {
            KahunaCommunicationType.Grpc => new GrpcCommunication(null),
            KahunaCommunicationType.Rest => new RestCommunication(null),
            _ => throw new ArgumentOutOfRangeException(nameof(communicationType), communicationType, null)
        };
    }
}
