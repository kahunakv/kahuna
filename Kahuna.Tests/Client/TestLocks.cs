
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
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability,
        [CombinatorialValues(true, false)] bool upgradeUrls
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType, upgradeUrls);
        
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await client.GetOrCreateLock(lockName, 1000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestLockAcquisitionAndExpiresWithMilliseconds(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability,
        [CombinatorialValues(true, false)] bool upgradeUrls
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType, upgradeUrls);
        
        string lockName = GetRandomLockName();

        // Attempt to acquire the same lock twice:
        KahunaLock kLock1 = await client.GetOrCreateLock(lockName, 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        KahunaLock kLock2 = await client.GetOrCreateLock(lockName, 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(kLock1.IsAcquired);
        Assert.Equal(0, kLock1.FencingToken);
        
        Assert.False(kLock2.IsAcquired);

        // Dispose the first lock so that the lock becomes available:
        await kLock1.DisposeAsync();
        await kLock2.DisposeAsync();

        // Now try acquiring the lock again:
        await using KahunaLock kLock3 = await client.GetOrCreateLock(lockName, 1000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(kLock3.IsAcquired);
        Assert.Equal(1, kLock3.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireLockExpiresWithTimeSpan(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability,
        [CombinatorialValues(true, false)] bool upgradeUrls
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType, upgradeUrls);
        
        string lockName = GetRandomLockName();

        KahunaLock kLock = await client.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await client.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(kLock2.IsAcquired);
        Assert.Equal(1, kLock2.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireLockExpires4(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability,
        [CombinatorialValues(true, false)] bool upgradeUrls
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType, upgradeUrls);
        
        string lockName = GetRandomLockName();

        KahunaLock kLock = await client.GetOrCreateLock(
            lockName, 
            expiryTime: 1000, 
            waitTime: 1000, 
            retryTime: 500,
            durability: durability, 
            TestContext.Current.CancellationToken
        );
        
        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await client.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), durability: durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(kLock2.IsAcquired);
        Assert.Equal(1, kLock2.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireLockExpiresRace(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability,
        [CombinatorialValues(true, false)] bool upgradeUrls
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType, upgradeUrls);
        
        List<Task> tasks = new(50);

        for (int i = 0; i < 50; i++)
            tasks.Add(AcquireLockConcurrently(client, durability));

        await Task.WhenAll(tasks);
    }
    
    private static async Task AcquireLockConcurrently(KahunaClient client, LockDurability durability) 
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await client.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(5), 
            wait: TimeSpan.FromSeconds(5), 
            retry: TimeSpan.FromMilliseconds(500),
            durability: durability
        );
        
        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await client.GetOrCreateLock(lockName, TimeSpan.FromSeconds(5), durability: durability);
        Assert.True(kLock2.IsAcquired);
        Assert.Equal(1, kLock2.FencingToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireSameLockExpiresRace(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability,
        [CombinatorialValues(true, false)] bool upgradeUrls
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType, upgradeUrls);
        
        string lockName = GetRandomLockName();
        
        List<Task> tasks = new(10);

        for (int i = 0; i < 10; i++)
            tasks.Add(AcquireSameLockConcurrently(lockName, client, durability));

        await Task.WhenAll(tasks);
        
        Assert.Equal(10, total);
    }
    
    private async Task AcquireSameLockConcurrently(string lockName, KahunaClient client, LockDurability durability)
    {
        using CancellationTokenSource cts = new();
        
        cts.CancelAfter(TimeSpan.FromSeconds(10));
        
        await using KahunaLock kLock = await client.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(10), 
            wait: TimeSpan.FromSeconds(11),
            retry: TimeSpan.FromMilliseconds(500),
            durability: durability,
            cancellationToken: cts.Token
        );
        
        if (!kLock.IsAcquired)
            return;

        total++;
    }
    
    [Theory, CombinatorialData]
    public async Task TestValidateAcquireAndExtendLock(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability,
        [CombinatorialValues(true, false)] bool upgradeUrls
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType, upgradeUrls);
        
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await client.GetOrCreateLock(lockName, 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);
        
        (bool extended, long fencingToken) = await kLock.TryExtend(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.True(extended);
        Assert.Equal(kLock.FencingToken, fencingToken);

        KahunaLockInfo? lockInfo = await client.GetLockInfo(lockName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.Owner);
        HLCTimestamp expires = lockInfo.Expires;
        
        (extended, fencingToken) = await kLock.TryExtend(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.True(extended);
        Assert.Equal(kLock.FencingToken, fencingToken);
        
        lockInfo = await client.GetLockInfo(lockName, durability, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.Owner);
        Assert.True(lockInfo.Expires - expires > TimeSpan.Zero);
        
        //Assert.Equal(lockInfo.Expires > DateTime.UtcNow, true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestAcquireLockAndGetInfo(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType, 
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType, 
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability,
        [CombinatorialValues(true, false)] bool upgradeUrls
    ) 
    {
        KahunaClient client = GetClientByType(communicationType, clientType, upgradeUrls);
        
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await client.GetOrCreateLock(lockName, 10000, durability: durability, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(kLock.IsAcquired);
        Assert.Equal(0, kLock.FencingToken);
        
        (bool extended, long fencingToken) = await kLock.TryExtend(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.True(extended);
        Assert.Equal(kLock.FencingToken, fencingToken);

        KahunaLockInfo? lockInfo = await kLock.GetInfo(TestContext.Current.CancellationToken);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.Owner);
        HLCTimestamp expires = lockInfo.Expires;
        
        (extended, fencingToken) = await kLock.TryExtend(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.True(extended);
        Assert.Equal(kLock.FencingToken, fencingToken);
        
        lockInfo = await kLock.GetInfo(TestContext.Current.CancellationToken);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.Owner);
        Assert.True(lockInfo.Expires - expires > TimeSpan.Zero);
        
        //Assert.Equal(lockInfo.Expires > DateTime.UtcNow, true);
    }
    
    private KahunaClient GetClientByType(KahunaCommunicationType communicationType, KahunaClientType clientType, bool _)
    {
        return clientType switch
        {
            KahunaClientType.SingleEndpoint => new(url, null, GetCommunicationByType(communicationType),null),
            KahunaClientType.PoolOfEndpoints => new(urls, null, GetCommunicationByType(communicationType)),
            _ => throw new ArgumentOutOfRangeException(nameof(clientType), clientType, null)
        };
    }

    private static IKahunaCommunication GetCommunicationByType(KahunaCommunicationType communicationType)
    {
        return communicationType switch
        {
            KahunaCommunicationType.Grpc => new GrpcCommunication(null, null),
            KahunaCommunicationType.Rest => new RestCommunication(null),
            _ => throw new ArgumentOutOfRangeException(nameof(communicationType), communicationType, null)
        };
    }
}
