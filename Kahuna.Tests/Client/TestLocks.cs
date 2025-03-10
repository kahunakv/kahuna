
using Kahuna.Client;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Tests.Client;

public class TestLocks
{
    private readonly KahunaClient locks = new("https://localhost:8082", null);
    
    private int total;

    private static string GetRandomLockName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory]
    [InlineData(LockConsistency.Linearizable)]
    [InlineData(LockConsistency.Ephemeral)]
    public async Task TestValidateAcquireSingleLock(LockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await locks.GetOrCreateLock(lockName, 1000, consistency: consistency);

        Assert.True(kLock.IsAcquired);
    }
    
    [Theory]
    [InlineData(LockConsistency.Linearizable)]
    [InlineData(LockConsistency.Ephemeral)]
    public async Task TestLockAcquisitionAndExpiresWithMilliseconds(LockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        // Attempt to acquire the same lock twice:
        KahunaLock kLock1 = await locks.GetOrCreateLock(lockName, 10000, consistency: consistency);
        KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, 10000, consistency: consistency);

        Assert.True(kLock1.IsAcquired);
        Assert.False(kLock2.IsAcquired);

        // Dispose the first lock so that the lock becomes available:
        await kLock1.DisposeAsync();
        await kLock2.DisposeAsync();

        // Now try acquiring the lock again:
        await using KahunaLock kLock3 = await locks.GetOrCreateLock(lockName, 1000, consistency: consistency);
        Assert.True(kLock3.IsAcquired);
    }
    
    [Theory]
    [InlineData(LockConsistency.Linearizable)]
    [InlineData(LockConsistency.Ephemeral)]
    public async Task TestValidateAcquireLockExpiresWithTimeSpan(LockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock.IsAcquired);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock2.IsAcquired);
    }
    
    [Theory]
    [InlineData(LockConsistency.Linearizable)]
    [InlineData(LockConsistency.Ephemeral)]
    public async Task TestValidateAcquireLockExpires4(LockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await locks.GetOrCreateLock(
            lockName, 
            expiryTime: 1000, 
            waitTime: 1000, 
            retryTime: 500
        );
        
        Assert.True(kLock.IsAcquired);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock2.IsAcquired);
    }
    
    [Theory]
    [InlineData(LockConsistency.Linearizable)]
    [InlineData(LockConsistency.Ephemeral)]
    public async Task TestValidateAcquireLockExpiresRace(LockConsistency consistency)
    {
        List<Task> tasks = new(50);

        for (int i = 0; i < 50; i++)
            tasks.Add(AcquireLockConcurrently(consistency));

        await Task.WhenAll(tasks);
    }
    
    private async Task AcquireLockConcurrently(LockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await locks.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(5), 
            wait: TimeSpan.FromSeconds(5), 
            retry: TimeSpan.FromMilliseconds(500),
            consistency: consistency
        );
        
        Assert.True(kLock.IsAcquired);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(5), consistency: consistency);
        Assert.True(kLock2.IsAcquired);
    }
    
    [Theory]
    [InlineData(LockConsistency.Linearizable)]
    [InlineData(LockConsistency.Ephemeral)]
    public async Task TestValidateAcquireSameLockExpiresRace(LockConsistency consistency)
    {
        string lockName = GetRandomLockName();
        
        List<Task> tasks = new(10);

        for (int i = 0; i < 10; i++)
            tasks.Add(AcquireSameLockConcurrently(lockName, consistency));

        await Task.WhenAll(tasks);
        
        Assert.Equal(10, total);
    }
    
    private async Task AcquireSameLockConcurrently(string lockName, LockConsistency consistency)
    {
        await using KahunaLock kLock = await locks.GetOrCreateLock(
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
    
    [Theory]
    [InlineData(LockConsistency.Linearizable)]
    [InlineData(LockConsistency.Ephemeral)]
    public async Task TestValidateAcquireAndExtendLock(LockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await locks.GetOrCreateLock(lockName, 10000, consistency: consistency);

        Assert.True(kLock.IsAcquired);
        
        (bool extended, long fencingToken) = await kLock.TryExtend(TimeSpan.FromSeconds(10));
        Assert.True(extended);

        KahunaLockInfo? lockInfo = await locks.GetLockInfo(lockName, consistency);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.LockId);
        HLCTimestamp expires = lockInfo.Expires;
        
        await kLock.TryExtend(TimeSpan.FromSeconds(10));
        Assert.True(extended);
        
        lockInfo = await locks.GetLockInfo(lockName, consistency);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.LockId);
        Assert.True(lockInfo.Expires - expires > TimeSpan.Zero);
        
        //Assert.Equal(lockInfo.Expires > DateTime.UtcNow, true);
    }
}
