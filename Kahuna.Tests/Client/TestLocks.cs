
using Kahuna.Client;
using Kommander.Time;

namespace Kahuna.Tests.Client;

public class TestLocks
{
    private readonly KahunaClient locks = new("http://localhost:8081", null);
    
    private int total;

    private static string GetRandomLockName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory]
    [InlineData(KahunaLockConsistency.Consistent)]
    [InlineData(KahunaLockConsistency.Ephemeral)]
    public async Task TestValidateAcquireSingleLock(KahunaLockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await locks.GetOrCreateLock(lockName, 1000, consistency: consistency);

        Assert.True(kLock.IsAcquired);
    }
    
    [Theory]
    [InlineData(KahunaLockConsistency.Consistent)]
    [InlineData(KahunaLockConsistency.Ephemeral)]
    public async Task TestLockAcquisitionAndExpiresWithMilliseconds(KahunaLockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        // Attempt to acquire the same lock twice:
        KahunaLock kLock1 = await locks.GetOrCreateLock(lockName, 10000, consistency: consistency);
        KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, 10000, consistency: consistency);

        Assert.True(kLock1.IsAcquired);
        Assert.False(kLock2.IsAcquired);

        // Dispose the first lock so that the lock becomes available:
        await kLock1.DisposeAsync();

        // Now try acquiring the lock again:
        await using KahunaLock kLock3 = await locks.GetOrCreateLock(lockName, 1000, consistency: consistency);
        Assert.True(kLock3.IsAcquired);
        
        await kLock2.DisposeAsync();
    }
    
    [Theory]
    [InlineData(KahunaLockConsistency.Consistent)]
    [InlineData(KahunaLockConsistency.Ephemeral)]
    public async Task TestValidateAcquireLockExpiresWithTimeSpan(KahunaLockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock.IsAcquired);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock2.IsAcquired);
    }
    
    [Theory]
    [InlineData(KahunaLockConsistency.Consistent)]
    [InlineData(KahunaLockConsistency.Ephemeral)]
    public async Task TestValidateAcquireLockExpires4(KahunaLockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await locks.GetOrCreateLock(
            lockName, 
            expiryTime: 1000, 
            waitTime: 1000, 
            retryTime: 100
        );
        
        Assert.True(kLock.IsAcquired);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock2.IsAcquired);
    }
    
    [Theory]
    //[InlineData(KahunaLockConsistency.Consistent)]
    [InlineData(KahunaLockConsistency.Ephemeral)]
    public async Task TestValidateAcquireLockExpiresRace(KahunaLockConsistency consistency)
    {
        List<Task> tasks = new(50);

        for (int i = 0; i < 50; i++)
            tasks.Add(AcquireLockConcurrently(consistency));

        await Task.WhenAll(tasks);
    }
    
    private async Task AcquireLockConcurrently(KahunaLockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await locks.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(1), 
            wait: TimeSpan.FromSeconds(1), 
            retry: TimeSpan.FromMilliseconds(100),
            consistency: consistency
        );
        
        Assert.True(kLock.IsAcquired);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1), consistency: consistency);
        Assert.True(kLock2.IsAcquired);
    }
    
    [Theory]
    //[InlineData(KahunaLockConsistency.Consistent)]
    [InlineData(KahunaLockConsistency.Ephemeral)]
    public async Task TestValidateAcquireSameLockExpiresRace(KahunaLockConsistency consistency)
    {
        List<Task> tasks = new(10);
        string lockName = GetRandomLockName();

        for (int i = 0; i < 10; i++)
            tasks.Add(AcquireSameLockConcurrently(lockName, consistency));

        await Task.WhenAll(tasks);
        
        Assert.Equal(10, total);
    }
    
    private async Task AcquireSameLockConcurrently(string lockName, KahunaLockConsistency consistency)
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
    [InlineData(KahunaLockConsistency.Consistent)]
    [InlineData(KahunaLockConsistency.Ephemeral)]
    public async Task TestValidateAcquireAndExtendLock(KahunaLockConsistency consistency)
    {
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await locks.GetOrCreateLock(lockName, 5000, consistency: consistency);

        Assert.True(kLock.IsAcquired);
        
        bool extended = await kLock.TryExtend(TimeSpan.FromSeconds(10));
        Assert.True(extended);

        KahunaLockInfo? lockInfo = await locks.GetLockInfo(lockName);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.LockId);
        HLCTimestamp expires = lockInfo.Expires;
        
        await kLock.TryExtend(TimeSpan.FromSeconds(10));
        Assert.True(extended);
        
        lockInfo = await locks.GetLockInfo(lockName);
        Assert.NotNull(lockInfo);
        
        Assert.Equal(lockInfo.Owner, kLock.LockId);
        Assert.True(lockInfo.Expires - expires > TimeSpan.Zero);
        
        //Assert.Equal(lockInfo.Expires > DateTime.UtcNow, true);
    }
}
