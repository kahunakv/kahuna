
using Kahuna.Client;

namespace Kahuna.Tests.Client;

public class TestLocks
{
    private readonly KahunaClient locks = new("http://localhost:2070", null);
    
    private int total;

    private static string GetRandomLockName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Fact]
    public async Task TestValidateAcquireLock()
    {
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await locks.GetOrCreateLock(lockName, 1000);

        Assert.True(kLock.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireLock2()
    {
        string lockName = GetRandomLockName();

        await using KahunaLock kLock1 = await locks.GetOrCreateLock(lockName, 10000);
        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, 10000);

        Assert.True(kLock1.IsAcquired);
        Assert.False(kLock2.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireLockExpires2()
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await locks.GetOrCreateLock(lockName, 1000);
        Assert.True(kLock.IsAcquired);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, 1000);
        Assert.True(kLock2.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireLockExpires3()
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1));
        Assert.True(kLock.IsAcquired);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1));
        Assert.True(kLock2.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireLockExpires4()
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

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1));
        Assert.True(kLock2.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireLockExpiresRace()
    {
        List<Task> tasks = new(50);

        for (int i = 0; i < 50; i++)
            tasks.Add(AcquireLockConcurrently());

        await Task.WhenAll(tasks);
    }
    
    private async Task AcquireLockConcurrently()
    {
        string lockName = GetRandomLockName();

        KahunaLock kLock = await locks.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(1), 
            wait: TimeSpan.FromSeconds(1), 
            retry: TimeSpan.FromMilliseconds(100)
        );
        
        Assert.True(kLock.IsAcquired);

        await kLock.DisposeAsync();

        await using KahunaLock kLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1));
        Assert.True(kLock2.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireSameLockExpiresRace()
    {
        List<Task> tasks = new(10);
        string lockName = GetRandomLockName();

        for (int i = 0; i < 10; i++)
            tasks.Add(AcquireSameLockConcurrently(lockName));

        await Task.WhenAll(tasks);
        
        Assert.Equal(10, total);
    }
    
    private async Task AcquireSameLockConcurrently(string lockName)
    {
        await using KahunaLock kLock = await locks.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(10), 
            wait: TimeSpan.FromSeconds(11),
            retry: TimeSpan.FromMilliseconds(100)
        );
        
        if (!kLock.IsAcquired)
            return;

        total++;
    }
    
    [Fact]
    public async Task TestValidateAcquireAndExtendLock()
    {
        string lockName = GetRandomLockName();

        await using KahunaLock kLock = await locks.GetOrCreateLock(lockName, 1000);

        Assert.True(kLock.IsAcquired);
        
        await kLock.TryExtend(TimeSpan.FromSeconds(1));

        var lockInfo = await locks.GetLockInfo(lockName);
    }
}
