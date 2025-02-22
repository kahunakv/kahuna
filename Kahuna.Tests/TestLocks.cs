
using Kahuna.Client;

namespace Kahuna.Tests;

public class TestLocks
{
    private readonly KahunaClient locks = new("http://localhost:2070");
    
    private int total;

    private static string GetRandomLockName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Fact]
    public async Task TestValidateAcquireLock()
    {
        string lockName = GetRandomLockName();

        await using KahunaLock redLock = await locks.GetOrCreateLock(lockName, 1000);

        Assert.True(redLock.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireLock2()
    {
        string lockName = GetRandomLockName();

        await using KahunaLock redLock1 = await locks.GetOrCreateLock(lockName, 10000);
        await using KahunaLock redLock2 = await locks.GetOrCreateLock(lockName, 10000);

        Assert.True(redLock1.IsAcquired);
        Assert.False(redLock2.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireLockExpires2()
    {
        string lockName = GetRandomLockName();

        KahunaLock redLock = await locks.GetOrCreateLock(lockName, 1000);
        Assert.True(redLock.IsAcquired);

        await redLock.DisposeAsync();

        await using KahunaLock redLock2 = await locks.GetOrCreateLock(lockName, 1000);
        Assert.True(redLock2.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireLockExpires3()
    {
        string lockName = GetRandomLockName();

        KahunaLock redLock = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1));
        Assert.True(redLock.IsAcquired);

        await redLock.DisposeAsync();

        await using KahunaLock redLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1));
        Assert.True(redLock2.IsAcquired);
    }
    
    [Fact]
    public async Task TestValidateAcquireLockExpires4()
    {
        string lockName = GetRandomLockName();

        KahunaLock redLock = await locks.GetOrCreateLock(
            lockName, 
            expiryTime: 1000, 
            waitTime: 1000, 
            retryTime: 100
        );
        
        Assert.True(redLock.IsAcquired);

        await redLock.DisposeAsync();

        await using KahunaLock redLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1));
        Assert.True(redLock2.IsAcquired);
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

        KahunaLock redLock = await locks.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(1), 
            wait: TimeSpan.FromSeconds(1), 
            retry: TimeSpan.FromMilliseconds(100)
        );
        
        Assert.True(redLock.IsAcquired);

        await redLock.DisposeAsync();

        await using KahunaLock redLock2 = await locks.GetOrCreateLock(lockName, TimeSpan.FromSeconds(1));
        Assert.True(redLock2.IsAcquired);
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
        await using KahunaLock redLock = await locks.GetOrCreateLock(
            lockName, 
            expiry: TimeSpan.FromSeconds(10), 
            wait: TimeSpan.FromSeconds(11),
            retry: TimeSpan.FromMilliseconds(100)
        );
        
        if (!redLock.IsAcquired)
            return;

        total++;
    }
}
