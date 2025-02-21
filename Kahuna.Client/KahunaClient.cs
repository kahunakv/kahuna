
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Flurl.Http;

namespace Kahuna.Client;

public class KahunaClient
{
    private readonly string url;
    
    public KahunaClient(string url)
    {
        this.url = url;
    }
    
    private async Task<KahunaLockAdquireResult> TryAdquireLock(string key, string lockId, TimeSpan expiryTime)
    {
        KahunaLockRequest request = new() { LockName = key, LockId = lockId, ExpiresMs = expiryTime.TotalMilliseconds };
        string payload = JsonSerializer.Serialize(request);
        
        KahunaLockResponse response = await url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/kahuna/lock")
            .WithHeader("Accept", "application/json")
            .WithHeader("Content-Type", "application/json")
            .WithTimeout(5)
            .WithSettings(o => o.HttpVersion = "2.0")
            .PostStringAsync(payload)
            .ReceiveJson<KahunaLockResponse>();
        
        return response.Type == 0 ? KahunaLockAdquireResult.Success : KahunaLockAdquireResult.Conflicted;
    }
    
    private async Task<(KahunaLockAdquireResult, string?)> Lock(string key, TimeSpan expiryTime, TimeSpan wait, TimeSpan retry)
    {
        try
        {
            string lockId = Guid.NewGuid().ToString("N");
            
            Stopwatch stopWatch = Stopwatch.StartNew();
            KahunaLockAdquireResult result = KahunaLockAdquireResult.Error;

            while (stopWatch.Elapsed < wait)
            {
                result = await TryAdquireLock(key, lockId, expiryTime).ConfigureAwait(false);

                if (result != KahunaLockAdquireResult.Success)
                {
                    await Task.Delay(retry).ConfigureAwait(false);
                    continue;
                }

                return (result, lockId);
            }

            return (result, null);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error locking lock instance: {0}", ex.Message);

            return (KahunaLockAdquireResult.Error, null);
        }
    }
    
    private async Task<(KahunaLockAdquireResult, string?)> Lock(string key, TimeSpan expiryTime)
    {
        try
        {
            string lockId = Guid.NewGuid().ToString();

            KahunaLockAdquireResult result = await TryAdquireLock(key, lockId, expiryTime);

            return (result, lockId);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error locking lock instance: {0}", ex.Message);

            return (KahunaLockAdquireResult.Error, null);
        }
    }
    
    public async Task<KahunaLock> GetOrCreateLock(string resource, int expiryTime = 30000, int waitTime = 10000, int retryTime = 100)
    {
        TimeSpan expiry = TimeSpan.FromMilliseconds(expiryTime);
        TimeSpan wait = TimeSpan.FromMilliseconds(waitTime);
        TimeSpan retry = TimeSpan.FromMilliseconds(retryTime);

        return await GetOrCreateLock(resource, expiry, wait, retry);
    }

    public async Task<KahunaLock> GetOrCreateLock(string resource, int expiryTime = 30000)
    {
        TimeSpan expiry = TimeSpan.FromMilliseconds(expiryTime);
        
        return await GetOrCreateLock(resource, expiry);
    }

    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, TimeSpan wait, TimeSpan retry)
    {
        if (retry == TimeSpan.Zero)
            throw new Exception("Retry cannot be zero");
        
        if (wait == TimeSpan.Zero)
            return new(this, resource, await Lock(resource, expiry));
        
        return new(this, resource, await Lock(resource, expiry, wait, retry));
    }

    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry)
    {
        return new(this, resource, await Lock(resource, expiry));
    }
    
    public async Task<bool> Unlock(string key, string lockId)
    {
        try
        {
            KahunaLockRequest request = new() { LockName = key, LockId = lockId };
            string payload = JsonSerializer.Serialize(request);
        
            KahunaLockResponse response = await url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/kahuna/unlock")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(5)
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload)
                .ReceiveJson<KahunaLockResponse>();
        
            return response.Type == 2;
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error locking lock instance: {0}", ex.Message);
            return false;
        }
    }
}
