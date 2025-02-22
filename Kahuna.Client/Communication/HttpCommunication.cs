
using System.Text.Json;
using Flurl.Http;
using Kahuna.Client.Communication.Data;
using Microsoft.Extensions.Logging;

namespace Kahuna.Client.Communication;

internal sealed class HttpCommunication
{
    protected AsyncRetryPolicy BuildRetryPolicy(ILogger? logger, int medianFirstRetryDelay = 1)
    {
        IEnumerable<TimeSpan> delay = Backoff.DecorrelatedJitterBackoffV2(
            medianFirstRetryDelay: TimeSpan.FromSeconds(medianFirstRetryDelay),
            retryCount: 5
        );

        AsyncRetryPolicy retryPolicy = Policy.Handle<FlurlHttpException>(IsTransientError)
            .WaitAndRetryAsync(delay, (ex, timeSpan) => OnRetry(ex, timeSpan, logger));

        return retryPolicy;
    }
    
    private static void OnRetry(Exception ex, TimeSpan timeSpan, ILogger? logger)
    {
        if (logger is not null)
            logger.LogWarning("Retry: {Exception} {Time}", ex.Message, timeSpan);
        else
            Console.WriteLine("Retry: {0} {1}", ex.Message, timeSpan);
    }
    
    internal async Task<KahunaLockAcquireResult> TryAcquireLock(string url, string key, string lockId, int expiryTime)
    {
        KahunaLockRequest request = new() { LockName = key, LockId = lockId, ExpiresMs = expiryTime };
        string payload = JsonSerializer.Serialize(request);
        
        KahunaLockResponse response = await url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/kahuna/lock")
            .WithHeader("Accept", "application/json")
            .WithHeader("Content-Type", "application/json")
            .WithTimeout(5)
            .WithSettings(o => o.HttpVersion = "2.0")
            .PostStringAsync(payload)
            .ReceiveJson<KahunaLockResponse>()
            .ConfigureAwait(false);
        
        return response.Type == 0 ? KahunaLockAcquireResult.Success : KahunaLockAcquireResult.Conflicted;
    }
    
    internal async Task<bool> TryUnlock(string url, string key, string lockId)
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
            .ReceiveJson<KahunaLockResponse>()
            .ConfigureAwait(false);
        
        return response.Type == 2;
    }
}