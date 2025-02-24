
using System.Net;
using System.Text.Json;
using Flurl.Http;
using Kahuna.Client.Communication.Data;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;

namespace Kahuna.Client.Communication;

internal sealed class HttpCommunication
{
    private AsyncRetryPolicy BuildRetryPolicy(ILogger? logger, int medianFirstRetryDelay = 1)
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
    
    private static bool IsTransientError(FlurlHttpException exception)
    {
        if (exception is FlurlHttpTimeoutException)
            return true;

        bool canBeRetried = exception.StatusCode.HasValue && CanHttpCodeBeRetried(exception.StatusCode.Value);
        if (canBeRetried)
            return canBeRetried;

        if (!exception.StatusCode.HasValue && !string.IsNullOrEmpty(exception.Message) && exception.Message.Contains("An error occurred while sending the request"))
            return true;

        return false;
    }

    private static bool CanHttpCodeBeRetried(int httpStatus)
    {
        return httpStatus switch
        {
            (int)HttpStatusCode.RequestTimeout or // 408
            (int)HttpStatusCode.BadGateway or // 502
            (int)HttpStatusCode.ServiceUnavailable or // 503
            (int)HttpStatusCode.GatewayTimeout or // 504
            (int)HttpStatusCode.TooManyRequests => // 429
              true,
            _ => false
        };
    }
    
    internal async Task<KahunaLockAcquireResult> TryAcquireLock(string url, string key, string lockId, int expiryTime)
    {
        KahunaLockRequest request = new() { LockName = key, LockId = lockId, ExpiresMs = expiryTime };
        string payload = JsonSerializer.Serialize(request);

        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
        KahunaLockResponse? response = await retryPolicy.ExecuteAsync(() =>
            url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/kahuna/lock")
            .WithHeader("Accept", "application/json")
            .WithHeader("Content-Type", "application/json")
            .WithTimeout(5)
            .WithSettings(o => o.HttpVersion = "2.0")
            .PostStringAsync(payload)
            .ReceiveJson<KahunaLockResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null");    
        
        return response.Type == LockResponseType.Locked ? KahunaLockAcquireResult.Success : KahunaLockAcquireResult.Conflicted;
    }
    
    internal async Task<bool> TryUnlock(string url, string key, string lockId)
    {
        KahunaLockRequest request = new() { LockName = key, LockId = lockId };
        string payload = JsonSerializer.Serialize(request);
        
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
        KahunaLockResponse? response = await retryPolicy.ExecuteAsync(() => 
            url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/kahuna/unlock")
            .WithHeader("Accept", "application/json")
            .WithHeader("Content-Type", "application/json")
            .WithTimeout(5)
            .WithSettings(o => o.HttpVersion = "2.0")
            .PostStringAsync(payload)
            .ReceiveJson<KahunaLockResponse>())
            .ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null");
        
        return response.Type == LockResponseType.Unlocked;
    }
    
    internal async Task<bool> TryExtend(string url, string key, string lockId, double expiryTime)
    {
        KahunaLockRequest request = new() { LockName = key, LockId = lockId, ExpiresMs = expiryTime };
        string payload = JsonSerializer.Serialize(request);
        
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
        KahunaLockResponse? response = await retryPolicy.ExecuteAsync(() => 
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kahuna/extend-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaLockResponse>())
            .ConfigureAwait(false);
        
        return response.Type == LockResponseType.Extended;
    }
    
    internal async Task<KahunaLockInfo?> Get(string url, string key)
    {
        KahunaGetRequest request = new() { LockName = key };
        string payload = JsonSerializer.Serialize(request);
        
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
        KahunaGetResponse? response = await retryPolicy.ExecuteAsync(() => 
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kahuna/get-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaGetResponse>())
            .ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null");
        
        //Console.WriteLine("{0}", response.Type);

        if (response.Type != LockResponseType.Got)
            return null;
        
        return new(response.Owner ?? "", response.Expires, response.FencingToken);
    }
}