
using System.Net;
using System.Text.Json;
using Flurl.Http;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;

namespace Kahuna.Client.Communication;

internal sealed class HttpCommunication
{
    private readonly ILogger? logger;
    
    public HttpCommunication(ILogger? logger)
    {
        this.logger = logger;
    }
    
    private static AsyncRetryPolicy BuildRetryPolicy(ILogger? logger, int medianFirstRetryDelay = 1)
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

        if (!exception.StatusCode.HasValue && 
            !string.IsNullOrEmpty(exception.Message) && 
            (exception.Message.Contains("An error occurred while sending the request") || exception.Message.Contains("Call timed out")))
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
    
    internal async Task<KahunaLockAcquireResult> TryAcquireLock(string url, string key, string lockId, int expiryTime, LockConsistency consistency)
    {
        KahunaLockRequest request = new() { LockName = key, LockId = lockId, ExpiresMs = expiryTime, Consistency = consistency };
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
        
        KahunaLockResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
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
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == LockResponseType.Locked)
                return KahunaLockAcquireResult.Success;
            
            if (response.Type == LockResponseType.Busy)
                return KahunaLockAcquireResult.Conflicted;

        } while (response.Type == LockResponseType.MustRetry);
            
        throw new KahunaException("Failed to lock", response.Type);
    }
    
    internal async Task<bool> TryUnlock(string url, string resource, string lockId, LockConsistency consistency)
    {
        KahunaLockRequest request = new() { LockName = resource, LockId = lockId, Consistency = consistency };
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
        
        KahunaLockResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() => 
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
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == LockResponseType.Unlocked)
                return true;

        } while (response.Type == LockResponseType.MustRetry);
        
        Console.WriteLine(response.Type);
        
        throw new KahunaException("Failed to unlock", response.Type);
    }
    
    internal async Task<bool> TryExtend(string url, string resource, string lockId, int expiryTime, LockConsistency consistency)
    {
        KahunaLockRequest request = new() { LockName = resource, LockId = lockId, ExpiresMs = expiryTime, Consistency = consistency };
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);

        KahunaLockResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
            
            response = await retryPolicy.ExecuteAsync(() => 
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
            
            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
            
            if (response.Type == LockResponseType.Extended)
                return true;

        } while (response.Type == LockResponseType.MustRetry);
        
        throw new KahunaException("Failed to extend lock", response.Type);
    }
    
    internal async Task<KahunaLockInfo?> Get(string url, string resource, LockConsistency consistency)
    {
        KahunaGetLockRequest request = new() { LockName = resource, Consistency = consistency };
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaGetLockRequest);

        KahunaGetLockResponse? response;

        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);

            response = await retryPolicy.ExecuteAsync(() =>
                    url
                        .WithOAuthBearerToken("xxx")
                        .AppendPathSegments("v1/kahuna/get-lock")
                        .WithHeader("Accept", "application/json")
                        .WithHeader("Content-Type", "application/json")
                        .WithTimeout(5)
                        .WithSettings(o => o.HttpVersion = "2.0")
                        .PostStringAsync(payload)
                        .ReceiveJson<KahunaGetLockResponse>())
                .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == LockResponseType.Got)
                return new(response.Owner ?? "", response.Expires, response.FencingToken);
            
        } while (response.Type == LockResponseType.MustRetry);
        
        throw new KahunaException("Failed to get lock information", response.Type);
    }
}