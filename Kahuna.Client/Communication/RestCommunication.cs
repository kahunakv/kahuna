
using System.Net;
using System.Text.Json;
using Flurl.Http;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;

namespace Kahuna.Client.Communication;

public class RestCommunication : IKahunaCommunication
{
    private readonly ILogger? logger;
    
    public RestCommunication(ILogger? logger)
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
    
    public async Task<(KahunaLockAcquireResult, long)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability)
    {
        KahunaLockRequest request = new()
        {
            Resource = resource, 
            Owner = owner, 
            ExpiresMs = expiryTime, 
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
        
        KahunaLockResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/locks/try-lock")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(5)
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload)
                .ReceiveJson<KahunaLockResponse>()).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == LockResponseType.Locked)
                return (KahunaLockAcquireResult.Success, response.FencingToken);
            
            if (response.Type == LockResponseType.Busy)
                return (KahunaLockAcquireResult.Conflicted, response.FencingToken);

        } while (response.Type == LockResponseType.MustRetry);
            
        throw new KahunaException("Failed to lock", response.Type);
    }
    
    public async Task<bool> TryUnlock(string url, string resource, byte[] owner, LockDurability durability)
    {
        KahunaLockRequest request = new()
        {
            Resource = resource, 
            Owner = owner, 
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
        
        KahunaLockResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() => 
                url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/locks/try-unlock")
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
    
    public async Task<(bool, long)> TryExtend(string url, string resource, byte[] owner, int expiryTime, LockDurability durability)
    {
        KahunaLockRequest request = new()
        {
            Resource = resource, 
            Owner = owner, 
            ExpiresMs = expiryTime, 
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);

        KahunaLockResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
            
            response = await retryPolicy.ExecuteAsync(() => 
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/locks/try-extend")
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
                return (true, response.FencingToken);

        } while (response.Type == LockResponseType.MustRetry);
        
        throw new KahunaException("Failed to extend lock", response.Type);
    }
    
    public async Task<KahunaLockInfo?> Get(string url, string resource, LockDurability durability)
    {
        KahunaGetLockRequest request = new()
        {
            LockName = resource, 
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaGetLockRequest);

        KahunaGetLockResponse? response;

        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);

            response = await retryPolicy.ExecuteAsync(() =>
                    url
                        .WithOAuthBearerToken("xxx")
                        .AppendPathSegments("v1/locks/get-info")
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
                return new(response.Owner, response.Expires, response.FencingToken);
            
        } while (response.Type == LockResponseType.MustRetry);
        
        throw new KahunaException("Failed to get lock information", response.Type);
    }

    public async Task<(bool, long)> TrySetKeyValue(string url, string key, byte[]? value, int expiryTime, KeyValueFlags flags, KeyValueConsistency consistency)
    {
        KahunaSetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value, 
            ExpiresMs = expiryTime,
            Flags = flags,
            Consistency = consistency
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);
        
        KahunaSetKeyValueResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaSetKeyValueResponse>()).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == KeyValueResponseType.Set)
                return (true, response.Revision);
            
            if (response.Type == KeyValueResponseType.NotSet)
                return (false, response.Revision);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to set key/value: " + response.Type, response.Type);
    }

    public async Task<(bool, long)> TryCompareValueAndSetKeyValue(string url, string key, byte[]? value, byte[]? compareValue, int expiryTime, KeyValueConsistency consistency)
    {
        KahunaSetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value, 
            CompareValue = compareValue,
            ExpiresMs = expiryTime,
            Flags = KeyValueFlags.SetIfEqualToValue,
            Consistency = consistency
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);
        
        KahunaSetKeyValueResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaSetKeyValueResponse>()).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == KeyValueResponseType.Set)
                return (true, response.Revision);
            
            if (response.Type == KeyValueResponseType.NotSet)
                return (false, response.Revision);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to set key/value: " + response.Type, response.Type);
    }

    public async Task<(bool, long)> TryCompareRevisionAndSetKeyValue(string url, string key, byte[]? value, long compareRevision, int expiryTime, KeyValueConsistency consistency)
    {
        KahunaSetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value, 
            CompareRevision = compareRevision,
            ExpiresMs = expiryTime,
            Flags = KeyValueFlags.SetIfEqualToRevision,
            Consistency = consistency
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);
        
        KahunaSetKeyValueResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaSetKeyValueResponse>()).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == KeyValueResponseType.Set)
                return (true, response.Revision);
            
            if (response.Type == KeyValueResponseType.NotSet)
                return (false, response.Revision);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to set key/value: " + response.Type, response.Type);
    }

    public async Task<(byte[]?, long)> TryGetKeyValue(string url, string key, KeyValueConsistency consistency)
    {
        KahunaGetKeyValueRequest request = new()
        {
            Key = key, 
            Consistency = consistency
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaGetKeyValueRequest);
        
        KahunaGetKeyValueResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-get")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaGetKeyValueResponse>()).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == KeyValueResponseType.Get)
                return (response.Value, response.Revision);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (null, response.Revision);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to get key/value: " + response.Type, response.Type);
    }

    public async Task<(bool, long)> TryDeleteKeyValue(string url, string key, KeyValueConsistency consistency)
    {
        KahunaDeleteKeyValueRequest request = new()
        {
            Key = key, 
            Consistency = consistency
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaDeleteKeyValueRequest);
        
        KahunaDeleteKeyValueResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-delete")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaDeleteKeyValueResponse>()).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == KeyValueResponseType.Deleted)
                return (true, response.Revision);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, response.Revision);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to delete key/value: " + response.Type, response.Type);
    }

    public async Task<(bool, long)> TryExtendKeyValue(string url, string key, int expiresMs, KeyValueConsistency consistency)
    {
        KahunaExtendKeyValueRequest request = new()
        {
            Key = key,
            ExpiresMs = expiresMs,
            Consistency = consistency
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaExtendKeyValueRequest);
        
        KahunaDeleteKeyValueResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-extend")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaDeleteKeyValueResponse>()).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == KeyValueResponseType.Extended)
                return (true, response.Revision);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, response.Revision);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to extend key/value: " + response.Type, response.Type);
    }

    public Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransaction(string url, byte[] script, string? hash)
    {
        throw new NotImplementedException();
    }
}