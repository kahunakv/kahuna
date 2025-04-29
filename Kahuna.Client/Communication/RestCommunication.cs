
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using System.Text.Json;
using Flurl.Http;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;

namespace Kahuna.Client.Communication;

/// <summary>
/// Represents a communication mechanism using REST protocol for interacting
/// with Kahuna's functionalities such as locks and key-value operations.
/// Implements the IKahunaCommunication interface to provide methods for
/// interacting with Kahuna's lock acquisition, extension, deletion,
/// and key-value transactions.
/// This class provides a set of asynchronous methods to facilitate communication
/// with a REST-based backend for lock management and key-value store operations.
/// </summary>
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

    /// <summary>
    /// Attempts to acquire a lock for a specified resource with the given configuration.
    /// </summary>
    /// <param name="url">The endpoint URL for the lock request.</param>
    /// <param name="resource">The resource name for which the lock is requested.</param>
    /// <param name="owner">The identifier of the lock owner.</param>
    /// <param name="expiryTime">The duration of the lock in milliseconds.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>
    /// A tuple containing the lock acquisition result as <see cref="KahunaLockAcquireResult"/>,
    /// the expiration time of the lock in milliseconds, and an optional error message if any.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the lock acquisition fails for any reason that cannot be retried.
    /// </exception>
    public async Task<(KahunaLockAcquireResult, long, string?)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken)
    {
        KahunaLockRequest request = new()
        {
            Resource = resource,
            Owner = owner,
            ExpiresMs = expiryTime,
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);

        int retries = 0;
        KahunaLockResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/locks/try-lock")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload, cancellationToken: cancellationToken)
                .ReceiveJson<KahunaLockResponse>()).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == LockResponseType.Locked)
                return (KahunaLockAcquireResult.Success, response.FencingToken, response.ServedFrom);
            
            if (response.Type == LockResponseType.Busy)
                return (KahunaLockAcquireResult.Conflicted, response.FencingToken, response.ServedFrom);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Errored);

        } while (response.Type == LockResponseType.MustRetry);
            
        throw new KahunaException("Failed to lock", response.Type);
    }

    /// <summary>
    /// Attempts to release a lock for the specified resource with the provided configuration.
    /// </summary>
    /// <param name="url">The endpoint URL for the unlock request.</param>
    /// <param name="resource">The resource name for which the unlock is requested.</param>
    /// <param name="owner">The identifier of the lock owner.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>
    /// A boolean indicating whether the lock was successfully released.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the unlock operation fails for any reason that cannot be retried.
    /// </exception>
    public async Task<bool> TryUnlock(string url, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken)
    {
        KahunaLockRequest request = new()
        {
            Resource = resource,
            Owner = owner,
            Durability = durability
        };

        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);
        
        int retries = 0;
        KahunaLockResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() => 
                url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/locks/try-unlock")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload, cancellationToken: cancellationToken)
                .ReceiveJson<KahunaLockResponse>())
                .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == LockResponseType.Unlocked)
                return true;

            if (response.Type == LockResponseType.LockDoesNotExist)
                return false;
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Errored);

        } while (response.Type == LockResponseType.MustRetry);
        
        throw new KahunaException("Failed to unlock: " + response.Type, response.Type);
    }

    /// <summary>
    /// Attempts to extend the lock for a specified resource with the given configuration.
    /// </summary>
    /// <param name="url">The endpoint URL for the lock extension request.</param>
    /// <param name="resource">The resource name for which the lock extension is requested.</param>
    /// <param name="owner">The identifier of the lock owner.</param>
    /// <param name="expiryTime">The new duration of the lock in milliseconds.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>
    /// A tuple where the first element is a boolean indicating whether the lock extension was successful,
    /// and the second element is the updated expiration time of the lock in milliseconds, if successful.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the lock extension fails due to an unrecoverable error.
    /// </exception>
    public async Task<(bool, long)> TryExtendLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken)
    {
        KahunaLockRequest request = new()
        {
            Resource = resource,
            Owner = owner,
            ExpiresMs = expiryTime,
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);

        int retries = 0;
        KahunaLockResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
            
            response = await retryPolicy.ExecuteAsync(() => 
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/locks/try-extend")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaLockResponse>())
                    .ConfigureAwait(false);
            
            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
            
            if (response.Type == LockResponseType.Extended)
                return (true, response.FencingToken);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Errored);

        } while (response.Type == LockResponseType.MustRetry);
        
        throw new KahunaException("Failed to extend lock", response.Type);
    }

    /// <summary>
    /// Retrieves lock information for the specified resource using the given configuration.
    /// </summary>
    /// <param name="url">The endpoint URL to fetch the lock information from.</param>
    /// <param name="resource">The resource name associated with the lock request.</param>
    /// <param name="durability">The durability level of the lock, either ephemeral or persistent.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation if needed.</param>
    /// <returns>
    /// An instance of <see cref="KahunaLockInfo"/> if the lock information is successfully retrieved;
    /// otherwise, null if no lock information could be found.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when an irrecoverable error occurs while attempting to retrieve the lock information.
    /// </exception>
    public async Task<KahunaLockInfo?> GetLock(string url, string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        KahunaGetLockRequest request = new()
        {
            Resource = resource,
            Durability = durability
        };

        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaGetLockRequest);

        int retries = 0;
        KahunaGetLockResponse? response;

        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);

            response = await retryPolicy.ExecuteAsync(() =>
                    url
                        .WithOAuthBearerToken("xxx")
                        .AppendPathSegments("v1/locks/get-info")
                        .WithHeader("Accept", "application/json")
                        .WithHeader("Content-Type", "application/json")
                        .WithSettings(o => o.HttpVersion = "2.0")
                        .PostStringAsync(payload, cancellationToken: cancellationToken)
                        .ReceiveJson<KahunaGetLockResponse>())
                        .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == LockResponseType.Got)
                return new(response.Owner, response.Expires, response.FencingToken);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Errored);
            
        } while (response.Type == LockResponseType.MustRetry);
        
        throw new KahunaException("Failed to get lock information", response.Type);
    }

    /// <summary>
    /// Attempts to set a key-value pair in the specified data store with the given configuration.
    /// </summary>
    /// <param name="url">The endpoint URL for the key-value storage service.</param>
    /// <param name="transactionId">The unique transaction identifier for this operation.</param>
    /// <param name="key">The key to be set in the storage system.</param>
    /// <param name="value">The value associated with the specified key. Can be null to delete the key.</param>
    /// <param name="expiryTime">The expiration time for the key-value pair in milliseconds.</param>
    /// <param name="flags">The flags that specify conditions for the set operation (e.g., overwrite conditions).</param>
    /// <param name="durability">The durability level for the operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>
    /// A tuple containing three elements: a boolean indicating success, the expiration time of the key-value pair in milliseconds,
    /// and an integer representing the time taken for the operation.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the key-value set operation fails and cannot be retried.
    /// </exception>
    public async Task<(bool, long, int)> TrySetKeyValue(
        string url, 
        HLCTimestamp transactionId, 
        string key, 
        byte[]? value, 
        int expiryTime, 
        KeyValueFlags flags, 
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        KahunaSetKeyValueRequest request = new()
        {
            TransactionId = transactionId,
            Key = key,
            Value = value,
            ExpiresMs = expiryTime,
            Flags = flags,
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        int retries = 0;
        KahunaSetKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaSetKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Set)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.NotSet)
                return (false, response.Revision, 0);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to set key/value: " + response.Type, response.Type);
    }

    public Task<(List<KahunaSetKeyValueResponseItem>, int)> TrySetManyKeyValues(string url, IEnumerable<KahunaSetKeyValueRequestItem> requestItems, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Attempts to compare the specified value with an existing key's value and set a new value if they match.
    /// </summary>
    /// <param name="url">The endpoint URL for the key-value operation.</param>
    /// <param name="transactionId">The transaction identifier for this key-value operation.</param>
    /// <param name="key">The key of the entry to compare and optionally set.</param>
    /// <param name="value">The value to set if the comparison succeeds.</param>
    /// <param name="compareValue">The value to compare against the existing key's value.</param>
    /// <param name="expiryTime">The expiration time of the key-value pair in milliseconds.</param>
    /// <param name="durability">The durability type of the key-value operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>
    /// A tuple containing a boolean indicating whether the operation succeeded,
    /// the updated expiration time in milliseconds, and an integer representing the time taken for the operation.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the key-value operation fails due to an unrecoverable error.
    /// </exception>
    public async Task<(bool, long, int)> TryCompareValueAndSetKeyValue(
        string url,
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        int expiryTime,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        KahunaSetKeyValueRequest request = new()
        {
            TransactionId = transactionId,
            Key = key, 
            Value = value, 
            CompareValue = compareValue,
            ExpiresMs = expiryTime,
            Flags = KeyValueFlags.SetIfEqualToValue,
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);
        
        int retries = 0;
        KahunaSetKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaSetKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Set)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.NotSet)
                return (false, response.Revision, 0);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to set key/value: " + response.Type, response.Type);
    }

    /// <summary>
    /// Attempts to compare the revision of an existing key and sets a new value if the comparison succeeds.
    /// </summary>
    /// <param name="url">The endpoint URL for the key-value operation.</param>
    /// <param name="transactionId">The unique transaction identifier for this operation.</param>
    /// <param name="key">The key to be compared and updated.</param>
    /// <param name="value">The new value to set if the revision comparison succeeds, or null to remove the key.</param>
    /// <param name="compareRevision">The revision value to compare the key against.</param>
    /// <param name="expiryTime">The expiration duration of the key in milliseconds.</param>
    /// <param name="durability">The durability type of the key-value pair (ephemeral or persistent).</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>
    /// A tuple containing a boolean indicating success or failure, the new revision of the key,
    /// and the time taken for the operation in milliseconds.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the key-value operation fails due to a non-retriable error.
    /// </exception>
    public async Task<(bool, long, int)> TryCompareRevisionAndSetKeyValue(
        string url,
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        long compareRevision,
        int expiryTime,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        KahunaSetKeyValueRequest request = new()
        {
            TransactionId = transactionId,
            Key = key, 
            Value = value, 
            CompareRevision = compareRevision,
            ExpiresMs = expiryTime,
            Flags = KeyValueFlags.SetIfEqualToRevision,
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);
        
        int retries = 0;
        KahunaSetKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaSetKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Set)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.NotSet)
                return (false, response.Revision, 0);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to set key/value: " + response.Type, response.Type);
    }

    /// <summary>
    /// Attempts to retrieve a key-value pair from the specified endpoint along with its metadata.
    /// </summary>
    /// <param name="url">The URL of the endpoint for the key-value retrieval request.</param>
    /// <param name="transactionId">The globally unique transaction identifier for the operation.</param>
    /// <param name="key">The key associated with the value to be retrieved.</param>
    /// <param name="revision">The specific revision of the key-value pair to fetch.</param>
    /// <param name="durability">Specifies the durability type (e.g., ephemeral or persistent) for the requested data.</param>
    /// <param name="cancellationToken">A token that can be used to cancel the operation before completion.</param>
    /// <returns>
    /// A tuple containing a boolean indicating success, the retrieved value as a byte array (or null if not found),
    /// the revision of the fetched key, and an integer representing the time taken to execute the operation.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the key-value retrieval process encounters an unrecoverable error.
    /// </exception>
    public async Task<(bool, byte[]?, long, int)> TryGetKeyValue(
        string url,
        HLCTimestamp transactionId,
        string key,
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        KahunaGetKeyValueRequest request = new()
        {
            TransactionId = transactionId,
            Key = key, 
            Revision = revision,
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaGetKeyValueRequest);
        
        int retries = 0;
        KahunaGetKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-get")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaGetKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Get)
                return (true, response.Value, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, null, response.Revision, 0);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to get key/value: " + response.Type, response.Type);
    }

    /// <summary>
    /// Attempts to verify the existence of a specific key-value pair in the system with the given parameters.
    /// </summary>
    /// <param name="url">The endpoint URL for the key-value existence request.</param>
    /// <param name="transactionId">The unique transaction identifier.</param>
    /// <param name="key">The key associated with the key-value pair to check existence.</param>
    /// <param name="revision">The specific revision number of the key-value pair.</param>
    /// <param name="durability">The durability type of the key-value (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation if needed.</param>
    /// <returns>
    /// A tuple containing:
    /// - A boolean indicating if the key-value pair exists.
    /// - A long value representing the revision of the key.
    /// - An integer representing the time taken to execute the operation.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the key-value existence check operation fails due to a non-retryable error.
    /// </exception>
    public async Task<(bool, long, int)> TryExistsKeyValue(
        string url,
        HLCTimestamp transactionId,
        string key,
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        KahunaExistsKeyValueRequest request = new()
        {
            TransactionId = transactionId,
            Key = key, 
            Revision = revision,
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaExistsKeyValueRequest);
        
        KahunaExistsKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-exists")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaExistsKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Exists)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, response.Revision, 0);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to check if exists key/value: " + response.Type, response.Type);
    }

    /// <summary>
    /// Attempts to delete a key-value pair for the specified key with the given configuration and transaction context.
    /// </summary>
    /// <param name="url">The endpoint URL of the key-value store service.</param>
    /// <param name="transactionId">The transaction identifier for the deletion operation.</param>
    /// <param name="key">The key of the key-value pair to be deleted.</param>
    /// <param name="durability">The desired durability level for the operation, indicating whether the deletion should be ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe for cancellation of the operation.</param>
    /// <returns>
    /// A tuple containing a boolean indicating if the deletion was successful,
    /// the duration it took to process the request in milliseconds, and an integer representing the time taken to execute the operation.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the deletion operation fails for a non-retryable reason.
    /// </exception>
    public async Task<(bool, long, int)> TryDeleteKeyValue(
        string url, 
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        KahunaDeleteKeyValueRequest request = new()
        {
            TransactionId = transactionId,
            Key = key,
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaDeleteKeyValueRequest);
        
        KahunaDeleteKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-delete")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaDeleteKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Deleted)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, response.Revision, 0);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to delete key/value: " + response.Type, response.Type);
    }

    /// <summary>
    /// Attempts to extend the expiration time of a specified key/value pair with the given configuration.
    /// </summary>
    /// <param name="url">The endpoint URL to be used for the operation.</param>
    /// <param name="transactionId">The transaction identifier associated with the operation.</param>
    /// <param name="key">The key of the key/value pair to be extended.</param>
    /// <param name="expiresMs">The new expiration time for the key/value pair in milliseconds.</param>
    /// <param name="durability">The durability type of the key/value pair (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>
    /// A tuple containing a boolean indicating success or failure, the updated expiration time in milliseconds, and an integer representing the time taken to execute the operation
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown when the operation fails permanently and cannot be retried.
    /// </exception>
    public async Task<(bool, long, int)> TryExtendKeyValue(
        string url, 
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        KahunaExtendKeyValueRequest request = new()
        {
            TransactionId = transactionId,
            Key = key,
            ExpiresMs = expiresMs,
            Durability = durability
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaExtendKeyValueRequest);
        
        KahunaDeleteKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-extend")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaDeleteKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Extended)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, response.Revision, 0);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        throw new KahunaException("Failed to extend key/value: " + response.Type, response.Type);
    }

    /// <summary>
    /// Attempts to execute a key-value transaction script on the specified endpoint.
    /// </summary>
    /// <param name="url">The endpoint URL to which the transaction script is sent.</param>
    /// <param name="script">The script in binary format to be executed as part of the transaction.</param>
    /// <param name="hash">An optional hash representing the script version or content for verification.</param>
    /// <param name="parameters">A list of key-value parameters required for the script execution.</param>
    /// <param name="cancellationToken">A cancellation token used to cancel the operation.</param>
    /// <returns>
    /// An instance of <see cref="KahunaKeyValueTransactionResult"/> representing the outcome of the transaction script execution.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown if the transaction fails, is aborted, or cannot be completed due to an unrecoverable error.
    /// </exception>
    public async Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransactionScript(string url, byte[] script, string? hash, List<KeyValueParameter>? parameters, CancellationToken cancellationToken)
    {
        KeyValueTransactionRequest request = new()
        {
            Hash = hash,
            Script = script,
            Parameters = parameters
        };
        
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KeyValueTransactionRequest);

        int retries = 0;
        KeyValueTransactionResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
        
            response = await retryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-execute-tx-script")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KeyValueTransactionResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
            if (response.Type is < KeyValueResponseType.Errored or KeyValueResponseType.DoesNotExist)
                return new()
                {
                    Type = response.Type,
                    //Value = response.Values,
                    //Revision = response.Revision
                };
            
            if (response.Type == KeyValueResponseType.MustRetry)
                logger?.LogDebug("Server asked to retry transaction");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        //throw new KahunaException("Failed to extend key/value: " + response.Type, response.Type);
        
        if (!string.IsNullOrEmpty(response.Reason))
            throw new KahunaException(response.Reason, response.Type);

        if (response.Type == KeyValueResponseType.Aborted)
            throw new KahunaException("Transaction aborted", response.Type);

        throw new KahunaException("Failed to execute key/value transaction:" + response.Type, response.Type);
    }

    public Task<bool> TryAcquireExclusiveKeyValueLock(string url, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task<(bool, List<string>)> GetByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task<(bool, List<string>)> ScanAllByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task<(string, HLCTimestamp transactionId)> StartTransactionSession(string url, string uniqueId, KahunaTransactionOptions txOptions, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task<bool> CommitTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task<bool> RollbackTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}