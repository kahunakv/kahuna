
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using System.Security.Cryptography;
using System.Text.Json;
using Flurl.Http;
using Kommander.Diagnostics;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
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
    /// <summary>
    /// Guards one-time Flurl global configuration so multiple instances don't race.
    /// Flurl's WithDefaults is process-global; we set it at most once per process.
    /// </summary>
    private static int flurlConfigured;

    private readonly ILogger? logger;

    public RestCommunication(ILogger? logger, KahunaOptions? options = null)
    {
        this.logger = logger;
        ConfigureFlurl(options);
    }

    private static void ConfigureFlurl(KahunaOptions? options)
    {
        // Check first whether there is anything to apply; only then latch the guard.
        // This prevents a null-options (no-op) first call from consuming the latch and
        // silently blocking a later non-default config.
        // Residual: two distinct non-default configs still first-wins — Flurl WithDefaults
        // is inherently process-global.
        if (options is null || (!options.AllowInsecureCertificateValidation && options.TrustedServerCertificateThumbprints.Count == 0))
            return; // leave platform-default validation in place

        if (Interlocked.CompareExchange(ref flurlConfigured, 1, 0) != 0)
            return;

        if (options.AllowInsecureCertificateValidation)
        {
            FlurlHttp.Clients.WithDefaults(x => x.ConfigureInnerHandler(
                ih => ih.ServerCertificateCustomValidationCallback = (_, _, _, _) => true));
            return;
        }

        // Thumbprint pinning
        IReadOnlyList<string> thumbprints = options.TrustedServerCertificateThumbprints;
        FlurlHttp.Clients.WithDefaults(x => x.ConfigureInnerHandler(ih =>
            ih.ServerCertificateCustomValidationCallback = (_, certificate, _, _) =>
            {
                if (certificate is null) return false;
                byte[] hash = SHA256.HashData(certificate.GetRawCertData());
                string thumbprint = Convert.ToHexString(hash);
                return thumbprints.Any(t => string.Equals(t, thumbprint, StringComparison.OrdinalIgnoreCase));
            }));
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
        logger?.LogWarning("Retry: {Exception} {Time}", ex.Message, timeSpan);
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

    private static bool IsCancellationException(Exception exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is OperationCanceledException)
                return true;
        }

        return false;
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

        // MustRetry is unbounded — same policy as the gRPC transport.  Termination is driven by
        // the caller's CancellationToken (the CT3 default deadline only bounds a single
        // unresponsive call inside the batcher, not this retry loop).  The backoff grows from
        // ~1ms to ~10ms over the first 10 steps then caps, so a stuck server is not busy-polled.
        using IEnumerator<TimeSpan> mustRetryBackoff = Backoff
            .DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: 10)
            .GetEnumerator();
        TimeSpan mustRetryDelay = TimeSpan.FromMilliseconds(1);
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);

        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);

            KahunaLockResponse? response;

            try
            {
                response = await retryPolicy.ExecuteAsync(() =>
                    url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/locks/try-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaLockResponse>()).ConfigureAwait(false);
            }
            catch (FlurlHttpException ex) when (cancellationToken.IsCancellationRequested && IsCancellationException(ex))
            {
                throw new OperationCanceledException("Operation cancelled", ex, cancellationToken);
            }

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == LockResponseType.Locked)
                return (KahunaLockAcquireResult.Success, response.FencingToken, response.ServedFrom);

            if (response.Type == LockResponseType.Busy)
                return (KahunaLockAcquireResult.Conflicted, response.FencingToken, response.ServedFrom);

            if (response.Type != LockResponseType.MustRetry)
                throw new KahunaException("Failed to lock", response.Type);

            if (mustRetryBackoff.MoveNext())
                mustRetryDelay = mustRetryBackoff.Current;

            await Task.Delay(mustRetryDelay, cancellationToken).ConfigureAwait(false);
        }
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        // The REST transport does not yet forward register-remote operation identity; coordinatorKey
        // and operationId are accepted to satisfy the shared communication contract but are not sent.
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

    public async Task<(List<KahunaSetKeyValueResponseItem>, int)> TrySetManyKeyValues(
        string url,
        IEnumerable<KahunaSetKeyValueRequestItem> requestItems,
        CancellationToken cancellationToken
    )
    {
        KahunaSetManyKeyValueRequest request = new()
        {
            Items = [.. requestItems]
        };

        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaSetManyKeyValueRequest);

        if (cancellationToken.IsCancellationRequested)
            throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);

        KahunaSetManyKeyValueResponse? response = await retryPolicy.ExecuteAsync(() =>
            url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/kv/try-set-many")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload, cancellationToken: cancellationToken)
                .ReceiveJson<KahunaSetManyKeyValueResponse>())
                .ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null", KeyValueResponseType.Errored);

        return (response.Items ?? [], response.TimeElapsedMs);
    }

    public async Task<(List<KahunaDeleteKeyValueResponseItem>, int)> TryDeleteManyKeyValues(
        string url,
        IEnumerable<KahunaDeleteKeyValueRequestItem> requestItems,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        // REST does not yet forward batch operation identity to the coordinator; the anchored
        // register-remote path is exercised over gRPC.
        KahunaDeleteManyKeyValueRequest request = new()
        {
            Items = [.. requestItems]
        };

        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaDeleteManyKeyValueRequest);

        if (cancellationToken.IsCancellationRequested)
            throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);

        KahunaDeleteManyKeyValueResponse? response = await retryPolicy.ExecuteAsync(() =>
            url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/kv/try-delete-many")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload, cancellationToken: cancellationToken)
                .ReceiveJson<KahunaDeleteManyKeyValueResponse>())
                .ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null", KeyValueResponseType.Errored);

        return (response.Items ?? [], response.TimeElapsedMs);
    }

    public Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryGetManyKeyValues(string url, HLCTimestamp transactionId, IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken)
        => throw new NotSupportedException("GetManyKeyValues is not available over the REST transport; use the gRPC transport.");

    public Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryExistsManyKeyValues(string url, HLCTimestamp transactionId, IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken)
        => throw new NotSupportedException("ExistsManyKeyValues is not available over the REST transport; use the gRPC transport.");

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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
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
    public async Task<(bool, byte[]?, long, HLCTimestamp, int)> TryGetKeyValue(
        string url,
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
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
                return (true, response.Value, response.Revision, HLCTimestamp.Zero, 0);

            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, null, response.Revision, HLCTimestamp.Zero, 0);
            
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
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
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

    public Task<bool> TryAcquireExclusiveKeyValueLock(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
        => throw new NotSupportedException("TryAcquireExclusiveKeyValueLock is not available over the REST transport; use the gRPC transport.");

    public Task<bool> TryAcquireExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
        => throw new NotSupportedException("TryAcquireExclusivePrefixKeyValueLock is not available over the REST transport; use the gRPC transport.");

    public Task TryReleaseExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
        => throw new NotSupportedException("TryReleaseExclusivePrefixKeyValueLock is not available over the REST transport; use the gRPC transport.");

    public Task<bool> TryAcquireRangeKeyValueLock(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
        => throw new NotSupportedException("TryAcquireRangeKeyValueLock is not available over the REST transport; use the gRPC transport.");

    public Task TryReleaseExclusiveRangeKeyValueLock(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken)
        => throw new NotSupportedException("TryReleaseExclusiveRangeKeyValueLock is not available over the REST transport; use the gRPC transport.");

    public Task<KeyValueGetByRangePageResult> GetByRange(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
        => throw new NotSupportedException("GetByRange is not available over the REST transport; use the gRPC transport.");

    public IAsyncEnumerable<KeyValueGetByBucketItem> ScanByRange(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
        => throw new NotSupportedException("ScanByRange is not available over the REST transport; use the gRPC transport.");

    public Task<List<KeyValueGetByBucketItem>> GetByBucket(string url, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
        => throw new NotSupportedException("GetByBucket is not available over the REST transport; use the gRPC transport.");

    public Task<List<KeyValueGetByBucketItem>> ScanAllByPrefix(string url, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
        => throw new NotSupportedException("ScanAllByPrefix is not available over the REST transport; use the gRPC transport.");

    public Task<(string, HLCTimestamp transactionId)> StartTransactionSession(string url, string uniqueId, KahunaTransactionOptions txOptions, CancellationToken cancellationToken)
        => throw new NotSupportedException("StartTransactionSession is not available over the REST transport; use the gRPC transport.");

    public Task<(bool committed, string? recordAnchorKey)> CommitTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, string? recordAnchorKey, CancellationToken cancellationToken)
        => throw new NotSupportedException("CommitTransactionSession is not available over the REST transport; use the gRPC transport.");

    public Task<bool> RollbackTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, string? recordAnchorKey, CancellationToken cancellationToken)
        => throw new NotSupportedException("RollbackTransactionSession is not available over the REST transport; use the gRPC transport.");

    public async Task<(SequenceResponseType, ReadOnlySequenceEntry?, int)> GetSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        KahunaSequenceNameRequest request = new() { Name = name, Durability = durability };
        KahunaSequenceResponse response = await PostSequenceRequest(url, "get", request, KahunaJsonContext.Default.KahunaSequenceNameRequest, cancellationToken).ConfigureAwait(false);
        return (response.Type, response.Sequence, (int)stopwatch.GetElapsedMilliseconds());
    }

    public async Task<(SequenceResponseType, long, int)> CreateSequence(string url, string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        KahunaSequenceCreateRequest request = new() { Name = name, InitialValue = initialValue, Increment = increment, MaxValue = maxValue, Durability = durability };
        KahunaSequenceResponse response = await PostSequenceRequest(url, "create", request, KahunaJsonContext.Default.KahunaSequenceCreateRequest, cancellationToken).ConfigureAwait(false);
        return (response.Type, response.Revision, (int)stopwatch.GetElapsedMilliseconds());
    }

    public async Task<(SequenceResponseType, SequenceAllocation, int)> NextSequenceValue(string url, string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        KahunaSequenceNextRequest request = new() { Name = name, IdempotencyKey = idempotencyKey, Durability = durability };
        KahunaSequenceResponse response = await PostSequenceRequest(url, "next", request, KahunaJsonContext.Default.KahunaSequenceNextRequest, cancellationToken).ConfigureAwait(false);
        return (response.Type, response.Allocation, (int)stopwatch.GetElapsedMilliseconds());
    }

    public async Task<(SequenceResponseType, SequenceAllocation, int)> ReserveSequenceRange(string url, string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        KahunaSequenceReserveRequest request = new() { Name = name, Count = count, IdempotencyKey = idempotencyKey, Durability = durability };
        KahunaSequenceResponse response = await PostSequenceRequest(url, "reserve", request, KahunaJsonContext.Default.KahunaSequenceReserveRequest, cancellationToken).ConfigureAwait(false);
        return (response.Type, response.Allocation, (int)stopwatch.GetElapsedMilliseconds());
    }

    public async Task<(SequenceResponseType, int)> DeleteSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        KahunaSequenceNameRequest request = new() { Name = name, Durability = durability };
        KahunaSequenceResponse response = await PostSequenceRequest(url, "delete", request, KahunaJsonContext.Default.KahunaSequenceNameRequest, cancellationToken).ConfigureAwait(false);
        return (response.Type, (int)stopwatch.GetElapsedMilliseconds());
    }

    private static async Task<KahunaSequenceResponse> PostSequenceRequest<T>(string url, string action, T request, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> jsonTypeInfo, CancellationToken cancellationToken)
    {
        string payload = JsonSerializer.Serialize(request, jsonTypeInfo);
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);

        KahunaSequenceResponse? response = await retryPolicy.ExecuteAsync(() =>
            url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/sequences/" + action)
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostStringAsync(payload, cancellationToken: cancellationToken)
                .ReceiveJson<KahunaSequenceResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null", SequenceResponseType.Error);

        return response;
    }

    public Task<bool> RegisterKeyRange(string url, string keySpace, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("RegisterKeyRange is not available over the REST transport; use the gRPC transport.");
    }

    public async Task<KahunaClusterMembershipResponse> GetClusterMembership(string url, CancellationToken cancellationToken)
    {
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);

        KahunaClusterMembershipResponse? response = await retryPolicy.ExecuteAsync(() =>
            url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/cluster/membership")
                .WithHeader("Accept", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .GetAsync(cancellationToken: cancellationToken)
                .ReceiveJson<KahunaClusterMembershipResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("GetClusterMembership returned null", LockResponseType.Errored);

        return response;
    }

    public async Task<KahunaBackupInfo> TakeFullBackup(string url, CancellationToken cancellationToken)
    {
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);
        KahunaBackupInfo? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups/full")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostJsonAsync(new { }, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaBackupInfo>()).ConfigureAwait(false);
        return response ?? throw new KahunaException("TakeFullBackup returned null", LockResponseType.Errored);
    }

    public async Task<KahunaBackupInfo> TakeIncrementalBackup(string url, Guid parentBackupId, CancellationToken cancellationToken)
    {
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);
        KahunaBackupInfo? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups/incremental")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostJsonAsync(new KahunaBackupIncrementalRequest { ParentBackupId = parentBackupId }, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaBackupInfo>()).ConfigureAwait(false);
        return response ?? throw new KahunaException("TakeIncrementalBackup returned null", LockResponseType.Errored);
    }

    public async Task<KahunaBackupInfo> TakeCoordinatedBackup(string url, CancellationToken cancellationToken)
    {
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);
        KahunaBackupInfo? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups/coordinated")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostJsonAsync(new { }, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaBackupInfo>()).ConfigureAwait(false);
        return response ?? throw new KahunaException("TakeCoordinatedBackup returned null", LockResponseType.Errored);
    }

    public async Task<(KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry)> AcquireSnapshotHold(
        string url, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken)
    {
        KahunaAcquireSnapshotHoldRequest request = new()
        {
            HolderId  = holderId,
            Timestamp = timestamp,
            LeaseMs   = leaseMs
        };

        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaAcquireSnapshotHoldRequest);
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);

        KahunaAcquireSnapshotHoldResponse? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/kv/snapshot-hold/acquire")
               .WithHeader("Accept", "application/json")
               .WithHeader("Content-Type", "application/json")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostStringAsync(payload, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaAcquireSnapshotHoldResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("AcquireSnapshotHold returned null", KeyValueResponseType.Errored);

        return (response.Type, response.HoldId, response.LeaseExpiry);
    }

    public async Task<(KeyValueResponseType type, HLCTimestamp leaseExpiry)> RenewSnapshotHold(
        string url, string holdId, int leaseMs, CancellationToken cancellationToken)
    {
        KahunaRenewSnapshotHoldRequest request = new() { HoldId = holdId, LeaseMs = leaseMs };

        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaRenewSnapshotHoldRequest);
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);

        KahunaRenewSnapshotHoldResponse? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/kv/snapshot-hold/renew")
               .WithHeader("Accept", "application/json")
               .WithHeader("Content-Type", "application/json")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostStringAsync(payload, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaRenewSnapshotHoldResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("RenewSnapshotHold returned null", KeyValueResponseType.Errored);

        return (response.Type, response.LeaseExpiry);
    }

    public async Task<KeyValueResponseType> ReleaseSnapshotHold(
        string url, string holdId, CancellationToken cancellationToken)
    {
        KahunaReleaseSnapshotHoldRequest request = new() { HoldId = holdId };

        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaReleaseSnapshotHoldRequest);
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);

        KahunaReleaseSnapshotHoldResponse? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/kv/snapshot-hold/release")
               .WithHeader("Accept", "application/json")
               .WithHeader("Content-Type", "application/json")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostStringAsync(payload, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaReleaseSnapshotHoldResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("ReleaseSnapshotHold returned null", KeyValueResponseType.Errored);

        return response.Type;
    }

    public async Task<(HLCTimestamp effectiveFloor, int liveHolds)> GetSnapshotFloor(
        string url, CancellationToken cancellationToken)
    {
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);

        KahunaGetSnapshotFloorResponse? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/kv/snapshot-floor")
               .WithSettings(o => o.HttpVersion = "2.0")
               .GetAsync(cancellationToken: cancellationToken)
               .ReceiveJson<KahunaGetSnapshotFloorResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("GetSnapshotFloor returned null", KeyValueResponseType.Errored);

        return (response.EffectiveFloor, response.LiveHolds);
    }

    public async Task<List<KahunaBackupInfo>> ListBackups(string url, CancellationToken cancellationToken)
    {
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);
        List<KahunaBackupInfo>? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups")
               .WithSettings(o => o.HttpVersion = "2.0")
               .GetAsync(cancellationToken: cancellationToken)
               .ReceiveJson<List<KahunaBackupInfo>>()).ConfigureAwait(false);
        return response ?? [];
    }

    public async Task<List<KahunaBackupInfo>> GetBackupChain(string url, Guid leafBackupId, CancellationToken cancellationToken)
    {
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);
        List<KahunaBackupInfo>? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups", leafBackupId.ToString(), "chain")
               .WithSettings(o => o.HttpVersion = "2.0")
               .GetAsync(cancellationToken: cancellationToken)
               .ReceiveJson<List<KahunaBackupInfo>>()).ConfigureAwait(false);
        return response ?? [];
    }

    public async Task<KahunaRestoreResponse> Restore(string url, Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken cancellationToken)
    {
        AsyncRetryPolicy retryPolicy = BuildRetryPolicy(logger);
        KahunaRestoreResponse? response = await retryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/restore")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostJsonAsync(new KahunaBackupRestoreRequest
               {
                   LeafBackupId = leafBackupId,
                   TargetDir = targetDir,
                   TargetTimeMs = targetTimeMs
               }, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaRestoreResponse>()).ConfigureAwait(false);
        return response ?? throw new KahunaException("Restore returned null", LockResponseType.Errored);
    }
}
