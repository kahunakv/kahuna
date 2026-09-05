
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
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

    /// <summary>
    /// A JSON request body that is already encoded as UTF-8. Posting a string would build a full UTF-16
    /// copy between the JSON writer and the socket, and HTTP carries bytes, so that copy is pure waste.
    /// It derives from <see cref="ByteArrayContent"/>, so the body stays readable more than once and a
    /// redirect or a retry can replay it. The caller owns the array and must not mutate it after the
    /// send, because every attempt reads the same buffer.
    /// </summary>
    internal sealed class Utf8JsonContent : ByteArrayContent
    {
        public Utf8JsonContent(byte[] utf8Json) : base(utf8Json)
        {
            Headers.TryAddWithoutValidation("Content-Type", "application/json");
        }
    }

    /// <summary>
    /// Retry policy for the calls that do not log a retry. A Polly policy holds no state across
    /// executions: each execution asks the jitter sequence for its own enumerator, so concurrent
    /// callers keep independent delays and independent attempt counts. One instance therefore serves
    /// the whole process, instead of a fresh policy, builder, jitter sequence and callback per call.
    /// </summary>
    private static readonly AsyncRetryPolicy SharedRetryPolicy = BuildRetryPolicy(null);

    private readonly ILogger? logger;

    /// <summary>
    /// Retry policy for the calls that log a retry. The logger belongs to this instance, so this
    /// policy cannot be shared across the process the way <see cref="SharedRetryPolicy"/> is.
    /// </summary>
    private readonly AsyncRetryPolicy loggingRetryPolicy;

    public RestCommunication(ILogger? logger, KahunaOptions? options = null)
    {
        this.logger = logger;
        loggingRetryPolicy = BuildRetryPolicy(logger);
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
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaLockRequest);

        // MustRetry is unbounded — same policy as the gRPC transport.  Termination is driven by
        // the caller's CancellationToken (the CT3 default deadline only bounds a single
        // unresponsive call inside the batcher, not this retry loop).  The backoff grows from
        // ~1ms to ~10ms over the first 10 steps then caps, so a stuck server is not busy-polled.
        // The sequence is built on the first refusal, not before the first attempt: an acquisition that
        // succeeds outright is the common case, and it needs no backoff state at all. The gRPC
        // transport initializes the equivalent state the same way.
        IEnumerator<TimeSpan>? mustRetryBackoff = null;
        TimeSpan mustRetryDelay = TimeSpan.FromMilliseconds(1);

        try
        {
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    throw new KahunaException("Operation cancelled", LockResponseType.Errored);

                KahunaLockResponse? response;

                try
                {
                    response = await SharedRetryPolicy.ExecuteAsync(() =>
                        url
                        .WithOAuthBearerToken("xxx")
                        .AppendPathSegments("v1/locks/try-lock")
                        .WithHeader("Accept", "application/json")
                        .WithHeader("Content-Type", "application/json")
                        .WithSettings(o => o.HttpVersion = "2.0")
                        .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
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

                mustRetryBackoff ??= Backoff
                    .DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: 10)
                    .GetEnumerator();

                if (mustRetryBackoff.MoveNext())
                    mustRetryDelay = mustRetryBackoff.Current;

                await Task.Delay(mustRetryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            mustRetryBackoff?.Dispose();
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

        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaLockRequest);
        
        // The server maps every transient failure of a release — a leader flip, an unresolved leader,
        // a storage stall — to MustRetry, so MustRetry is the normal shape of a release that has not
        // failed. The loop is bounded by a deadline, not by an attempt count: a fixed count of attempts
        // spends its whole budget in the first few milliseconds, and a release that gives up leaves the
        // lock held until its expiry.
        int retries = 0;
        long retryDeadline = 0;

        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            KahunaLockResponse? response = await SharedRetryPolicy.ExecuteAsync(() => 
                url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/locks/try-unlock")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<KahunaLockResponse>())
                .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == LockResponseType.Unlocked)
                return true;

            if (response.Type == LockResponseType.LockDoesNotExist)
                return false;

            // Report the code the server actually sent. A retry budget must never overwrite it.
            if (response.Type != LockResponseType.MustRetry)
                throw new KahunaException("Failed to unlock: " + response.Type, response.Type);

            if (retries == 0)
                retryDeadline = GetLockRetryDeadline();
            else if (Stopwatch.GetTimestamp() >= retryDeadline)
                throw new KahunaException("Retries exhausted.", LockResponseType.MustRetry);

            await WaitBeforeMustRetry(retries, cancellationToken).ConfigureAwait(false);

            retries++;
        }
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
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaLockRequest);

        // See TryUnlock: MustRetry is a transient server condition, so the loop is bounded by a
        // deadline instead of a fixed count of attempts that all fall inside the same few milliseconds.
        int retries = 0;
        long retryDeadline = 0;

        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            KahunaLockResponse? response = await SharedRetryPolicy.ExecuteAsync(() => 
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/locks/try-extend")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaLockResponse>())
                    .ConfigureAwait(false);
            
            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
            
            if (response.Type == LockResponseType.Extended)
                return (true, response.FencingToken);

            // Report the code the server actually sent. A retry budget must never overwrite it.
            if (response.Type != LockResponseType.MustRetry)
                throw new KahunaException("Failed to extend lock: " + response.Type, response.Type);

            if (retries == 0)
                retryDeadline = GetLockRetryDeadline();
            else if (Stopwatch.GetTimestamp() >= retryDeadline)
                throw new KahunaException("Retries exhausted.", LockResponseType.MustRetry);

            await WaitBeforeMustRetry(retries, cancellationToken).ConfigureAwait(false);

            retries++;
        }
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

        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaGetLockRequest);

        // See TryUnlock: MustRetry is a transient server condition, so the loop is bounded by a
        // deadline instead of a fixed count of attempts that all fall inside the same few milliseconds.
        int retries = 0;
        long retryDeadline = 0;

        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            KahunaGetLockResponse? response = await SharedRetryPolicy.ExecuteAsync(() =>
                    url
                        .WithOAuthBearerToken("xxx")
                        .AppendPathSegments("v1/locks/get-info")
                        .WithHeader("Accept", "application/json")
                        .WithHeader("Content-Type", "application/json")
                        .WithSettings(o => o.HttpVersion = "2.0")
                        .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                        .ReceiveJson<KahunaGetLockResponse>())
                        .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == LockResponseType.Got)
                return new(response.Owner, response.Expires, response.FencingToken);

            // Report the code the server actually sent. A retry budget must never overwrite it.
            if (response.Type != LockResponseType.MustRetry)
                throw new KahunaException("Failed to get lock information: " + response.Type, response.Type);

            if (retries == 0)
                retryDeadline = GetLockRetryDeadline();
            else if (Stopwatch.GetTimestamp() >= retryDeadline)
                throw new KahunaException("Retries exhausted.", LockResponseType.MustRetry);

            await WaitBeforeMustRetry(retries, cancellationToken).ConfigureAwait(false);

            retries++;
        }
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
        KahunaSetKeyValueRequest request = new()
        {
            TransactionId = transactionId,
            Key = key,
            Value = value,
            ExpiresMs = expiryTime,
            Flags = flags,
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);

        int retries = 0;
        KahunaSetKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            response = await SharedRetryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaSetKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Set)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.NotSet)
                return (false, response.Revision, 0);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

            if (response.Type == KeyValueResponseType.MustRetry)
                await WaitBeforeMustRetry(retries - 1, cancellationToken).ConfigureAwait(false);

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

        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaSetManyKeyValueRequest);

        if (cancellationToken.IsCancellationRequested)
            throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

        KahunaSetManyKeyValueResponse? response = await SharedRetryPolicy.ExecuteAsync(() =>
            url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/kv/try-set-many")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<KahunaSetManyKeyValueResponse>())
                .ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null", KeyValueResponseType.Errored);

        // A retryable server-side failure arrives as HTTP 200 whose body is only {"type":101}:
        // returning its absent item list as an empty batch would read as "nothing was written".
        // Per-item outcomes (including InvalidInput rejections) still flow through Items.
        if (response.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.Errored)
            throw new KahunaException("TrySetManyKeyValues failed", response.Type);

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
        // The whole batch registers as one coordinator operation so its confirmed persistent keys anchor
        // the transaction record deterministically. Absent for the non-transactional batch path.
        KahunaDeleteManyKeyValueRequest request = new()
        {
            Items = [.. requestItems]
        };

        if (!string.IsNullOrEmpty(coordinatorKey) && !operationId.IsEmpty)
        {
            request.CoordinatorKey = coordinatorKey;
            request.OperationIdHigh = operationId.High;
            request.OperationIdLow = operationId.Low;
        }

        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaDeleteManyKeyValueRequest);

        if (cancellationToken.IsCancellationRequested)
            throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

        KahunaDeleteManyKeyValueResponse? response = await SharedRetryPolicy.ExecuteAsync(() =>
            url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/kv/try-delete-many")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<KahunaDeleteManyKeyValueResponse>())
                .ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null", KeyValueResponseType.Errored);

        // Same refusal classification as TrySetManyKeyValues: an envelope-level MustRetry has no
        // per-key outcomes, so surfacing it as an empty batch would misreport "nothing was deleted".
        if (response.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.Errored)
            throw new KahunaException("TryDeleteManyKeyValues failed", response.Type);

        return (response.Items ?? [], response.TimeElapsedMs);
    }

    public Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryGetManyKeyValues(string url, HLCTimestamp transactionId, IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken)
        => PostManyKeyValues(url, "try-get-many", transactionId, requestItems, cancellationToken);

    public Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryExistsManyKeyValues(string url, HLCTimestamp transactionId, IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken)
        => PostManyKeyValues(url, "try-exists-many", transactionId, requestItems, cancellationToken);

    /// <summary>
    /// Shared body of the two batched point-read calls: both carry the same request shape and differ
    /// only in the endpoint they post to and whether the server fills in values.
    /// </summary>
    private async Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> PostManyKeyValues(
        string url,
        string verb,
        HLCTimestamp transactionId,
        IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems,
        CancellationToken cancellationToken
    )
    {
        KahunaManyKeyValuesRequest request = new()
        {
            TransactionId = transactionId,
            Items = requestItems as List<KahunaGetManyKeyValuesRequestItem> ?? [.. requestItems]
        };

        KahunaManyKeyValuesResponse response = await PostKeyValueRequest<KahunaManyKeyValuesRequest, KahunaManyKeyValuesResponse>(
            url, verb, request, KahunaJsonContext.Default.KahunaManyKeyValuesRequest, cancellationToken
        ).ConfigureAwait(false);

        // A refusal envelope carries no per-key outcomes: treating it as an empty batch would read
        // as "none of these keys exist" for a request that never reached a handler.
        if (response.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.Errored)
            throw new KahunaException($"{verb} failed", response.Type);

        return (response.Items ?? [], response.TimeElapsedMs);
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
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);
        
        int retries = 0;
        KahunaSetKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
            
            response = await SharedRetryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaSetKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Set)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.NotSet)
                return (false, response.Revision, 0);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

            if (response.Type == KeyValueResponseType.MustRetry)
                await WaitBeforeMustRetry(retries - 1, cancellationToken).ConfigureAwait(false);

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
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaSetKeyValueRequest);
        
        int retries = 0;
        KahunaSetKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
            
            response = await SharedRetryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-set")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaSetKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Set)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.NotSet)
                return (false, response.Revision, 0);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

            if (response.Type == KeyValueResponseType.MustRetry)
                await WaitBeforeMustRetry(retries - 1, cancellationToken).ConfigureAwait(false);

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
            ReadTimestamp = readTimestamp,
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaGetKeyValueRequest);
        
        int retries = 0;
        KahunaGetKeyValueResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
            
            response = await SharedRetryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-get")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaGetKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Get)
                return (true, response.Value, response.Revision, response.LastModified, 0);

            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, null, response.Revision, HLCTimestamp.Zero, 0);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

            if (response.Type == KeyValueResponseType.MustRetry)
                await WaitBeforeMustRetry(retries - 1, cancellationToken).ConfigureAwait(false);

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
            ReadTimestamp = readTimestamp,
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaExistsKeyValueRequest);
        
        KahunaExistsKeyValueResponse? response;
        
        int retries = 0;
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            response = await SharedRetryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-exists")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaExistsKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Exists)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, response.Revision, 0);

            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

            if (response.Type == KeyValueResponseType.MustRetry)
                await WaitBeforeMustRetry(retries - 1, cancellationToken).ConfigureAwait(false);

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
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaDeleteKeyValueRequest);
        
        KahunaDeleteKeyValueResponse? response;
        
        int retries = 0;
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            response = await SharedRetryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-delete")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaDeleteKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Deleted)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, response.Revision, 0);

            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

            if (response.Type == KeyValueResponseType.MustRetry)
                await WaitBeforeMustRetry(retries - 1, cancellationToken).ConfigureAwait(false);

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
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaExtendKeyValueRequest);
        
        KahunaDeleteKeyValueResponse? response;
        
        int retries = 0;
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            response = await SharedRetryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-extend")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaDeleteKeyValueResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == KeyValueResponseType.Extended)
                return (true, response.Revision, 0);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                return (false, response.Revision, 0);

            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

            if (response.Type == KeyValueResponseType.MustRetry)
                await WaitBeforeMustRetry(retries - 1, cancellationToken).ConfigureAwait(false);

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
    public async Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransactionScript(string url, byte[] script, string? hash, List<KeyValueParameter>? parameters, CancellationToken cancellationToken, TransactionPriority priority = TransactionPriority.Normal)
    {
        KeyValueTransactionRequest request = new()
        {
            Hash = hash,
            Script = script,
            Parameters = parameters,
            Priority = priority
        };
        
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KeyValueTransactionRequest);

        int retries = 0;
        KeyValueTransactionResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            response = await SharedRetryPolicy.ExecuteAsync(() =>
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kv/try-execute-tx-script")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                    .ReceiveJson<KeyValueTransactionResponse>())
                    .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
            if (response.Type is < KeyValueResponseType.Errored or KeyValueResponseType.DoesNotExist)
                return new()
                {
                    Type = response.Type,
                    Values = GetTransactionValues(response.Values)
                };
            
            if (response.Type == KeyValueResponseType.MustRetry)
                logger?.LogDebug("Server asked to retry transaction");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

            if (response.Type == KeyValueResponseType.MustRetry)
                await WaitBeforeMustRetry(retries - 1, cancellationToken).ConfigureAwait(false);

        } while (response.Type == KeyValueResponseType.MustRetry);
            
        //throw new KahunaException("Failed to extend key/value: " + response.Type, response.Type);
        
        if (!string.IsNullOrEmpty(response.Reason))
            throw new KahunaException(response.Reason, response.Type);

        if (response.Type == KeyValueResponseType.Aborted)
            throw new KahunaException("Transaction aborted", response.Type);

        throw new KahunaException("Failed to execute key/value transaction:" + response.Type, response.Type);
    }

    /// <summary>Maps the REST script response's per-value items into the client result shape,
    /// mirroring the gRPC path's mapping. Null in, null out.</summary>
    private static List<KahunaKeyValueTransactionResultValue>? GetTransactionValues(List<KahunaTxKeyValueResponseItem>? responseValues)
    {
        if (responseValues is null)
            return null;

        List<KahunaKeyValueTransactionResultValue> values = new(responseValues.Count);

        foreach (KahunaTxKeyValueResponseItem response in responseValues)
            values.Add(new()
            {
                Key = response.Key,
                Value = response.Value,
                Revision = response.Revision,
                Expires = response.Expires,
                LastModified = response.LastModified
            });

        return values;
    }

    public async Task<bool> TryAcquireExclusiveKeyValueLock(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaAcquireKeyValueLockRequest request = new()
        {
            TransactionId = transactionId,
            Key = key,
            ExpiresMs = expiresMs,
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        KahunaKeyValueLockResponse response = await PostWithMustRetry<KahunaAcquireKeyValueLockRequest, KahunaKeyValueLockResponse>(
            url, "try-acquire-exclusive-lock", request, KahunaJsonContext.Default.KahunaAcquireKeyValueLockRequest,
            r => r.Type, cancellationToken
        ).ConfigureAwait(false);

        if (response.Type == KeyValueResponseType.Locked)
            return true;

        throw new KahunaException($"Failed to acquire key/value lock for '{key}': {response.Type}.", response.Type);
    }

    public async Task<bool> TryAcquireExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaAcquireKeyValueLockRequest request = new()
        {
            TransactionId = transactionId,
            Key = prefixKey,
            ExpiresMs = expiresMs,
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        KahunaKeyValueLockResponse response = await PostWithMustRetry<KahunaAcquireKeyValueLockRequest, KahunaKeyValueLockResponse>(
            url, "try-acquire-prefix-lock", request, KahunaJsonContext.Default.KahunaAcquireKeyValueLockRequest,
            r => r.Type, cancellationToken
        ).ConfigureAwait(false);

        if (response.Type == KeyValueResponseType.Locked)
            return true;

        throw new KahunaException($"Failed to acquire exclusive prefix lock for '{prefixKey}': {response.Type}.", response.Type);
    }

    public async Task TryReleaseExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        KahunaReleaseKeyValueLockRequest request = new()
        {
            TransactionId = transactionId,
            Key = prefixKey,
            Durability = durability
        };

        await PostKeyValueRequest<KahunaReleaseKeyValueLockRequest, KahunaKeyValueLockResponse>(
            url, "try-release-prefix-lock", request, KahunaJsonContext.Default.KahunaReleaseKeyValueLockRequest, cancellationToken
        ).ConfigureAwait(false);
    }

    public async Task<bool> TryAcquireRangeKeyValueLock(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaAcquireRangeLockRequest request = new()
        {
            TransactionId = transactionId,
            Prefix = prefix,
            StartKey = startKey,
            StartInclusive = startInclusive,
            EndKey = endKey,
            EndInclusive = endInclusive,
            ExpiresMs = expiresMs,
            Durability = durability,
            Mode = mode,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        KahunaKeyValueLockResponse response = await PostWithMustRetry<KahunaAcquireRangeLockRequest, KahunaKeyValueLockResponse>(
            url, "try-acquire-range-lock", request, KahunaJsonContext.Default.KahunaAcquireRangeLockRequest,
            r => r.Type, cancellationToken
        ).ConfigureAwait(false);

        if (response.Type == KeyValueResponseType.Locked)
            return true;

        throw new KahunaException($"Failed to acquire range lock for '{prefix}': {response.Type}.", response.Type);
    }

    public async Task TryReleaseExclusiveRangeKeyValueLock(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        KahunaReleaseRangeLockRequest request = new()
        {
            TransactionId = transactionId,
            Prefix = prefix,
            StartKey = startKey,
            StartInclusive = startInclusive,
            EndKey = endKey,
            EndInclusive = endInclusive,
            Durability = durability
        };

        await PostKeyValueRequest<KahunaReleaseRangeLockRequest, KahunaKeyValueLockResponse>(
            url, "try-release-range-lock", request, KahunaJsonContext.Default.KahunaReleaseRangeLockRequest, cancellationToken
        ).ConfigureAwait(false);
    }

    public async Task<KeyValueGetByRangePageResult> GetByRange(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaGetByRangeRequest request = new()
        {
            TransactionId = transactionId,
            Prefix = prefix,
            StartKey = startKey,
            StartInclusive = startInclusive,
            EndKey = endKey,
            EndInclusive = endInclusive,
            Limit = limit,
            ReadTimestamp = readTimestamp,
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        KahunaGetByRangeResponse response = await PostWithMustRetry<KahunaGetByRangeRequest, KahunaGetByRangeResponse>(
            url, "get-by-range", request, KahunaJsonContext.Default.KahunaGetByRangeRequest,
            r => r.Type, cancellationToken
        ).ConfigureAwait(false);

        if (response.Type is not (KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist))
            throw new KahunaException($"Failed to get by range for '{prefix}': {response.Type}.", response.Type);

        return new()
        {
            Items = response.Items ?? [],
            NextCursor = response.NextCursor,
            HasMore = response.HasMore
        };
    }

    /// <summary>
    /// REST has no server-push equivalent of the gRPC range-scan stream, so the stream is reconstructed
    /// from the paged endpoint: each page's <c>nextCursor</c> is echoed back verbatim on the next request.
    /// The cursor also carries the snapshot the first page fixed, so the whole scan observes one view
    /// even though it spans several HTTP round-trips.
    /// </summary>
    public async IAsyncEnumerable<KeyValueGetByBucketItem> ScanByRange(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        KahunaGetByRangeRequest request = new()
        {
            TransactionId = transactionId,
            Prefix = prefix,
            StartKey = startKey,
            StartInclusive = startInclusive,
            EndKey = endKey,
            EndInclusive = endInclusive,
            Limit = pageSize,
            ReadTimestamp = readTimestamp,
            Durability = durability
        };

        while (true)
        {
            KahunaGetByRangeResponse response = await PostWithMustRetry<KahunaGetByRangeRequest, KahunaGetByRangeResponse>(
                url, "get-by-range", request, KahunaJsonContext.Default.KahunaGetByRangeRequest,
                r => r.Type, cancellationToken
            ).ConfigureAwait(false);

            if (response.Type is not (KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist))
                throw new KahunaException($"Failed to scan by range for '{prefix}': {response.Type}.", response.Type);

            if (response.Items is not null)
            {
                foreach (KeyValueGetByBucketItem item in response.Items)
                    yield return item;
            }

            if (!response.HasMore || string.IsNullOrEmpty(response.NextCursor))
                break;

            request.Cursor = response.NextCursor;
        }
    }

    public async Task<List<KeyValueGetByBucketItem>> GetByBucket(string url, HLCTimestamp transactionId, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaGetByBucketRequest request = new()
        {
            TransactionId = transactionId,
            PrefixKey = prefixKey,
            ReadTimestamp = readTimestamp,
            Durability = durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        KahunaGetByBucketResponse response = await PostWithMustRetry<KahunaGetByBucketRequest, KahunaGetByBucketResponse>(
            url, "get-by-bucket", request, KahunaJsonContext.Default.KahunaGetByBucketRequest,
            r => r.Type, cancellationToken
        ).ConfigureAwait(false);

        return ReadBucketItems(response, prefixKey);
    }

    public async Task<List<KeyValueGetByBucketItem>> ScanAllByPrefix(string url, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        KahunaScanAllByPrefixRequest request = new()
        {
            PrefixKey = prefixKey,
            ReadTimestamp = readTimestamp,
            Durability = durability
        };

        KahunaGetByBucketResponse response = await PostWithMustRetry<KahunaScanAllByPrefixRequest, KahunaGetByBucketResponse>(
            url, "scan-all-by-prefix", request, KahunaJsonContext.Default.KahunaScanAllByPrefixRequest,
            r => r.Type, cancellationToken
        ).ConfigureAwait(false);

        return ReadBucketItems(response, prefixKey);
    }

    private static List<KeyValueGetByBucketItem> ReadBucketItems(KahunaGetByBucketResponse response, string prefixKey)
    {
        if (response.Type == KeyValueResponseType.Get)
            return response.Items ?? [];

        // An empty bucket is an ordinary answer, not a failure.
        if (response.Type == KeyValueResponseType.DoesNotExist)
            return [];

        throw new KahunaException($"Failed to scan key/values for '{prefixKey}': {response.Type}.", response.Type);
    }

    public async Task<(string, HLCTimestamp transactionId)> StartTransactionSession(string url, string uniqueId, KahunaTransactionOptions txOptions, CancellationToken cancellationToken)
    {
        KahunaStartTransactionRequest request = new()
        {
            CoordinatorKey = uniqueId,
            Timeout = txOptions.Timeout,
            LockingType = txOptions.Locking,
            AsyncRelease = txOptions.AsyncRelease,
            AutoCommit = txOptions.AutoCommit,
            ReadValidation = txOptions.ReadValidation,
            DecisionDurability = txOptions.DecisionDurability,
            Priority = txOptions.Priority,
            ReadTimestamp = txOptions.ReadTimestamp,
            AdmissionWaitMs = txOptions.AdmissionWaitMs
        };

        KahunaStartTransactionResponse response = await PostWithMustRetry<KahunaStartTransactionRequest, KahunaStartTransactionResponse>(
            url, "start-tx-session", request, KahunaJsonContext.Default.KahunaStartTransactionRequest,
            r => r.Type, cancellationToken
        ).ConfigureAwait(false);

        if (response.Type == KeyValueResponseType.Set)
            return (url, response.TransactionId);

        throw new KahunaException("Failed to start key/value transaction: " + response.Type, response.Type);
    }

    public async Task<(bool committed, string? recordAnchorKey)> CommitTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, string? recordAnchorKey, CancellationToken cancellationToken)
    {
        KahunaCommitTransactionRequest request = new()
        {
            CoordinatorKey = uniqueId,
            TransactionId = transactionId,
            // Send the known record anchor so a retry after coordinator loss reaches the durable decision.
            RecordAnchorKey = recordAnchorKey
        };

        KahunaCommitTransactionResponse response = await PostWithMustRetry<KahunaCommitTransactionRequest, KahunaCommitTransactionResponse>(
            url, "commit-tx-session", request, KahunaJsonContext.Default.KahunaCommitTransactionRequest,
            r => r.Type, cancellationToken,
            // Carry the coordinator's canonical anchor into the next attempt: a commit that lost its
            // coordinating session still reaches the durable decision as long as the anchor travels with it.
            (req, resp) =>
            {
                if (resp.RecordAnchorKey is not null)
                    req.RecordAnchorKey = resp.RecordAnchorKey;
            }
        ).ConfigureAwait(false);

        if (response.Type == KeyValueResponseType.Committed)
            return (true, response.RecordAnchorKey);

        throw new KahunaException("Failed to commit key/value transaction: " + response.Type, response.Type);
    }

    public async Task<bool> RollbackTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, string? recordAnchorKey, CancellationToken cancellationToken)
    {
        KahunaCommitTransactionRequest request = new()
        {
            CoordinatorKey = uniqueId,
            TransactionId = transactionId,
            // The anchor lets a rollback retry consult the durable decision (a decided commit cannot be undone).
            RecordAnchorKey = recordAnchorKey
        };

        KahunaCommitTransactionResponse response = await PostWithMustRetry<KahunaCommitTransactionRequest, KahunaCommitTransactionResponse>(
            url, "rollback-tx-session", request, KahunaJsonContext.Default.KahunaCommitTransactionRequest,
            r => r.Type, cancellationToken
        ).ConfigureAwait(false);

        if (response.Type == KeyValueResponseType.RolledBack)
            return true;

        throw new KahunaException("Failed to rollback key/value transaction: " + response.Type, response.Type);
    }

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
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, jsonTypeInfo);
        KahunaSequenceResponse? response = await SharedRetryPolicy.ExecuteAsync(() =>
            url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/sequences/" + action)
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<KahunaSequenceResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null", SequenceResponseType.Error);

        return response;
    }

    // The range-administration calls below all follow the replication-factor contract: refusals come
    // back as 4xx carrying the verdict in the body, so non-2xx statuses are accepted rather than
    // turned into exceptions, and none of them is wrapped in the retry policy — whether and where to
    // repeat a map mutation is the caller's decision, guided by the response's own status.

    public async Task<KahunaRegisterKeyRangeResponse> RegisterKeyRange(string url, string keySpace, CancellationToken cancellationToken)
    {
        KahunaRegisterKeyRangeResponse? response = await url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/ranges/register")
            .WithHeader("Accept", "application/json")
            .WithSettings(o => o.HttpVersion = "2.0")
            .AllowAnyHttpStatus()
            .PostJsonAsync(new KahunaKeyRangeRequest { KeySpace = keySpace }, cancellationToken: cancellationToken)
            .ReceiveJson<KahunaRegisterKeyRangeResponse>()
            .ConfigureAwait(false);

        if (response is null || string.IsNullOrEmpty(response.Status))
            throw new KahunaException("RegisterKeyRange returned no outcome", LockResponseType.Errored);

        return response;
    }

    public async Task<KahunaRemoveKeyRangeResponse> RemoveKeyRange(string url, string keySpace, CancellationToken cancellationToken)
    {
        KahunaRemoveKeyRangeResponse? response = await url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/ranges/unregister")
            .WithHeader("Accept", "application/json")
            .WithSettings(o => o.HttpVersion = "2.0")
            .AllowAnyHttpStatus()
            .PostJsonAsync(new KahunaKeyRangeRequest { KeySpace = keySpace }, cancellationToken: cancellationToken)
            .ReceiveJson<KahunaRemoveKeyRangeResponse>()
            .ConfigureAwait(false);

        if (response is null || string.IsNullOrEmpty(response.Status))
            throw new KahunaException("RemoveKeyRange returned no outcome", LockResponseType.Errored);

        return response;
    }

    public async Task<KahunaRangeMapResponse> GetRanges(string url, string? keySpace, CancellationToken cancellationToken)
    {
        KahunaRangeMapResponse? response = await loggingRetryPolicy.ExecuteAsync(() =>
        {
            IFlurlRequest request = url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/ranges")
                .WithHeader("Accept", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0");

            if (!string.IsNullOrEmpty(keySpace))
                request = request.SetQueryParam("keySpace", keySpace);

            return request
                .GetAsync(cancellationToken: cancellationToken)
                .ReceiveJson<KahunaRangeMapResponse>();
        }).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("GetRanges returned null", LockResponseType.Errored);

        return response;
    }

    public async Task<KahunaSplitRangeResponse> SplitRange(
        string url, string keySpace, string splitKey, CancellationToken cancellationToken)
    {
        KahunaSplitRangeResponse? response = await url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/ranges/split")
            .WithHeader("Accept", "application/json")
            .WithSettings(o => o.HttpVersion = "2.0")
            .AllowAnyHttpStatus()
            .PostJsonAsync(
                new KahunaSplitRangeRequest { KeySpace = keySpace, SplitKey = splitKey },
                cancellationToken: cancellationToken)
            .ReceiveJson<KahunaSplitRangeResponse>()
            .ConfigureAwait(false);

        if (response is null || string.IsNullOrEmpty(response.Status))
            throw new KahunaException("SplitRange returned no outcome", LockResponseType.Errored);

        return response;
    }

    public async Task<KahunaMergeRangesResponse> MergeRanges(string url, CancellationToken cancellationToken)
    {
        KahunaMergeRangesResponse? response = await url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/ranges/merge")
            .WithHeader("Accept", "application/json")
            .WithSettings(o => o.HttpVersion = "2.0")
            .AllowAnyHttpStatus()
            .PostAsync(cancellationToken: cancellationToken)
            .ReceiveJson<KahunaMergeRangesResponse>()
            .ConfigureAwait(false);

        if (response is null || string.IsNullOrEmpty(response.Status))
            throw new KahunaException("MergeRanges returned no outcome", LockResponseType.Errored);

        return response;
    }

    /// <summary>
    /// How many times a call re-issues a request the server answered with MustRetry before handing the
    /// retryable outcome back to the application.
    /// </summary>
    private const int MustRetryAttempts = 5;

    /// <summary>
    /// Backoff ladder in milliseconds, applied between MustRetry attempts. MustRetry signals a transient
    /// condition — a leader flip, a write intent still settling, an in-doubt finalize a recovery sweep is
    /// resolving — and none of those clear within the microseconds an immediate re-issue takes, so retrying
    /// with no delay burns the whole retry budget inside the same instant that produced the first MustRetry
    /// while hammering a server that is, by definition, already struggling. The ladder grows from ~1ms
    /// toward ~10ms and then holds; past its end the last value is reused.
    /// </summary>
    private static readonly int[] MustRetryDelaysMs = [1, 2, 3, 4, 6, 8, 10];

    /// <summary>
    /// Waits before re-issuing a request the server answered with MustRetry. The delay carries ±25% jitter
    /// so a fleet of clients that all saw the same leader flip does not retry in lockstep and re-create the
    /// contention they are waiting out. Allocation-free and stateless, so every retry loop in this transport
    /// can share one policy.
    /// </summary>
    /// <summary>
    /// How long a lock release, extension or read keeps re-issuing a request the server answered with
    /// MustRetry. Acquisition retries without a bound, because the caller chose to wait for the lock.
    /// The other three verbs need a bound: <see cref="KahunaLock.DisposeAsync"/> releases with
    /// <see cref="CancellationToken.None"/>, so an unbounded loop there would hang the disposal of an
    /// <c>await using</c> block. The bound is a deadline rather than an attempt count, because a leader
    /// flip or a storage stall clears on a wall-clock scale, not after a fixed number of round trips.
    /// </summary>
    private static readonly TimeSpan LockMustRetryDeadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Returns the <see cref="Stopwatch"/> timestamp at which the MustRetry loop of a lock release,
    /// extension or read gives up.
    /// </summary>
    private static long GetLockRetryDeadline() =>
        Stopwatch.GetTimestamp() + (long)(LockMustRetryDeadline.TotalSeconds * Stopwatch.Frequency);

    private static Task WaitBeforeMustRetry(int attempt, CancellationToken cancellationToken)
    {
        int baseMs = MustRetryDelaysMs[Math.Min(attempt, MustRetryDelaysMs.Length - 1)];
        double jittered = baseMs * (0.75 + Random.Shared.NextDouble() * 0.5);

        return Task.Delay(TimeSpan.FromMilliseconds(jittered), cancellationToken);
    }

    /// <summary>
    /// Posts a key-value request once and returns the deserialised response. Transport-level failures are
    /// retried by the shared HTTP policy; a MustRetry answer is handed back to the caller untouched.
    /// </summary>
    private static async Task<TResponse> PostKeyValueRequest<TRequest, TResponse>(
        string url,
        string verb,
        TRequest request,
        System.Text.Json.Serialization.Metadata.JsonTypeInfo<TRequest> jsonTypeInfo,
        CancellationToken cancellationToken
    ) where TResponse : class
    {
        if (cancellationToken.IsCancellationRequested)
            throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, jsonTypeInfo);
        TResponse? response = await SharedRetryPolicy.ExecuteAsync(() =>
            url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/kv/" + verb)
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<TResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("Response is null", KeyValueResponseType.Errored);

        return response;
    }

    /// <summary>
    /// Posts a key-value request, re-issuing it while the server answers MustRetry. Exhausting the budget
    /// is reported as <see cref="KeyValueResponseType.MustRetry"/>, never as Aborted: the condition is still
    /// transient, and telling the caller the transaction genuinely conflicted would be a lie that stops them
    /// from re-driving a request that can still succeed.
    /// </summary>
    /// <param name="readType">Reads the outcome out of the response so this helper stays response-shape agnostic.</param>
    /// <param name="carryForward">
    /// Optional hook to fold state from a MustRetry response into the next attempt's request.
    /// </param>
    private async Task<TResponse> PostWithMustRetry<TRequest, TResponse>(
        string url,
        string verb,
        TRequest request,
        System.Text.Json.Serialization.Metadata.JsonTypeInfo<TRequest> jsonTypeInfo,
        Func<TResponse, KeyValueResponseType> readType,
        CancellationToken cancellationToken,
        Action<TRequest, TResponse>? carryForward = null
    ) where TResponse : class
    {
        for (int attempt = 0; ; attempt++)
        {
            TResponse response = await PostKeyValueRequest<TRequest, TResponse>(
                url, verb, request, jsonTypeInfo, cancellationToken
            ).ConfigureAwait(false);

            if (readType(response) != KeyValueResponseType.MustRetry)
                return response;

            if (logger is not null && logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("Server asked to retry {Verb}", verb);

            carryForward?.Invoke(request, response);

            if (attempt + 1 >= MustRetryAttempts)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

            await WaitBeforeMustRetry(attempt, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<KahunaClusterMembershipResponse> GetClusterMembership(string url, CancellationToken cancellationToken)
    {
        KahunaClusterMembershipResponse? response = await loggingRetryPolicy.ExecuteAsync(() =>
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

    public async Task<KahunaClusterPlacementResponse> GetClusterPlacement(string url, CancellationToken cancellationToken)
    {
        KahunaClusterPlacementResponse? response = await loggingRetryPolicy.ExecuteAsync(() =>
            url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/cluster/placement")
                .WithHeader("Accept", "application/json")
                .WithSettings(o => o.HttpVersion = "2.0")
                .GetAsync(cancellationToken: cancellationToken)
                .ReceiveJson<KahunaClusterPlacementResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("GetClusterPlacement returned null", LockResponseType.Errored);

        return response;
    }

    public async Task<KahunaSetReplicationFactorResponse> SetReplicationFactor(
        string url, int partitionId, int replicationFactor, CancellationToken cancellationToken)
    {
        // Refusals come back as 409 carrying the verdict in the body (a follower refuses; the
        // caller retries against the leader), so non-2xx statuses are accepted rather than turned
        // into exceptions — and the request is not wrapped in the retry policy: whether and where
        // to repeat a map mutation is the caller's decision.
        KahunaSetReplicationFactorResponse? response = await url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/cluster/replication-factor")
            .WithHeader("Accept", "application/json")
            .WithSettings(o => o.HttpVersion = "2.0")
            .AllowAnyHttpStatus()
            .PostJsonAsync(
                new KahunaSetReplicationFactorRequest { PartitionId = partitionId, ReplicationFactor = replicationFactor },
                cancellationToken: cancellationToken)
            .ReceiveJson<KahunaSetReplicationFactorResponse>()
            .ConfigureAwait(false);

        if (response is null || string.IsNullOrEmpty(response.Status))
            throw new KahunaException("SetReplicationFactor returned no outcome", LockResponseType.Errored);

        return response;
    }

    public async Task<KahunaClusterLeaveResponse> LeaveCluster(string url, CancellationToken cancellationToken)
    {
        // Refusals and unresolved attempts come back as 409/503/504 carrying the verdict in the
        // body, so the non-2xx statuses are accepted rather than turned into exceptions — and the
        // request is deliberately not wrapped in the retry policy: whether to repeat a decommission
        // is the caller's decision, guided by the response's own retryable flag.
        KahunaClusterLeaveResponse? response = await url
            .WithOAuthBearerToken("xxx")
            .AppendPathSegments("v1/cluster/leave")
            .WithHeader("Accept", "application/json")
            .WithSettings(o => o.HttpVersion = "2.0")
            .AllowAnyHttpStatus()
            .PostJsonAsync(new { }, cancellationToken: cancellationToken)
            .ReceiveJson<KahunaClusterLeaveResponse>()
            .ConfigureAwait(false);

        if (response is null || string.IsNullOrEmpty(response.Outcome))
            throw new KahunaException("LeaveCluster returned no outcome", LockResponseType.Errored);

        return response;
    }

    public async Task<KahunaBackupInfo> TakeFullBackup(string url, CancellationToken cancellationToken)
    {
        KahunaBackupInfo? response = await InvokeBackupRest(() => loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups/full")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostJsonAsync(new { }, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaBackupInfo>())).ConfigureAwait(false);
        return response ?? throw new KahunaException("TakeFullBackup returned null", LockResponseType.Errored);
    }

    public async Task<KahunaBackupInfo> TakeIncrementalBackup(string url, Guid parentBackupId, CancellationToken cancellationToken)
    {
        KahunaBackupInfo? response = await InvokeBackupRest(() => loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups/incremental")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostJsonAsync(new KahunaBackupIncrementalRequest { ParentBackupId = parentBackupId }, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaBackupInfo>())).ConfigureAwait(false);
        return response ?? throw new KahunaException("TakeIncrementalBackup returned null", LockResponseType.Errored);
    }

    public async Task<KahunaBackupInfo> TakeCoordinatedBackup(string url, CancellationToken cancellationToken)
    {
        KahunaBackupInfo? response = await InvokeBackupRest(() => loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups/coordinated")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostJsonAsync(new { }, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaBackupInfo>())).ConfigureAwait(false);
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

        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaAcquireSnapshotHoldRequest);
        KahunaAcquireSnapshotHoldResponse? response = await loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/kv/snapshot-hold/acquire")
               .WithHeader("Accept", "application/json")
               .WithHeader("Content-Type", "application/json")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
               .ReceiveJson<KahunaAcquireSnapshotHoldResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("AcquireSnapshotHold returned null", KeyValueResponseType.Errored);

        return (response.Type, response.HoldId, response.LeaseExpiry);
    }

    public async Task<(KeyValueResponseType type, HLCTimestamp leaseExpiry)> RenewSnapshotHold(
        string url, string holdId, int leaseMs, CancellationToken cancellationToken)
    {
        KahunaRenewSnapshotHoldRequest request = new() { HoldId = holdId, LeaseMs = leaseMs };

        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaRenewSnapshotHoldRequest);
        KahunaRenewSnapshotHoldResponse? response = await loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/kv/snapshot-hold/renew")
               .WithHeader("Accept", "application/json")
               .WithHeader("Content-Type", "application/json")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
               .ReceiveJson<KahunaRenewSnapshotHoldResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("RenewSnapshotHold returned null", KeyValueResponseType.Errored);

        return (response.Type, response.LeaseExpiry);
    }

    public async Task<KeyValueResponseType> ReleaseSnapshotHold(
        string url, string holdId, CancellationToken cancellationToken)
    {
        KahunaReleaseSnapshotHoldRequest request = new() { HoldId = holdId };

        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, KahunaJsonContext.Default.KahunaReleaseSnapshotHoldRequest);
        KahunaReleaseSnapshotHoldResponse? response = await loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/kv/snapshot-hold/release")
               .WithHeader("Accept", "application/json")
               .WithHeader("Content-Type", "application/json")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostAsync(new Utf8JsonContent(payload), cancellationToken: cancellationToken)
               .ReceiveJson<KahunaReleaseSnapshotHoldResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("ReleaseSnapshotHold returned null", KeyValueResponseType.Errored);

        return response.Type;
    }

    public async Task<(HLCTimestamp effectiveFloor, int liveHolds)> GetSnapshotFloor(
        string url, CancellationToken cancellationToken)
    {
        KahunaGetSnapshotFloorResponse? response = await loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/kv/snapshot-floor")
               .WithSettings(o => o.HttpVersion = "2.0")
               .GetAsync(cancellationToken: cancellationToken)
               .ReceiveJson<KahunaGetSnapshotFloorResponse>()).ConfigureAwait(false);

        if (response is null)
            throw new KahunaException("GetSnapshotFloor returned null", KeyValueResponseType.Errored);

        // A retryable server-side failure arrives as an HTTP 200 whose body is only {"type":101}:
        // treating it as data would report zero live holds for a request that never reached the
        // floor registry. Get is a real answer; Set (the enum default) is what a body from a server
        // that predates the type field deserializes to, so it is accepted as success too.
        if (response.Type is not (KeyValueResponseType.Get or KeyValueResponseType.Set))
            throw new KahunaException("GetSnapshotFloor failed", response.Type);

        return (response.EffectiveFloor, response.LiveHolds);
    }

    public async Task<List<KahunaBackupInfo>> ListBackups(string url, CancellationToken cancellationToken)
    {
        List<KahunaBackupInfo>? response = await InvokeBackupRest(() => loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups")
               .WithSettings(o => o.HttpVersion = "2.0")
               .GetAsync(cancellationToken: cancellationToken)
               .ReceiveJson<List<KahunaBackupInfo>>())).ConfigureAwait(false);
        return response ?? [];
    }

    public async Task<List<KahunaBackupInfo>> GetBackupChain(string url, Guid leafBackupId, CancellationToken cancellationToken)
    {
        List<KahunaBackupInfo>? response = await InvokeBackupRest(() => loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups", leafBackupId.ToString(), "chain")
               .WithSettings(o => o.HttpVersion = "2.0")
               .GetAsync(cancellationToken: cancellationToken)
               .ReceiveJson<List<KahunaBackupInfo>>())).ConfigureAwait(false);
        return response ?? [];
    }

    public async Task<KahunaRestoreResponse> Restore(string url, Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken cancellationToken)
    {
        KahunaRestoreResponse? response = await InvokeBackupRest(() => loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/restore")
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostJsonAsync(new KahunaBackupRestoreRequest
               {
                   LeafBackupId = leafBackupId,
                   TargetDir = targetDir,
                   TargetTimeMs = targetTimeMs
               }, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaRestoreResponse>())).ConfigureAwait(false);
        return response ?? throw new KahunaException("Restore returned null", LockResponseType.Errored);
    }

    public async Task<KahunaBackupGcResult> RunBackupGarbageCollection(string url, bool dryRun, CancellationToken cancellationToken)
    {
        KahunaBackupGcResult? response = await InvokeBackupRest(() => loggingRetryPolicy.ExecuteAsync(() =>
            url.WithOAuthBearerToken("xxx")
               .AppendPathSegments("v1/backups/gc")
               .SetQueryParam("dryRun", dryRun)
               .WithSettings(o => o.HttpVersion = "2.0")
               .PostJsonAsync(new { }, cancellationToken: cancellationToken)
               .ReceiveJson<KahunaBackupGcResult>())).ConfigureAwait(false);
        return response ?? throw new KahunaException("Backup GC returned null", LockResponseType.Errored);
    }

    /// <summary>
    /// Runs a backup REST call, reconstructing a typed <see cref="KahunaBackupException"/> from the
    /// <see cref="KahunaBackupWire.OutcomeHttpHeader"/> response header when the server rejected it.
    /// </summary>
    private static async Task<T> InvokeBackupRest<T>(Func<Task<T>> call)
    {
        try
        {
            return await call().ConfigureAwait(false);
        }
        catch (FlurlHttpException ex)
        {
            IFlurlResponse? resp = ex.Call?.Response;
            if (resp is not null &&
                resp.Headers.TryGetFirst(KahunaBackupWire.OutcomeHttpHeader, out string? name) &&
                Enum.TryParse(name, out KahunaBackupOutcome outcome))
                throw new KahunaBackupException(outcome, ex.Message);
            throw;
        }
    }
}
