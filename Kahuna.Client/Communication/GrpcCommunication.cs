
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Polly.Contrib.WaitAndRetry;

namespace Kahuna.Client.Communication;

/// <summary>
/// Provides an implementation of the IKahunaCommunication interface for gRPC-based communication.
/// This class offers methods to perform distributed locking and manage key-value storage in a gRPC context.
/// </summary>
public class GrpcCommunication : IKahunaCommunication
{
    // gRPC client stubs are thread-safe and bound to a channel, so one cached stub per channel
    // replaces a per-call allocation on every unary path. ConditionalWeakTable lets the entry die
    // with its channel when the shared pool invalidates and disposes it.
    private static readonly ConditionalWeakTable<GrpcChannel, KeyValuer.KeyValuerClient> keyValueClients = new();

    private static readonly ConditionalWeakTable<GrpcChannel, Sequencer.SequencerClient> sequencerClients = new();

    private static readonly ConditionalWeakTable<GrpcChannel, Cluster.ClusterClient> clusterClients = new();

    private static readonly ConditionalWeakTable<GrpcChannel, Backups.BackupsClient> backupsClients = new();

    private static KeyValuer.KeyValuerClient GetKeyValueClient(GrpcChannel channel) =>
        keyValueClients.GetValue(channel, static c => new(c));

    private static Sequencer.SequencerClient GetSequencerClient(GrpcChannel channel) =>
        sequencerClients.GetValue(channel, static c => new(c));

    private static Cluster.ClusterClient GetClusterClient(GrpcChannel channel) =>
        clusterClients.GetValue(channel, static c => new(c));

    private static Backups.BackupsClient GetBackupsClient(GrpcChannel channel) =>
        backupsClients.GetValue(channel, static c => new(c));

    /// <summary>
    /// Returns the response payload as a byte array without copying when the ByteString's backing
    /// array is fully owned by it (the normal case for a freshly parsed message); falls back to a
    /// copy for sliced/rope-backed values so callers never observe bytes outside the payload.
    /// </summary>
    private static byte[] GetResponseBytes(ByteString value)
    {
        if (MemoryMarshal.TryGetArray(value.Memory, out ArraySegment<byte> segment)
            && segment.Array is not null
            && segment.Offset == 0
            && segment.Count == segment.Array.Length)
            return segment.Array;

        return value.ToByteArray();
    }

    /// <summary>
    /// How many times a transaction-session call (start / commit / rollback) re-issues a request the server
    /// answered with MustRetry before handing the retryable outcome back to the application.
    /// </summary>
    private const int TransactionRetries = 5;

    /// <summary>
    /// Growing delay between MustRetry attempts on a transaction-session call. The server returns MustRetry for a
    /// transient condition — a leader flip, an in-doubt finalize a recovery sweep is still resolving — and none of
    /// those clear within the microseconds an immediate re-issue takes, so retrying without a delay burns the whole
    /// retry budget inside the same instant that produced the first MustRetry. The delay grows from ~1ms toward
    /// ~10ms and then holds, matching the lock path's backoff. Instantiated only once a MustRetry is actually
    /// observed, so the common first-attempt success allocates nothing.
    /// </summary>
    private sealed class MustRetryBackoff : IDisposable
    {
        private readonly IEnumerator<TimeSpan> sequence = Backoff
            .DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: 10)
            .GetEnumerator();

        private TimeSpan delay = TimeSpan.FromMilliseconds(1);

        public Task WaitAsync(CancellationToken cancellationToken)
        {
            // Past the end of the sequence the last (capped) delay is reused for every further attempt.
            if (sequence.MoveNext())
                delay = sequence.Current;

            return Task.Delay(delay, cancellationToken);
        }

        public void Dispose() => sequence.Dispose();
    }

    private readonly ConcurrentDictionary<string, Lazy<GrpcBatcher>> batchers = new();

    private readonly KahunaOptions? options;
    
    private readonly ILogger? logger;
    
    public GrpcCommunication(KahunaOptions? options, ILogger? logger)
    {
        this.options = options;
        this.logger = logger;
    }

    /// <summary>
    /// Attempts to acquire a lock on a specified resource using the provided settings.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the lock request will be executed.</param>
    /// <param name="resource">The name of the resource to lock.</param>
    /// <param name="owner">A unique identifier representing the owner of the lock.</param>
    /// <param name="expiryTime">The duration, in milliseconds, for which the lock will remain valid.</param>
    /// <param name="durability">Specifies the durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests while attempting to acquire the lock.</param>
    /// <returns>
    /// A tuple containing the result of the lock acquisition (<see cref="KahunaLockAcquireResult"/>),
    /// the remaining TTL (time-to-live) for the lock, and an optional string error message.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the lock acquisition process encounters an error or fails.</exception>
    public async Task<(KahunaLockAcquireResult, long, string?)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),
            ExpiresMs = expiryTime,
            Durability = (GrpcLockDurability)durability
        };
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        GrpcTryLockResponse? response = null;

        // MustRetry means the server has a transient condition (replication catch-up, leader
        // election) and wants the client to retry.  Elections can run 100s of ms to seconds, so
        // this loop is intentionally unbounded.  Termination is driven by the caller's
        // CancellationToken — pass one (or set KahunaOptions.DefaultOperationTimeout and use a
        // linked token) to bound it.  The CT3 default deadline does NOT bound this loop: it is
        // applied per Enqueue inside the batcher, so it only aborts a single unresponsive call,
        // not the overall retry loop when the server keeps returning MustRetry quickly.  The
        // backoff grows from ~1ms toward ~10ms over the first 10 retries, then stays capped at the
        // last value, so a stuck server is not busy-polled. The backoff sequence is only allocated
        // on the first MustRetry, keeping the common first-attempt success allocation-free.
        IEnumerator<TimeSpan>? mustRetryBackoff = null;
        TimeSpan mustRetryDelay = TimeSpan.FromMilliseconds(1);

        try
        {
            while (true)
            {
                GrpcBatcherResponse batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);

                response = batchResponse.TryLock;

                if (response is null)
                    throw new KahunaException("Response is null", LockResponseType.Errored);

                if (response.Type == GrpcLockResponseType.LockResponseTypeLocked)
                    return (KahunaLockAcquireResult.Success, response.FencingToken, response.ServedFrom);

                if (response.Type == GrpcLockResponseType.LockResponseTypeBusy)
                    return (KahunaLockAcquireResult.Conflicted, -1, null);

                if (response.Type != GrpcLockResponseType.LockResponseTypeMustRetry)
                    throw new KahunaException("Failed to lock", (LockResponseType)response.Type);

                mustRetryBackoff ??= Backoff
                    .DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: 10)
                    .GetEnumerator();

                if (mustRetryBackoff.MoveNext())
                    mustRetryDelay = mustRetryBackoff.Current;
                // else: keep the last (capped) delay for all subsequent retries

                await Task.Delay(mustRetryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            mustRetryBackoff?.Dispose();
        }
    }

    /// <summary>
    /// Attempts to release a lock on a specified resource with the given settings.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to interact with.</param>
    /// <param name="resource">The name of the resource for which the lock release is requested.</param>
    /// <param name="owner">The identifier for the owner of the lock.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A boolean indicating whether the lock was successfully released.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails or encounters an unrecoverable error.</exception>
    public async Task<bool> TryUnlock(string url, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcUnlockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),
            Durability = (GrpcLockDurability)durability
        };
        
        int retries = 0;
        GrpcUnlockResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            GrpcBatcherResponse batchResponse;
                              
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.Unlock;

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeUnlocked)
                return true;
            
            if (response.Type is GrpcLockResponseType.LockResponseTypeInvalidOwner or GrpcLockResponseType.LockResponseTypeLockDoesNotExist)
                return false;
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Aborted);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to unlock: " + response.Type, (LockResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to extend the lock on a specified resource with updated expiry and durability settings.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to interact with.</param>
    /// <param name="resource">The name of the resource for which the lock extension is requested.</param>
    /// <param name="owner">The identifier for the owner of the lock.</param>
    /// <param name="expiryTime">The new expiry time in milliseconds to set for the lock extension.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple containing a boolean indicating whether the lock was successfully extended and the updated lock expiry timestamp.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails or encounters an unrecoverable error.</exception>
    public async Task<(bool, long)> TryExtendLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcExtendLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),
            ExpiresMs = expiryTime,
            Durability = (GrpcLockDurability)durability
        };
        
        int retries = 0;
        GrpcExtendLockResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            GrpcBatcherResponse batchResponse;
                              
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.ExtendLock;

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeExtended)
                return (true, response.FencingToken);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Aborted);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to extend", (LockResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to get information about a lock for a specified resource using the given parameters.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to communicate with.</param>
    /// <param name="resource">The name of the resource to be locked.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// An object of type <see cref="KahunaLockInfo"/> if the lock is successfully acquired, otherwise null.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails after retries or encounters an error.</exception>
    public async Task<KahunaLockInfo?> GetLock(string url, string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcGetLockRequest request = new()
        {
            Resource = resource,
            Durability = (GrpcLockDurability)durability
        };
        
        int retries = 0;
        GrpcGetLockResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
        
            //response = await client.GetLockAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
            
            GrpcBatcherResponse batchResponse;
                              
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.GetLock;

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeGot)
                return new(response.Owner?.ToByteArray(), new(response.ExpiresNode, response.ExpiresPhysical, response.ExpiresCounter), response.FencingToken);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Aborted);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to get lock information", (LockResponseType)response.Type);
    }

    /// <summary>
    /// Tries to set a key-value pair in a distributed key-value store with specified parameters.
    /// </summary>
    /// <param name="url">The address of the server or service endpoint.</param>
    /// <param name="transactionId">The transaction identifier used to associate the operation with a logical timeline.</param>
    /// <param name="key">The key to be set or updated in the key-value store.</param>
    /// <param name="value">The value to be associated with the key. Can be null if the intention is to set an empty value.</param>
    /// <param name="expiryTime">The expiration time in milliseconds for the key-value pair.</param>
    /// <param name="flags">The flags indicating the conditions or modes of the set operation.</param>
    /// <param name="durability">The durability guarantee of the operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple where:
    /// - The first item represents whether the operation succeeded.
    /// - The second item represents the new revision number of the key.
    /// - The third item represents the time taken for the operation in milliseconds.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails after retries.</exception>
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
        GrpcTrySetKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
            Flags = (GrpcKeyValueFlags)flags,
            ExpiresMs = expiryTime,
            Durability = (GrpcKeyValueDurability)durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        int retries = 0;
        GrpcTrySetKeyValueResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);

        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

            GrpcBatcherResponse batchResponse;
                              
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TrySetKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeSet)
                return (true, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeNotset)
                return (false, response.Revision, response.TimeElapsedMs);

            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);

        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to set key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    public async Task<(List<KahunaSetKeyValueResponseItem>, int)> TrySetManyKeyValues(
        string url, 
        IEnumerable<KahunaSetKeyValueRequestItem> requestItems, 
        CancellationToken cancellationToken
    )
    {
        GrpcTrySetManyKeyValueRequest request = new();
        
        AddSetManyKeyValueRequestItems(request.Items, requestItems);
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        if (cancellationToken.IsCancellationRequested)
            throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

        GrpcBatcherResponse batchResponse;
                              
        batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
        GrpcTrySetManyKeyValueResponse? response = batchResponse.TrySetManyKeyValues;

        if (response is null)
            throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
        return (GetSetManyKeyValueResponseItems(response.Items), response.TimeElapsedMs);
    }

    public async Task<(List<KahunaDeleteKeyValueResponseItem>, int)> TryDeleteManyKeyValues(
        string url,
        IEnumerable<KahunaDeleteKeyValueRequestItem> requestItems,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        GrpcTryDeleteManyKeyValueRequest request = new();

        AddDeleteManyKeyValueRequestItems(request.Items, requestItems);

        // The whole batch registers as one coordinator operation so its confirmed persistent keys anchor
        // the transaction record deterministically. Absent for the non-transactional batch path.
        if (!string.IsNullOrEmpty(coordinatorKey) && !operationId.IsEmpty)
        {
            request.CoordinatorKey = coordinatorKey;
            request.OperationIdHigh = operationId.High;
            request.OperationIdLow = operationId.Low;
        }

        GrpcBatcher batcher = GetSharedBatcher(url);

        if (cancellationToken.IsCancellationRequested)
            throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

        GrpcBatcherResponse batchResponse;

        batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);

        GrpcTryDeleteManyKeyValueResponse? response = batchResponse.TryDeleteManyKeyValues;

        if (response is null)
            throw new KahunaException("Response is null", KeyValueResponseType.Errored);

        return (GetDeleteManyKeyValueResponseItems(response.Items), response.TimeElapsedMs);
    }

    public async Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryGetManyKeyValues(
        string url,
        HLCTimestamp transactionId,
        IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems,
        CancellationToken cancellationToken
    )
    {
        // Intentionally unary: GetMany is already a bulk-key RPC; the streaming batcher's value
        // is coalescing individual single-key calls, not bulk requests that carry N keys internally.
        // GrpcBatchClientKeyValueResponse does not yet include GetMany/ExistsMany payload fields,
        // so routing through the batcher would require proto + server-handler changes. The shared
        // channel pool provides HTTP/2 multiplexing without that overhead.
        GrpcTryGetManyValuesRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
        AddManyKeyValuesRequestItems(request.Items, requestItems);

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        GrpcTryGetManyValuesResponse response = await client.TryGetManyValuesAsync(
            request, cancellationToken: cancellationToken
        ).ConfigureAwait(false);

        return (GetGetManyKeyValuesResponseItems(response.Items), 0);
    }

    public async Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryExistsManyKeyValues(
        string url,
        HLCTimestamp transactionId,
        IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems,
        CancellationToken cancellationToken
    )
    {
        // Intentionally unary — same rationale as TryGetManyKeyValues above.
        GrpcTryExistsManyValuesRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
        AddManyKeyValuesRequestItems(request.Items, requestItems);

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        GrpcTryExistsManyValuesResponse response = await client.TryExistsManyValuesAsync(
            request, cancellationToken: cancellationToken
        ).ConfigureAwait(false);

        return (GetExistsManyKeyValuesResponseItems(response.Items), 0);
    }

    private static void AddManyKeyValuesRequestItems(
        RepeatedField<GrpcTryManyValuesRequestItem> target,
        IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems)
    {
        foreach (KahunaGetManyKeyValuesRequestItem item in requestItems)
        {
            target.Add(new GrpcTryManyValuesRequestItem
            {
                Key = item.Key ?? "",
                Revision = item.Revision,
                Durability = (GrpcKeyValueDurability)item.Durability
            });
        }
    }

    private static List<KahunaGetManyKeyValuesResponseItem> GetGetManyKeyValuesResponseItems(
        RepeatedField<GrpcTryGetManyValuesResponseItem> items)
    {
        List<KahunaGetManyKeyValuesResponseItem> result = new(items.Count);
        foreach (GrpcTryGetManyValuesResponseItem item in items)
        {
            result.Add(new()
            {
                Key = item.Key,
                Type = (KeyValueResponseType)item.Type,
                Value = item.HasValue ? GetResponseBytes(item.Value) : null,
                Revision = item.Revision,
                LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                Durability = (KeyValueDurability)item.Durability
            });
        }
        return result;
    }

    private static List<KahunaGetManyKeyValuesResponseItem> GetExistsManyKeyValuesResponseItems(
        RepeatedField<GrpcTryExistsManyValuesResponseItem> items)
    {
        List<KahunaGetManyKeyValuesResponseItem> result = new(items.Count);
        foreach (GrpcTryExistsManyValuesResponseItem item in items)
        {
            result.Add(new()
            {
                Key = item.Key,
                Type = (KeyValueResponseType)item.Type,
                Revision = item.Revision,
                LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                Durability = (KeyValueDurability)item.Durability
            });
        }
        return result;
    }

    private static void AddSetManyKeyValueRequestItems(
        RepeatedField<GrpcTrySetManyKeyValueRequestItem> target,
        IEnumerable<KahunaSetKeyValueRequestItem> requestItems)
    {
        foreach (KahunaSetKeyValueRequestItem item in requestItems)
        {
            target.Add(new GrpcTrySetManyKeyValueRequestItem
            {
                Key = item.Key,
                Value = item.Value is not null ? UnsafeByteOperations.UnsafeWrap(item.Value) : null,
                ExpiresMs = item.ExpiresMs,
                Flags = (GrpcKeyValueFlags)item.Flags,
                Durability = (GrpcKeyValueDurability)item.Durability
            });
        }
    }
    
    private static List<KahunaSetKeyValueResponseItem> GetSetManyKeyValueResponseItems(RepeatedField<GrpcTrySetManyKeyValueResponseItem> grpcReponseItems)
    {                
        List<KahunaSetKeyValueResponseItem> responseItems = new(grpcReponseItems.Count);
        
        foreach (GrpcTrySetManyKeyValueResponseItem? item in grpcReponseItems)
        {
            responseItems.Add(new()
            {
                Key = item.Key,
                Revision = item.Revision,
                LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                Durability = (KeyValueDurability)item.Durability
            });
        }

        return responseItems;
    }

    private static void AddDeleteManyKeyValueRequestItems(
        RepeatedField<GrpcTryDeleteManyKeyValueRequestItem> target,
        IEnumerable<KahunaDeleteKeyValueRequestItem> requestItems)
    {
        foreach (KahunaDeleteKeyValueRequestItem item in requestItems)
        {
            target.Add(new GrpcTryDeleteManyKeyValueRequestItem
            {
                TransactionIdNode = item.TransactionId.N,
                TransactionIdPhysical = item.TransactionId.L,
                TransactionIdCounter = item.TransactionId.C,
                Key = item.Key,
                Durability = (GrpcKeyValueDurability)item.Durability
            });
        }
    }

    private static List<KahunaDeleteKeyValueResponseItem> GetDeleteManyKeyValueResponseItems(RepeatedField<GrpcTryDeleteManyKeyValueResponseItem> grpcResponseItems)
    {
        List<KahunaDeleteKeyValueResponseItem> responseItems = new(grpcResponseItems.Count);

        foreach (GrpcTryDeleteManyKeyValueResponseItem? item in grpcResponseItems)
        {
            responseItems.Add(new()
            {
                Key = item.Key,
                Type = (KeyValueResponseType)item.Type,
                Revision = item.Revision,
                LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                Durability = (KeyValueDurability)item.Durability
            });
        }

        return responseItems;
    }

    /// <summary>
    /// Attempts to compare the current value associated with a key in a distributed key-value store and set it to a new value if the comparison matches.
    /// </summary>
    /// <param name="url">The address of the server or service endpoint.</param>
    /// <param name="transactionId">The transaction identifier used to associate the operation with a logical timeline.</param> 
    /// <param name="key">The key whose value needs to be compared and possibly updated.</param>
    /// <param name="value">The new value to set if the current value matches the compare value. Can be null to represent an empty value.</param>
    /// <param name="compareValue">The value to compare against the current value. If the current value matches this, the update is performed. Can be null to represent an empty comparison value.</param>
    /// <param name="expiryTime">The expiration time in milliseconds for the key-value pair after it is updated.</param>
    /// <param name="durability">The durability guarantee of the operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple where:
    /// - The first item indicates whether the operation succeeded.
    /// - The second item represents the new revision number of the key.
    /// - The third item represents the time taken for the operation in milliseconds.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails due to retries being exhausted, cancellation, or other errors.</exception>
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
        GrpcTrySetKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
            CompareValue = compareValue is not null ? UnsafeByteOperations.UnsafeWrap(compareValue) : null,
            Flags = GrpcKeyValueFlags.SetIfEqualToValue,
            ExpiresMs = expiryTime,
            Durability = (GrpcKeyValueDurability)durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        int retries = 0;
        GrpcTrySetKeyValueResponse? response;
                
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse;
                              
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TrySetKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeSet)
                return (true, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeNotset)
                return (false, response.Revision, response.TimeElapsedMs);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);

        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to set key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to update a key-value pair in a distributed key-value store only if the current revision number matches a specified value.
    /// </summary>
    /// <param name="url">The address of the server or service endpoint.</param>
    /// <param name="transactionId">The transaction identifier used to associate the operation with a logical timeline.</param> 
    /// <param name="key">The key to be updated in the key-value store.</param>
    /// <param name="value">The new value to associate with the key. Can be null to set an empty value.</param>
    /// <param name="compareRevision">The expected current revision number of the key. The update will only occur if this value matches the actual revision number.</param>
    /// <param name="expiryTime">The expiration time in milliseconds for the key-value pair.</param>
    /// <param name="durability">The durability guarantee of the operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A tuple where:
    /// - The first item indicates whether the operation succeeded.
    /// - The second item provides the new revision number of the key.
    /// - The third item represents the time taken for the operation in milliseconds.</returns>
    /// <exception cref="KahunaException">Thrown if the operation fails or retries are exhausted.</exception>
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
        GrpcTrySetKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
            CompareRevision = compareRevision,
            Flags = GrpcKeyValueFlags.SetIfEqualToRevision,
            ExpiresMs = expiryTime,
            Durability = (GrpcKeyValueDurability)durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        int retries = 0;
        GrpcTrySetKeyValueResponse? response;
                
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
            
            GrpcBatcherResponse batchResponse;
                              
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TrySetKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeSet)
                return (true, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeNotset)
                return (false, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry set key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);

        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to set key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Tries to retrieve a key-value pair from a distributed key-value store with the specified parameters.
    /// </summary>
    /// <param name="url">The address of the server or service endpoint.</param>
    /// <param name="transactionId">The transaction identifier used to associate the operation with a logical timeline.</param>
    /// <param name="key">The key to be retrieved from the key-value store.</param>
    /// <param name="revision">The specific revision number of the key to retrieve. Use -1 to retrieve the latest revision.</param>
    /// <param name="durability">The durability guarantee of the operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple where:
    /// - The first item indicates whether the operation succeeded.
    /// - The second item is the value associated with the key, if found, represented as a byte array. Null if the key is not found.
    /// - The third item is the last revision number of the key retrieved.
    /// - The fourth item represents the time taken for the operation in milliseconds.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails after retries.</exception>
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
        GrpcTryGetKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Revision = revision,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
            Durability = (GrpcKeyValueDurability)durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        for (int unavailableRetries = 0; unavailableRetries < 2; unavailableRetries++)
        {
            int retries = 0;
            GrpcTryGetKeyValueResponse? response;
               
            GrpcBatcher batcher = GetSharedBatcher(url);
            
            try
            {
                do
                {
                    if (cancellationToken.IsCancellationRequested)
                        throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
                
                    GrpcBatcherResponse batchResponse;
                        
                    batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
                    response = batchResponse.TryGetKeyValue;

                    if (response is null)
                        throw new KahunaException("Response is null", KeyValueResponseType.Errored);

                    switch (response.Type)
                    {
                        case GrpcKeyValueResponseType.TypeGot:
                        {
                            byte[] value = GetResponseBytes(response.Value);

                            HLCTimestamp lastModified = new(response.LastModifiedNode, response.LastModifiedPhysical, response.LastModifiedCounter);
                            return (true, value, response.Revision, lastModified, response.TimeElapsedMs);
                        }

                        case GrpcKeyValueResponseType.TypeDoesNotExist:
                            return (false, null, 0, HLCTimestamp.Zero, response.TimeElapsedMs);
                    }
            
                    if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                        logger?.LogDebug("Server asked to retry get key/value");
            
                    if (++retries >= 5)
                        throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
                } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
                    
                throw new KahunaException("Failed to get key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
            }
            catch (RpcException ex) when (RetryableTransportFailure.IsRetryable(ex) && !cancellationToken.IsCancellationRequested && unavailableRetries == 0)
            {
                logger?.LogDebug(ex, "Retrying get key/value after gRPC stream became unavailable");
                await Task.Delay(25, cancellationToken).ConfigureAwait(false);
            }
        }

        throw new KahunaException("gRPC stream unavailable", KeyValueResponseType.Errored);
    }

    /// <summary>
    /// Attempts to check the existence of a key-value pair in the storage system with a specific revision, durability, and transaction ID.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to interact with.</param>
    /// <param name="transactionId">The ID of the transaction under which the key-value existence check is performed.</param>
    /// <param name="key">The key for the key-value pair to check.</param>
    /// <param name="revision">The specific revision of the key-value pair to verify existence.</param>
    /// <param name="durability">The durability level of the key-value pair (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests during the operation.</param>
    /// <returns>A tuple containing a boolean indicating whether the key-value pair exists, the current revision timestamp, and the response type as an integer.</returns>
    /// <exception cref="KahunaException">Thrown if the operation fails or encounters a retryable or non-recoverable error from the server.</exception>
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
        GrpcTryExistsKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Revision = revision,
            Durability = (GrpcKeyValueDurability)durability,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        int retries = 0;
        GrpcTryExistsKeyValueResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
        
            GrpcBatcherResponse batchResponse;
                
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TryExistsKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.TypeExists:
                    return (true, response.Revision, response.TimeElapsedMs);
                
                case GrpcKeyValueResponseType.TypeDoesNotExist:
                    return (false, 0, response.TimeElapsedMs);
            }
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry exists key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to check if exists key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to delete a key-value pair from the storage with the specified transaction ID and durability settings.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to interact with.</param>
    /// <param name="transactionId">The hybrid logical clock timestamp used to track the transaction.</param>
    /// <param name="key">The key of the key-value pair to be deleted.</param>
    /// <param name="durability">The durability type of the key-value pair (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple containing a boolean indicating if the deletion was successful, a long representing the logical timestamp of the operation, and an integer indicating the number of retries performed.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the deletion operation fails or encounters an unrecoverable error.</exception>
    public async Task<(bool, long, int)> TryDeleteKeyValue(string url, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        GrpcTryDeleteKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };
        
        int retries = 0;
        GrpcTryDeleteKeyValueResponse? response;               
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse;
                
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TryDeleteKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.TypeDeleted:
                    return (true, response.Revision, response.TimeElapsedMs);
                
                case GrpcKeyValueResponseType.TypeDoesNotExist:
                    return (false, response.Revision, response.TimeElapsedMs);
            }
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry delete key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to delete key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to extend the expiry of a key in a key-value store with updated settings for expiry and durability.
    /// </summary>
    /// <param name="url">The endpoint URL of the server that manages the key-value store.</param>
    /// <param name="transactionId">The transaction ID associated with the operation.</param>
    /// <param name="key">The key in the key-value store whose expiry should be extended.</param>
    /// <param name="expiresMs">The new expiry duration in milliseconds to set for the key.</param>
    /// <param name="durability">The durability option for the key-value operation, indicating how it should be persisted (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests during the operation.</param>
    /// <returns>
    /// A tuple containing a boolean that indicates whether the operation was successful,
    /// the updated expiry timestamp in ticks, and the time taken for the operation in milliseconds.
    /// </returns>
    /// <exception cref="KahunaException">Thrown when the operation fails or encounters an unrecoverable error.</exception>
    public async Task<(bool, long, int)> TryExtendKeyValue(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        GrpcTryExtendKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability)durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        int retries = 0;
        GrpcTryExtendKeyValueResponse? response;               
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
        
            GrpcBatcherResponse batchResponse;
                
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TryExtendKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.TypeExtended:
                    return (true, response.Revision, response.TimeElapsedMs);
                
                case GrpcKeyValueResponseType.TypeDoesNotExist:
                    return (false, 0, response.TimeElapsedMs);
            }
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry extend key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to extend key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to execute a key-value transaction script on a specified server with optional parameters and hash validation.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to execute the transaction script on.</param>
    /// <param name="script">The byte array containing the transaction script to be executed.</param>
    /// <param name="hash">An optional hash string used for script validation, ensuring integrity.</param>
    /// <param name="parameters">An optional list of key-value parameters to be passed to the transaction script.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// An instance of <see cref="KahunaKeyValueTransactionResult"/> representing the outcome of the transaction execution.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails, the transaction is aborted, or an unrecoverable error is encountered.</exception>
    public async Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransactionScript(string url, byte[] script, string? hash, List<KeyValueParameter>? parameters, CancellationToken cancellationToken, TransactionPriority priority = TransactionPriority.Normal)
    {
        GrpcTryExecuteTransactionScriptRequest request = new()
        {
            Script = UnsafeByteOperations.UnsafeWrap(script),
            Priority = TransactionPriorityWire.ToGrpc(priority)
        };
        
        if (hash is not null)
            request.Hash = hash;
        
        if (parameters is not null)
            AddTransactionParameters(request.Parameters, parameters);

        int retries = 0;
        GrpcTryExecuteTransactionScriptResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
            
            GrpcBatcherResponse batchResponse;
                
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TryExecuteTransactionScript;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type is < GrpcKeyValueResponseType.TypeErrored or GrpcKeyValueResponseType.TypeDoesNotExist)
                return new()
                {
                    Type = (KeyValueResponseType)response.Type,
                    Values = GetTransactionValues(response.Values),
                    TimeElapsedMs = response.TimeElapsedMs
                };
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry transaction");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);

        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
        
        if (!string.IsNullOrEmpty(response.Reason))
            throw new KahunaException(response.Reason, (KeyValueResponseType)response.Type);

        if (response.Type == GrpcKeyValueResponseType.TypeAborted)
            throw new KahunaException("Transaction aborted", (KeyValueResponseType)response.Type);

        throw new KahunaException("Failed to execute key/value transaction: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to acquire an exclusive key-value lock using the provided parameters.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the lock request will be executed.</param>
    /// <param name="transactionId">The high-level consistent timestamp associated with the ongoing transaction.</param>
    /// <param name="key">The key representing the resource to lock.</param>
    /// <param name="durability">The durability type of the lock, indicating whether it is ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests while attempting to acquire the lock.</param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result is a boolean indicating whether the lock was successfully acquired.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the lock acquisition process fails or encounters an error.</exception>
    public async Task<bool> TryAcquireExclusiveKeyValueLock(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        GrpcTryAcquireExclusiveLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability)durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        int retries = 0;
        GrpcTryAcquireExclusiveLockResponse? response;               
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
        
            GrpcBatcherResponse batchResponse;
                
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TryAcquireExclusiveLock;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeLocked)            
                return true;
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry acquire key/value lock");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to acquire key/value lock: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    public async Task<bool> TryAcquireExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        // Intentionally unary: prefix lock acquisition is a low-frequency control-plane op that
        // drives its own retry loop; routing through the streaming batcher adds no throughput benefit.
        GrpcTryAcquireExclusivePrefixLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            PrefixKey = prefixKey,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability)durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        for (int retries = 0; retries < 5; retries++)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

            GrpcTryAcquireExclusivePrefixLockResponse response = await client.TryAcquireExclusivePrefixLockAsync(
                request, cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            if (response.Type == GrpcKeyValueResponseType.TypeLocked)
                return true;

            if (response.Type is GrpcKeyValueResponseType.TypeAlreadyLocked)
                throw new KahunaException($"Failed to acquire exclusive prefix lock for '{prefixKey}': AlreadyLocked.", KeyValueResponseType.Aborted);

            if (response.Type != GrpcKeyValueResponseType.TypeMustRetry)
                throw new KahunaException($"Failed to acquire exclusive prefix lock for '{prefixKey}'.", KeyValueResponseType.Aborted);

            logger?.LogDebug("Server asked to retry acquire prefix key/value lock");
        }

        throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
    }

    public async Task TryReleaseExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        // Intentionally unary: low-frequency control-plane release; no coalescing value.
        GrpcTryReleaseExclusivePrefixLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            PrefixKey = prefixKey,
            Durability = (GrpcKeyValueDurability)durability
        };

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        await client.TryReleaseExclusivePrefixLockAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryAcquireRangeKeyValueLock(
        string url,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        // Intentionally unary: range lock acquisition is a low-frequency control-plane op with its
        // own retry loop; batching adds no throughput benefit here.
        GrpcTryAcquireExclusiveRangeLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Prefix = prefix,
            StartInclusive = startInclusive,
            EndInclusive = endInclusive,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability)durability,
            Mode = (GrpcRangeLockMode)mode,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        if (startKey is not null) request.StartKey = startKey;
        if (endKey is not null)   request.EndKey   = endKey;

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        for (int retries = 0; retries < 5; retries++)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

            GrpcTryAcquireExclusiveRangeLockResponse response = await client.TryAcquireExclusiveRangeLockAsync(
                request, cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            if (response.Type == GrpcKeyValueResponseType.TypeLocked)
                return true;

            if (response.Type == GrpcKeyValueResponseType.TypeAlreadyLocked)
                throw new KahunaException($"Failed to acquire exclusive range lock for '{prefix}': AlreadyLocked.", KeyValueResponseType.Aborted);

            if (response.Type != GrpcKeyValueResponseType.TypeMustRetry)
                throw new KahunaException($"Failed to acquire exclusive range lock for '{prefix}'.", KeyValueResponseType.Aborted);

            logger?.LogDebug("Server asked to retry acquire range key/value lock");
        }

        throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
    }

    public async Task TryReleaseExclusiveRangeKeyValueLock(
        string url,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        // Intentionally unary: low-frequency control-plane release; no coalescing value.
        GrpcTryReleaseExclusiveRangeLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Prefix = prefix,
            StartInclusive = startInclusive,
            EndInclusive = endInclusive,
            Durability = (GrpcKeyValueDurability)durability
        };

        if (startKey is not null) request.StartKey = startKey;
        if (endKey is not null)   request.EndKey   = endKey;

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        await client.TryReleaseExclusiveRangeLockAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<KeyValueGetByRangePageResult> GetByRange(
        string url,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        // Intentionally unary: range scan returns a paginated result set; it carries its own
        // retry logic and is not a candidate for per-key coalescing in the streaming batcher.
        GrpcGetByRangeRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Prefix = prefix,
            StartInclusive = startInclusive,
            EndInclusive = endInclusive,
            Limit = limit,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
            Durability = (GrpcKeyValueDurability)durability,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        if (startKey is not null) request.StartKey = startKey;
        if (endKey is not null)   request.EndKey   = endKey;

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        for (int retries = 0; retries < 5; retries++)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

            GrpcGetByRangeResponse response = await client.GetByRangeAsync(
                request, cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            if (response.Type == GrpcKeyValueResponseType.TypeGot || response.Type == GrpcKeyValueResponseType.TypeDoesNotExist)
            {
                return new()
                {
                    Items = GetByPrefixResponseItems(response.Items),
                    NextCursor = response.HasNextCursor ? response.NextCursor : null,
                    HasMore = response.HasMore
                };
            }

            if (response.Type != GrpcKeyValueResponseType.TypeMustRetry)
                throw new KahunaException($"Failed to get by range for '{prefix}'.", KeyValueResponseType.Errored);

            logger?.LogDebug("Server asked to retry get by range");

            // MustRetry is a transient answer (leadership transition, pending settlement); retrying
            // without a delay burns every attempt within the same transient window.
            await WaitBeforeMustRetry(retries, cancellationToken).ConfigureAwait(false);
        }

        throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
    }

    // Intentionally unary (server-streaming): range scan is a streaming server-push call, not a
    // per-key operation; the batcher's per-item coalescing model does not apply here.
    public async IAsyncEnumerable<KeyValueGetByBucketItem> ScanByRange(
        string url,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        int pageSize,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        GrpcGetByRangeRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Prefix = prefix,
            StartInclusive = startInclusive,
            EndInclusive = endInclusive,
            Limit = pageSize,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
            Durability = (GrpcKeyValueDurability)durability
        };

        if (startKey is not null) request.StartKey = startKey;
        if (endKey is not null)   request.EndKey   = endKey;

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        using Grpc.Core.AsyncServerStreamingCall<GrpcGetByRangePageResponse> stream =
            client.GetByRangeStream(request, cancellationToken: cancellationToken);

        await foreach (GrpcGetByRangePageResponse page in stream.ResponseStream.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (GrpcKeyValueByPrefixItemResponse item in page.Items)
            {
                yield return new KeyValueGetByBucketItem
                {
                    Key = item.Key,
                    Value = item.Value.IsEmpty ? null : GetResponseBytes(item.Value),
                    Revision = item.Revision,
                    LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter)
                };
            }
        }
    }

    private static List<KeyValueGetByBucketItem> GetByPrefixResponseItems(RepeatedField<GrpcKeyValueByPrefixItemResponse> grpcResponseItems)
    {
        List<KeyValueGetByBucketItem> items = new(grpcResponseItems.Count);

        foreach (GrpcKeyValueByPrefixItemResponse x in grpcResponseItems)
        {
            items.Add(new()
            {
                Key = x.Key,
                Value = GetResponseBytes(x.Value),
                Revision = x.Revision,
                LastModified = new(x.LastModifiedNode, x.LastModifiedPhysical, x.LastModifiedCounter)
            });
        }

        return items;
    }

    private static List<KahunaKeyValueTransactionResultValue> GetTransactionValues(RepeatedField<GrpcTryExecuteTransactionResponseValue> responseValues)
    {
        List<KahunaKeyValueTransactionResultValue> values = new(responseValues.Count);
        
        foreach (GrpcTryExecuteTransactionResponseValue response in responseValues)
        {
            KahunaKeyValueTransactionResultValue responseValue = new()
            {
                Key = response.Key,
                Value = GetResponseBytes(response.Value),
                Revision = response.Revision,
                Expires = new(response.ExpiresNode, response.ExpiresPhysical, response.ExpiresCounter),
                LastModified = new(response.LastModifiedNode, response.LastModifiedPhysical, response.LastModifiedCounter)
            };
            
            values.Add(responseValue);
        }
        
        return values;
    }

    /// <summary>
    /// Retrieves a list of keys that match the specified prefix from the key-value store.
    /// </summary>
    /// <param name="url">The endpoint URL of the key-value store server.</param>
    /// <param name="prefixKey">The prefix used to filter keys stored in the system.</param>
    /// <param name="durability">Specifies the durability type (e.g., ephemeral or persistent) for the request.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests during the execution of the operation.</param>
    /// <returns>
    /// A tuple where the first element is a boolean indicating the success of the operation,
    /// and the second element is a list of string keys matching the specified prefix.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown if the operation fails or encounters an error while attempting to retrieve keys by prefix.
    /// </exception>
    public async Task<List<KeyValueGetByBucketItem>> GetByBucket(string url, HLCTimestamp transactionId, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        GrpcGetByBucketRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            PrefixKey = prefixKey,
            Durability = (GrpcKeyValueDurability)durability,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
            CoordinatorKey = coordinatorKey,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low
        };

        int retries = 0;
        GrpcGetByBucketResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
        
            GrpcBatcherResponse batchResponse;
                
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.GetByBucket;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
            if (response.Type == GrpcKeyValueResponseType.TypeGot)
                return GetByPrefixResponseItems(response.Items);

            if (response.Type == GrpcKeyValueResponseType.TypeDoesNotExist)
                return [];

            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry get key/value by prefix");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to get key/value by prefix: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Scans and retrieves all key/value pairs that match a specified prefix from the target server.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to query.</param>
    /// <param name="prefixKey">The prefix used to filter the keys for the scan operation.</param>
    /// <param name="durability">Specifies the durability type for the keys being scanned (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests while scanning.</param>
    /// <returns>
    /// A tuple containing a boolean indicating success or failure, and a list of keys that match the specified prefix.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the scan operation encounters an error or fails to complete successfully.</exception>
    public async Task<List<KeyValueGetByBucketItem>> ScanAllByPrefix(string url, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcScanAllByPrefixRequest request = new()
        {
            PrefixKey = prefixKey,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
            Durability = (GrpcKeyValueDurability)durability
        };

        int retries = 0;
        GrpcScanAllByPrefixResponse? response;               
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse;
                
            batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.ScanByPrefix;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
            if (response.Type == GrpcKeyValueResponseType.TypeGot)
                return GetByPrefixResponseItems(response.Items);

            if (response.Type == GrpcKeyValueResponseType.TypeDoesNotExist)
                return [];

            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry scan key/value by prefix");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to scan key/value by prefix: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Initiates a new transactional session for key-value operations using the specified parameters.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the transaction session will be started.</param>
    /// <param name="uniqueId">A unique identifier for the transaction session.</param>
    /// <param name="txOptions">Configuration options for the transaction, such as timeout and locking type.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests while attempting to start the transaction session.</param>
    /// <returns>
    /// A tuple containing the session identifier as a string and the Hybrid Logical Clock (HLC) timestamp for the transaction.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown if the transaction session initiation fails or encounters an error.
    /// </exception>
    public async Task<(string, HLCTimestamp transactionId)> StartTransactionSession(string url, string uniqueId, KahunaTransactionOptions txOptions, CancellationToken cancellationToken)
    {
        GrpcStartTransactionRequest request = new()
        {
            CoordinatorKey = uniqueId,
            Timeout = txOptions.Timeout,
            LockingType = (GrpcLockingType)txOptions.Locking,
            AsyncRelease = txOptions.AsyncRelease,
            AutoCommit = txOptions.AutoCommit,
            ReadValidation = (GrpcReadValidation)txOptions.ReadValidation,
            DecisionDurability = (GrpcDecisionDurability)txOptions.DecisionDurability,
            Priority = TransactionPriorityWire.ToGrpc(txOptions.Priority),
            ReadTimestampNode = txOptions.ReadTimestamp.N,
            ReadTimestampPhysical = txOptions.ReadTimestamp.L,
            ReadTimestampCounter = txOptions.ReadTimestamp.C,
            AdmissionWaitMs = txOptions.AdmissionWaitMs
        };

        int retries = 0;

        GrpcBatcher batcher = GetSharedBatcher(url);
        GrpcStartTransactionResponse? response;
        MustRetryBackoff? backoff = null;

        try
        {
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

                GrpcBatcherResponse batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);

                response = batchResponse.StartTransaction;

                if (response is null)
                    throw new KahunaException("Response is null", KeyValueResponseType.Errored);

                if (response.Type == GrpcKeyValueResponseType.TypeSet)
                    return new(url, new(response.TransactionIdNode, response.TransactionIdPhysical, response.TransactionIdCounter));

                if (response.Type != GrpcKeyValueResponseType.TypeMustRetry)
                    throw new KahunaException("Failed to start key/value transaction: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);

                logger?.LogDebug("Server asked to retry start key/value transaction");

                // Out of attempts: the condition is still transient, so report it as such. Reporting Errored here
                // would tell the caller the request was malformed and must not be retried, when the truth is the
                // opposite — the same transaction can still be started.
                if (++retries >= TransactionRetries)
                    throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

                backoff ??= new();
                await backoff.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            backoff?.Dispose();
        }
    }

    /// <summary>
    /// Commits a transaction session to the server for the specified unique identifier and transaction ID.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the transaction will be committed.</param>
    /// <param name="uniqueId">A unique identifier for the session or request being committed.</param>
    /// <param name="transactionId">The hybrid logical clock timestamp representing the transaction to be committed.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests during the transaction commit operation.</param>
    /// <returns>
    /// A tuple whose <c>committed</c> flag indicates whether the transaction was successfully committed, and whose
    /// <c>recordAnchorKey</c> carries the canonical record anchor (the first persistent modified key) when present.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown if the transaction commit process encounters an error, fails, or exceeds retry limits.
    /// </exception>
    public async Task<(bool committed, string? recordAnchorKey)> CommitTransactionSession(
        string url,
        string uniqueId,
        HLCTimestamp transactionId,
        string? recordAnchorKey,
        CancellationToken cancellationToken
    )
    {
        GrpcCommitTransactionRequest request = new()
        {
            CoordinatorKey = uniqueId,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
        };

        // Send the known record anchor so a retry after coordinator loss reaches the durable decision.
        if (recordAnchorKey is not null)
            request.RecordAnchorKey = recordAnchorKey;

        int retries = 0;

        GrpcBatcher batcher = GetSharedBatcher(url);
        GrpcCommitTransactionResponse? response;
        MustRetryBackoff? backoff = null;

        try
        {
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

                GrpcBatcherResponse batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);

                response = batchResponse.CommitTransaction;

                if (response is null)
                    throw new KahunaException("Response is null", KeyValueResponseType.Errored);

                if (response.Type == GrpcKeyValueResponseType.TypeCommitted)
                    return (true, response.HasRecordAnchorKey ? response.RecordAnchorKey : null);

                if (response.Type != GrpcKeyValueResponseType.TypeMustRetry)
                    throw new KahunaException("Failed to commit key/value transaction: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);

                logger?.LogDebug("Server asked to retry commit key/value transaction");

                // Carry the coordinator's canonical anchor into the next attempt: a commit that lost its
                // coordinating session still reaches the durable decision as long as the anchor travels with it.
                if (response.HasRecordAnchorKey)
                    request.RecordAnchorKey = response.RecordAnchorKey;

                // Out of attempts: the finalize is still in doubt, not failed. Report it as retryable so the
                // caller re-drives the same commit rather than treating an undecided transaction as an error.
                if (++retries >= TransactionRetries)
                    throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

                backoff ??= new();
                await backoff.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            backoff?.Dispose();
        }
    }

    /// <summary>
    /// Attempts to rollback a transaction session with the specified transaction details.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the rollback request will be executed.</param>
    /// <param name="uniqueId">A unique identifier associated with the session or transaction.</param>
    /// <param name="transactionId">The HLCTimestamp representing the transaction to be rolled back.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests during the rollback operation.</param>
    /// <returns>
    /// A boolean value indicating whether the rollback operation was successful.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown if the rollback operation encounters an error, retries are exhausted, or the operation is explicitly cancelled.
    /// </exception>
    public async Task<bool> RollbackTransactionSession(
        string url,
        string uniqueId,
        HLCTimestamp transactionId,
        string? recordAnchorKey,
        CancellationToken cancellationToken
    )
    {
        GrpcRollbackTransactionRequest request = new()
        {
            CoordinatorKey = uniqueId,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
        };

        // Send the known record anchor so a rollback retry can consult a durably decided commit.
        if (recordAnchorKey is not null)
            request.RecordAnchorKey = recordAnchorKey;

        int retries = 0;

        GrpcBatcher batcher = GetSharedBatcher(url);
        GrpcRollbackTransactionResponse? response;
        MustRetryBackoff? backoff = null;

        try
        {
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

                GrpcBatcherResponse batchResponse = await batcher.Enqueue(request, cancellationToken).ConfigureAwait(false);

                response = batchResponse.RollbackTransaction;

                if (response is null)
                    throw new KahunaException("Response is null", KeyValueResponseType.Errored);

                if (response.Type == GrpcKeyValueResponseType.TypeRolledback)
                    return true;

                if (response.Type != GrpcKeyValueResponseType.TypeMustRetry)
                    throw new KahunaException("Failed to rollback key/value transaction: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);

                logger?.LogDebug("Server asked to retry rollback key/value transaction");

                // Out of attempts: the cleanup is incomplete, not refused. Report it as retryable so the caller
                // can re-drive it instead of reading an unfinished rollback as a permanent error.
                if (++retries >= TransactionRetries)
                    throw new KahunaException("Retries exhausted.", KeyValueResponseType.MustRetry);

                backoff ??= new();
                await backoff.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            backoff?.Dispose();
        }
    }

    public async Task<(SequenceResponseType, ReadOnlySequenceEntry?, int)> GetSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Sequencer.SequencerClient client = GetSequencerClient(channel);

        GrpcSequenceResponse response = await client.GetSequenceAsync(new()
        {
            Name = name,
            Durability = (GrpcSequenceDurability)durability
        }, cancellationToken: cancellationToken).ConfigureAwait(false);

        return ((SequenceResponseType)response.Type, ToReadOnlySequenceEntry(response.Sequence), response.TimeElapsedMs);
    }

    public async Task<(SequenceResponseType, long, int)> CreateSequence(string url, string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken cancellationToken)
    {
        GrpcCreateSequenceRequest request = new()
        {
            Name = name,
            InitialValue = initialValue,
            Increment = increment,
            Durability = (GrpcSequenceDurability)durability
        };

        if (maxValue.HasValue)
            request.MaxValue = maxValue.Value;

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Sequencer.SequencerClient client = GetSequencerClient(channel);
        GrpcSequenceResponse response = await client.CreateSequenceAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);

        return ((SequenceResponseType)response.Type, response.Revision, response.TimeElapsedMs);
    }

    public async Task<(SequenceResponseType, SequenceAllocation, int)> NextSequenceValue(string url, string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
    {
        GrpcNextSequenceRequest request = new()
        {
            Name = name,
            Durability = (GrpcSequenceDurability)durability
        };

        if (idempotencyKey is not null)
            request.IdempotencyKey = idempotencyKey;

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Sequencer.SequencerClient client = GetSequencerClient(channel);
        GrpcSequenceAllocationResponse response = await client.NextSequenceValueAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);

        return ((SequenceResponseType)response.Type, ToSequenceAllocation(response.Allocation), response.TimeElapsedMs);
    }

    public async Task<(SequenceResponseType, SequenceAllocation, int)> ReserveSequenceRange(string url, string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
    {
        GrpcReserveSequenceRangeRequest request = new()
        {
            Name = name,
            Count = count,
            Durability = (GrpcSequenceDurability)durability
        };

        if (idempotencyKey is not null)
            request.IdempotencyKey = idempotencyKey;

        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Sequencer.SequencerClient client = GetSequencerClient(channel);
        GrpcSequenceAllocationResponse response = await client.ReserveSequenceRangeAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);

        return ((SequenceResponseType)response.Type, ToSequenceAllocation(response.Allocation), response.TimeElapsedMs);
    }

    public async Task<(SequenceResponseType, int)> DeleteSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Sequencer.SequencerClient client = GetSequencerClient(channel);

        GrpcSequenceResponse response = await client.DeleteSequenceAsync(new()
        {
            Name = name,
            Durability = (GrpcSequenceDurability)durability
        }, cancellationToken: cancellationToken).ConfigureAwait(false);

        return ((SequenceResponseType)response.Type, response.TimeElapsedMs);
    }

    private static ReadOnlySequenceEntry? ToReadOnlySequenceEntry(GrpcSequenceEntry? entry)
    {
        if (entry is null || string.IsNullOrEmpty(entry.Name))
            return null;

        return new(
            entry.Name,
            entry.CurrentValue,
            entry.InitialValue,
            entry.Increment,
            entry.HasMaxValue ? entry.MaxValue : null,
            entry.Revision,
            (SequenceDurability)entry.Durability,
            new(entry.CreatedAtNode, entry.CreatedAtPhysical, entry.CreatedAtCounter),
            new(entry.UpdatedAtNode, entry.UpdatedAtPhysical, entry.UpdatedAtCounter)
        );
    }

    private static SequenceAllocation ToSequenceAllocation(GrpcSequenceAllocation? allocation)
    {
        if (allocation is null)
            return default;

        return new(
            allocation.Name,
            allocation.Start,
            allocation.End,
            allocation.Count,
            allocation.Revision
        );
    }
    
    private static void AddTransactionParameters(RepeatedField<GrpcKeyValueParameter> target, List<KeyValueParameter> parameters)
    {
        foreach (KeyValueParameter parameter in parameters)
        {
            GrpcKeyValueParameter grpcParameter = new()
            {
                Key = parameter.Key
            };

            if (parameter.Value is not null)
                grpcParameter.Value = parameter.Value;

            target.Add(grpcParameter);
        }
    }

    private GrpcBatcher GetSharedBatcher(string url)
    {
        Lazy<GrpcBatcher> lazyBatchers = batchers.GetOrAdd(url, CreateSharedBatcher);
        return lazyBatchers.Value;
    }

    private Lazy<GrpcBatcher> CreateSharedBatcher(string url)
    {
        TimeSpan timeout = options?.DefaultOperationTimeout ?? TimeSpan.FromSeconds(30);
        return new(() => new(url, timeout, options, logger));
    }

    // Snapshot hold operations are intentionally unary: they are infrequent control-plane calls.
    public async Task<(KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry)> AcquireSnapshotHold(
        string url, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        GrpcAcquireSnapshotHoldResponse response = await client.AcquireSnapshotHoldAsync(
            new GrpcAcquireSnapshotHoldRequest
            {
                HolderId          = holderId,
                TimestampNode     = timestamp.N,
                TimestampPhysical = timestamp.L,
                TimestampCounter  = timestamp.C,
                LeaseMs           = leaseMs
            },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        HLCTimestamp expiry = new(response.LeaseExpiryNode, response.LeaseExpiryPhysical, response.LeaseExpiryCounter);
        return ((KeyValueResponseType)response.Type, response.HoldId, expiry);
    }

    public async Task<(KeyValueResponseType type, HLCTimestamp leaseExpiry)> RenewSnapshotHold(
        string url, string holdId, int leaseMs, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        GrpcRenewSnapshotHoldResponse response = await client.RenewSnapshotHoldAsync(
            new GrpcRenewSnapshotHoldRequest { HoldId = holdId, LeaseMs = leaseMs },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        HLCTimestamp expiry = new(response.LeaseExpiryNode, response.LeaseExpiryPhysical, response.LeaseExpiryCounter);
        return ((KeyValueResponseType)response.Type, expiry);
    }

    public async Task<KeyValueResponseType> ReleaseSnapshotHold(
        string url, string holdId, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        GrpcReleaseSnapshotHoldResponse response = await client.ReleaseSnapshotHoldAsync(
            new GrpcReleaseSnapshotHoldRequest { HoldId = holdId },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        return (KeyValueResponseType)response.Type;
    }

    public async Task<(HLCTimestamp effectiveFloor, int liveHolds)> GetSnapshotFloor(
        string url, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        GrpcGetSnapshotFloorResponse response = await client.GetSnapshotFloorAsync(
            new GrpcGetSnapshotFloorRequest(),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // TypeGot is a real answer; TypeSet (the proto default) is what a server predating the
        // type field sends, so it is accepted as success too. Anything else — in particular
        // MustRetry, from a node that could not confirm meta-partition leadership — must not be
        // reported as data: an empty floor is indistinguishable from "no holds anywhere".
        if (response.Type is not (GrpcKeyValueResponseType.TypeGot or GrpcKeyValueResponseType.TypeSet))
            throw new KahunaException("GetSnapshotFloor failed", (KeyValueResponseType)response.Type);

        HLCTimestamp floor = new(response.EffectiveFloorNode, response.EffectiveFloorPhysical, response.EffectiveFloorCounter);
        return (floor, response.LiveHolds);
    }

    // Intentionally unary: key-range registration is a one-shot control-plane operation performed
    // at startup or on topology changes; it is not a hot-path call suitable for batcher coalescing.
    public async Task<KahunaRegisterKeyRangeResponse> RegisterKeyRange(string url, string keySpace, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        GrpcRegisterKeyRangeResponse response = await client.RegisterKeyRangeAsync(new()
        {
            KeySpace = keySpace
        }, cancellationToken: cancellationToken).ConfigureAwait(false);

        return new KahunaRegisterKeyRangeResponse
        {
            Success = response.Success,
            Status = response.Status,
            Seeded = response.Seeded,
            RoutingMode = response.RoutingMode,
            DescriptorCount = response.DescriptorCount,
            Reason = string.IsNullOrEmpty(response.Reason) ? null : response.Reason
        };
    }

    public async Task<KahunaRemoveKeyRangeResponse> RemoveKeyRange(string url, string keySpace, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        KeyValuer.KeyValuerClient client = GetKeyValueClient(channel);

        GrpcRemoveKeyRangeResponse response = await client.RemoveKeyRangeAsync(new()
        {
            KeySpace = keySpace
        }, cancellationToken: cancellationToken).ConfigureAwait(false);

        return new KahunaRemoveKeyRangeResponse
        {
            Success = response.Success,
            Status = response.Status,
            RoutingMode = response.RoutingMode,
            DescriptorCount = response.DescriptorCount,
            Reason = string.IsNullOrEmpty(response.Reason) ? null : response.Reason
        };
    }

    public async Task<KahunaRangeMapResponse> GetRanges(string url, string? keySpace, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Cluster.ClusterClient client = GetClusterClient(channel);

        GrpcGetRangesResponse response = await client.GetRangesAsync(
            new GrpcGetRangesRequest { KeySpace = keySpace ?? "" },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        List<KahunaKeySpaceRangesResponse> keySpaces = new(response.KeySpaces.Count);
        foreach (GrpcKeySpaceRanges space in response.KeySpaces)
        {
            KahunaKeySpaceRangesResponse entry = new()
            {
                KeySpace = space.KeySpace,
                RoutingMode = space.RoutingMode
            };

            // Field presence, not emptiness, is what says "±infinity": HasStartKey false means the
            // range opens at -inf, while an empty string would be a real (if odd) bound.
            foreach (GrpcRangeDescriptor descriptor in space.Descriptors)
                entry.Descriptors.Add(new KahunaRangeDescriptorResponse
                {
                    StartKey = descriptor.HasStartKey ? descriptor.StartKey : null,
                    EndKey = descriptor.HasEndKey ? descriptor.EndKey : null,
                    PartitionId = descriptor.PartitionId,
                    Generation = descriptor.Generation
                });

            keySpaces.Add(entry);
        }

        return new KahunaRangeMapResponse
        {
            Initialized = response.Initialized,
            LocalEndpoint = response.LocalEndpoint,
            KeySpaces = keySpaces
        };
    }

    public async Task<KahunaSplitRangeResponse> SplitRange(
        string url, string keySpace, string splitKey, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Cluster.ClusterClient client = GetClusterClient(channel);

        GrpcSplitRangeResponse response = await client.SplitRangeAsync(
            new GrpcSplitRangeRequest { KeySpace = keySpace, SplitKey = splitKey },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        return new KahunaSplitRangeResponse
        {
            Success = response.Success,
            Status = response.Status,
            Determinate = response.Determinate,
            NewPartitionId = response.NewPartitionId,
            NewGeneration = response.NewGeneration,
            LeaderHint = string.IsNullOrEmpty(response.LeaderHint) ? null : response.LeaderHint,
            Reason = string.IsNullOrEmpty(response.Reason) ? null : response.Reason
        };
    }

    public async Task<KahunaMergeRangesResponse> MergeRanges(string url, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Cluster.ClusterClient client = GetClusterClient(channel);

        GrpcMergeRangesResponse response = await client.MergeRangesAsync(
            new GrpcMergeRangesRequest(),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        return new KahunaMergeRangesResponse
        {
            Success = response.Success,
            Status = response.Status,
            Determinate = response.Determinate,
            Merges = response.Merges,
            LeaderHint = string.IsNullOrEmpty(response.LeaderHint) ? null : response.LeaderHint,
            Reason = string.IsNullOrEmpty(response.Reason) ? null : response.Reason
        };
    }

    public async Task<KahunaClusterMembershipResponse> GetClusterMembership(string url, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Cluster.ClusterClient client = GetClusterClient(channel);

        GrpcGetMembershipResponse response = await client.GetMembershipAsync(
            new GrpcGetMembershipRequest(),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        List<KahunaClusterMemberResponse> members = new(response.Members.Count);
        foreach (GrpcClusterMember m in response.Members)
        {
            members.Add(new KahunaClusterMemberResponse
            {
                Endpoint = m.Endpoint,
                NodeId = m.NodeId,
                Role = GrpcRoleToString(m.Role),
                JoinedVersion = m.JoinedVersion
            });
        }

        return new KahunaClusterMembershipResponse
        {
            MembershipVersion = response.MembershipVersion,
            Members = members,
            LocalRole = GrpcRoleToString(response.LocalRole),
            Initialized = response.Initialized
        };
    }

    public async Task<KahunaClusterPlacementResponse> GetClusterPlacement(string url, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Cluster.ClusterClient client = GetClusterClient(channel);

        GrpcGetPlacementResponse response = await client.GetPlacementAsync(
            new GrpcGetPlacementRequest(),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        List<KahunaPartitionPlacementResponse> partitions = new(response.Partitions.Count);
        foreach (GrpcPartitionPlacement p in response.Partitions)
        {
            KahunaPartitionPlacementResponse partition = new()
            {
                PartitionId = p.PartitionId,
                State = p.State,
                Generation = p.Generation,
                EffectiveReplicationFactor = p.EffectiveReplicationFactor,
                HostedLocally = p.HostedLocally
            };

            foreach (GrpcPartitionReplica r in p.Replicas)
                partition.Replicas.Add(new KahunaPartitionReplicaResponse
                {
                    Endpoint = r.Endpoint,
                    Role = GrpcReplicaRoleToString(r.Role)
                });

            partitions.Add(partition);
        }

        return new KahunaClusterPlacementResponse
        {
            ReplicationFactor = response.ReplicationFactor,
            RebalancerEnabled = response.RebalancerEnabled,
            Initialized = response.Initialized,
            LocalEndpoint = response.LocalEndpoint,
            HostedPartitionCount = response.HostedPartitionCount,
            Partitions = partitions
        };
    }

    public async Task<KahunaSetReplicationFactorResponse> SetReplicationFactor(
        string url, int partitionId, int replicationFactor, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Cluster.ClusterClient client = GetClusterClient(channel);

        GrpcSetReplicationFactorResponse response = await client.SetReplicationFactorAsync(
            new GrpcSetReplicationFactorRequest { PartitionId = partitionId, ReplicationFactor = replicationFactor },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        return new KahunaSetReplicationFactorResponse
        {
            Success = response.Success,
            Status = response.Status,
            Generation = response.Generation,
            Reason = string.IsNullOrEmpty(response.Reason) ? null : response.Reason
        };
    }

    private static string GrpcReplicaRoleToString(GrpcPartitionReplicaRole role) => role switch
    {
        GrpcPartitionReplicaRole.PartitionReplicaRoleVoter    => "Voter",
        GrpcPartitionReplicaRole.PartitionReplicaRoleLearner  => "Learner",
        GrpcPartitionReplicaRole.PartitionReplicaRoleRemoving => "Removing",
        _                                                     => role.ToString()
    };

    public async Task<KahunaClusterLeaveResponse> LeaveCluster(string url, CancellationToken cancellationToken)
    {
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
        Cluster.ClusterClient client = GetClusterClient(channel);

        GrpcClusterLeaveResponse response = await client.LeaveAsync(
            new GrpcClusterLeaveRequest(),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        return new KahunaClusterLeaveResponse
        {
            Left = response.Left,
            Drained = response.Drained,
            Outcome = GrpcLeaveOutcomeToString(response.Outcome),
            MembershipVersion = response.MembershipVersion,
            Retryable = response.Retryable,
            Reason = response.Reason
        };
    }

    /// <summary>
    /// Names match the outcome names the REST surface reports, so callers can branch on one
    /// vocabulary regardless of transport.
    /// </summary>
    private static string GrpcLeaveOutcomeToString(GrpcLeaveClusterOutcome outcome) => outcome switch
    {
        GrpcLeaveClusterOutcome.LeaveClusterOutcomeCommitted                 => "Committed",
        GrpcLeaveClusterOutcome.LeaveClusterOutcomeNotAMember                => "NotAMember",
        GrpcLeaveClusterOutcome.LeaveClusterOutcomeRefusedInsufficientVoters => "RefusedInsufficientVoters",
        GrpcLeaveClusterOutcome.LeaveClusterOutcomeNotInitialized            => "NotInitialized",
        GrpcLeaveClusterOutcome.LeaveClusterOutcomeNoLeader                  => "NoLeader",
        GrpcLeaveClusterOutcome.LeaveClusterOutcomeRefusedDrainInProgress    => "RefusedDrainInProgress",
        GrpcLeaveClusterOutcome.LeaveClusterOutcomeDrainTimedOut             => "DrainTimedOut",
        _                                                                    => "Timeout"
    };

    private static string GrpcRoleToString(GrpcClusterMemberRole role) => role switch
    {
        GrpcClusterMemberRole.ClusterMemberRoleLearner   => "Learner",
        GrpcClusterMemberRole.ClusterMemberRoleVoter     => "Voter",
        GrpcClusterMemberRole.ClusterMemberRoleLeaving   => "Leaving",
        GrpcClusterMemberRole.ClusterMemberRoleNotMember => "NotMember",
        _                                                 => "NotMember"
    };

    public Task<KahunaBackupInfo> TakeFullBackup(string url, CancellationToken cancellationToken) =>
        InvokeBackup(async () =>
        {
            GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
            Backups.BackupsClient client = GetBackupsClient(channel);
            GrpcBackupInfoResponse r = await client.TakeFullBackupAsync(
                new GrpcTakeFullBackupRequest(), cancellationToken: cancellationToken).ConfigureAwait(false);
            return FromGrpc(r);
        });

    public Task<KahunaBackupInfo> TakeIncrementalBackup(string url, Guid parentBackupId, CancellationToken cancellationToken) =>
        InvokeBackup(async () =>
        {
            GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
            Backups.BackupsClient client = GetBackupsClient(channel);
            GrpcBackupInfoResponse r = await client.TakeIncrementalBackupAsync(
                new GrpcTakeIncrementalBackupRequest { ParentBackupId = parentBackupId.ToString() },
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return FromGrpc(r);
        });

    public Task<KahunaBackupInfo> TakeCoordinatedBackup(string url, CancellationToken cancellationToken) =>
        InvokeBackup(async () =>
        {
            GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
            Backups.BackupsClient client = GetBackupsClient(channel);
            GrpcBackupInfoResponse r = await client.TakeCoordinatedBackupAsync(
                new GrpcTakeCoordinatedBackupRequest(), cancellationToken: cancellationToken).ConfigureAwait(false);
            return FromGrpc(r);
        });

    public Task<List<KahunaBackupInfo>> ListBackups(string url, CancellationToken cancellationToken) =>
        InvokeBackup(async () =>
        {
            GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
            Backups.BackupsClient client = GetBackupsClient(channel);
            GrpcListBackupsResponse r = await client.ListBackupsAsync(
                new GrpcListBackupsRequest(), cancellationToken: cancellationToken).ConfigureAwait(false);
            return r.Backups.Select(FromGrpc).ToList();
        });

    public Task<List<KahunaBackupInfo>> GetBackupChain(string url, Guid leafBackupId, CancellationToken cancellationToken) =>
        InvokeBackup(async () =>
        {
            GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
            Backups.BackupsClient client = GetBackupsClient(channel);
            GrpcListBackupsResponse r = await client.GetBackupChainAsync(
                new GrpcGetBackupChainRequest { LeafBackupId = leafBackupId.ToString() },
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return r.Backups.Select(FromGrpc).ToList();
        });

    public Task<KahunaRestoreResponse> Restore(string url, Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken cancellationToken) =>
        InvokeBackup(async () =>
        {
            GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
            Backups.BackupsClient client = GetBackupsClient(channel);
            GrpcRestoreResponse r = await client.RestoreAsync(
                new GrpcRestoreRequest { LeafBackupId = leafBackupId.ToString(), TargetDir = targetDir, TargetTimeMs = targetTimeMs },
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return new KahunaRestoreResponse
            {
                TargetDir = r.TargetDir,
                PartitionsRestored = r.PartitionsRestored,
                EntriesApplied = r.EntriesApplied,
                LastAppliedPhysicalMs = r.LastAppliedPhysicalMs,
                Chain = r.Chain.Select(FromGrpc).ToList(),
                Outcome = Enum.TryParse(r.Outcome, out KahunaBackupOutcome o) ? o : KahunaBackupOutcome.Ok,
                MinRecoverablePhysicalMs = r.MinRecoverablePhysicalMs,
                MaxRecoverablePhysicalMs = r.MaxRecoverablePhysicalMs
            };
        });

    public Task<KahunaBackupGcResult> RunBackupGarbageCollection(string url, bool dryRun, CancellationToken cancellationToken) =>
        InvokeBackup(async () =>
        {
            GrpcChannel channel = GrpcBatcher.GetSharedChannel(url, options);
            Backups.BackupsClient client = GetBackupsClient(channel);
            GrpcBackupGcResponse r = await client.RunBackupGarbageCollectionAsync(
                new GrpcBackupGcRequest { DryRun = dryRun }, cancellationToken: cancellationToken).ConfigureAwait(false);
            return new KahunaBackupGcResult
            {
                Applied = r.Applied,
                BytesReclaimed = r.BytesReclaimed,
                RetentionDeletions = r.RetentionDeletions.Select(d => new KahunaBackupGcDeletion
                {
                    BackupId = Guid.Parse(d.BackupId),
                    Type = d.Type,
                    CreatedAtUtc = DateTime.Parse(d.CreatedAtUtc),
                    Bytes = d.Bytes,
                    Reason = d.Reason
                }).ToList(),
                OrphanReclamations = r.OrphanReclamations.Select(o => new KahunaBackupGcOrphan
                {
                    Name = o.Name,
                    IsDirectory = o.IsDirectory,
                    Reason = o.Reason
                }).ToList()
            };
        });

    private static KahunaBackupInfo FromGrpc(GrpcBackupInfoResponse r) => new()
    {
        BackupId = Guid.Parse(r.BackupId),
        Type = r.Type,
        CreatedAtUtc = DateTime.Parse(r.CreatedAtUtc),
        ParentBackupId = string.IsNullOrEmpty(r.ParentBackupId) ? null : Guid.Parse(r.ParentBackupId),
        PartitionCount = r.PartitionCount,
        ClusterSnapshotNode = r.HasSnapshotTime ? r.SnapshotNode : null,
        ClusterSnapshotPhysical = r.HasSnapshotTime ? r.SnapshotPhysical : null,
        ClusterSnapshotCounter = r.HasSnapshotTime ? r.SnapshotCounter : null,
        RequestedKind = string.IsNullOrEmpty(r.RequestedKind) ? null : r.RequestedKind,
        ActualKind = string.IsNullOrEmpty(r.ActualKind) ? null : r.ActualKind,
        SubstitutionReason = string.IsNullOrEmpty(r.SubstitutionReason) ? null : r.SubstitutionReason,
        FormatVersion = r.FormatVersion,
        IsInvalid = r.IsInvalid,
        IsIncomplete = r.IsIncomplete,
        InvalidReason = string.IsNullOrEmpty(r.InvalidReason) ? null : r.InvalidReason,
        MinRecoverablePhysicalMs = r.HasCoverage ? r.MinRecoverablePhysicalMs : null,
        MaxRecoverablePhysicalMs = r.HasCoverage ? r.MaxRecoverablePhysicalMs : null,
        ClusterId = string.IsNullOrEmpty(r.ClusterId) ? null : r.ClusterId,
        CoordinatorNode = string.IsNullOrEmpty(r.CoordinatorNode) ? null : r.CoordinatorNode
    };

    /// <summary>
    /// Escalating base delays for the retry loops that re-issue a request the server answered with
    /// MustRetry. Mirrors the REST transport's policy.
    /// </summary>
    private static readonly int[] MustRetryDelaysMs = [1, 2, 3, 4, 6, 8, 10];

    /// <summary>
    /// Waits before re-issuing a request the server answered with MustRetry. The delay carries ±25% jitter
    /// so a fleet of clients that all saw the same leader flip does not retry in lockstep and re-create the
    /// contention they are waiting out. Allocation-free and stateless, so every retry loop in this transport
    /// can share one policy.
    /// </summary>
    private static Task WaitBeforeMustRetry(int attempt, CancellationToken cancellationToken)
    {
        int baseMs = MustRetryDelaysMs[Math.Min(attempt, MustRetryDelaysMs.Length - 1)];
        double jittered = baseMs * (0.75 + Random.Shared.NextDouble() * 0.5);

        return Task.Delay(TimeSpan.FromMilliseconds(jittered), cancellationToken);
    }

    /// <summary>
    /// Runs a backup gRPC call, reconstructing a typed <see cref="KahunaBackupException"/> from the
    /// outcome trailer when the server rejected the operation.
    /// </summary>
    private static async Task<T> InvokeBackup<T>(Func<Task<T>> call)
    {
        try
        {
            return await call().ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            Metadata.Entry? outcomeEntry = ex.Trailers.Get(KahunaBackupWire.OutcomeGrpcTrailer);
            if (outcomeEntry is not null &&
                Enum.TryParse(outcomeEntry.Value, out KahunaBackupOutcome outcome))
                throw new KahunaBackupException(outcome, ex.Status.Detail);
            throw;
        }
    }
}
