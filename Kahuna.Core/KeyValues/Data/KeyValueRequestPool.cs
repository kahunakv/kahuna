using System.Collections.Concurrent;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

internal static class KeyValueRequestPool
{
    /// <summary>
    /// Upper bound on retained requests. Bounded so a traffic burst cannot pin its peak object
    /// population indefinitely; returns beyond the cap drop the object for the GC to reclaim.
    /// </summary>
    private const int MaxPooled = 4096;

    /// <summary>
    /// Single shared pool rather than per-thread stacks: rent and return almost always land on
    /// different threads (a request is rented on the producer's thread and returned on an actor or
    /// continuation thread), so thread-local storage strands returned objects on threads that never
    /// rent and makes most rents allocate fresh.
    /// </summary>
    private static readonly ConcurrentQueue<KeyValueRequest> pooledRequests = new();

    /// <summary>
    /// Approximate pooled count used only to enforce <see cref="MaxPooled"/>. Checked before the
    /// enqueue, so brief overshoot by the number of concurrent returners is possible and harmless.
    /// </summary>
    private static int pooledCount;

    public static KeyValueRequest Rent(
        KeyValueRequestType type,
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key, 
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs, 
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability,
        int proposalId, 
        int partitionId,
        TaskCompletionSource<KeyValueResponse?>? promise
    )
    {
        if (pooledRequests.TryDequeue(out KeyValueRequest? request))
        {
            Interlocked.Decrement(ref pooledCount);

            request.Reset(
                type,
                transactionId,
                commitId,
                key, 
                value,
                compareValue,
                compareRevision,
                flags,
                expiresMs, 
                proposalTicketId,
                durability,
                proposalId, 
                partitionId,
                promise
            );
            
            return request;
        }
        
        return new(
            type,
            transactionId,
            commitId,
            key, 
            value,
            compareValue,
            compareRevision,
            flags,
            expiresMs, 
            proposalTicketId,
            durability,
            proposalId, 
            partitionId,
            promise
        );
    }
    
    /// <summary>
    /// Rents a <see cref="KeyValueRequest"/> pre-populated for a GetByRange scan.
    /// </summary>
    public static KeyValueRequest RentRange(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        TaskCompletionSource<KeyValueResponse?>? promise)
    {
        KeyValueRequest request = Rent(
            KeyValueRequestType.GetByRange,
            transactionId,
            HLCTimestamp.Zero,
            prefix,
            null,
            null,
            0,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            promise
        );

        request.StartKey       = startKey;
        request.StartInclusive = startInclusive;
        request.EndKey         = endKey;
        request.EndInclusive   = endInclusive;
        request.Limit          = limit;
        request.ReadTimestamp  = readTimestamp;

        return request;
    }

    /// <summary>
    /// Rents a <see cref="KeyValueRequest"/> pre-populated for an <c>InvalidateOrApply</c> message; the
    /// payload record is still allocated fresh. Ownership of the rented shell depends on the dispatch
    /// style: an awaited <c>Ask</c> caller keeps <paramref name="returnToPoolOnReceive"/> false and calls
    /// <see cref="Return"/> itself after the response arrives; a fire-and-forget <c>Send</c> caller passes
    /// true (and must keep no reference after the send) so the receiving actor recycles the request —
    /// mixing the two on one request would return it twice.
    /// </summary>
    public static KeyValueRequest RentInvalidateOrApply(
        string key,
        long revision,
        byte[]? value,
        HLCTimestamp expires,
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        KeyValueState state,
        bool forceResident,
        HLCTimestamp transactionId,
        int partitionId,
        bool noRevision,
        bool isRollback,
        bool returnToPoolOnReceive = false,
        bool backendHydrated = false,
        ReadOnlyKeyValueEntry? hydratedEntry = null,
        bool reconcile = false)
    {
        KeyValueRequest request = Rent(
            KeyValueRequestType.InvalidateOrApply,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            0,
            null
        );

        request.InvalidateOrApplyData = new(
            revision, value, expires, lastUsed, lastModified, state,
            forceResident, transactionId, partitionId, noRevision, isRollback,
            backendHydrated, hydratedEntry, reconcile);

        request.ReturnToPoolOnReceive = returnToPoolOnReceive;

        return request;
    }

    /// <summary>
    /// Rents a <c>CompleteProposal</c> message: the partition write aggregator notifying the owning key
    /// actor that its direct-write proposal committed, so the actor applies it and resolves
    /// <paramref name="promise"/>. Fire-and-forget with ownership transfer — the sender keeps no
    /// reference after the send and the receiving actor returns the request to the pool.
    /// </summary>
    public static KeyValueRequest RentCompleteProposal(
        string key,
        KeyValueDurability durability,
        int proposalId,
        int partitionId,
        TaskCompletionSource<KeyValueResponse?> promise)
    {
        KeyValueRequest request = Rent(
            KeyValueRequestType.CompleteProposal,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            proposalId,
            partitionId,
            promise);

        request.ReturnToPoolOnReceive = true;

        return request;
    }

    /// <summary>
    /// Rents a <c>ReleaseProposal</c> message: the partition write aggregator notifying the owning key
    /// actor that its direct-write proposal did not commit, so the actor unwinds it and resolves
    /// <paramref name="promise"/> (<see cref="KeyValueFlags.ReplicationRetry"/> marks a transient
    /// failure). Fire-and-forget with ownership transfer — the sender keeps no reference after the send
    /// and the receiving actor returns the request to the pool.
    /// </summary>
    public static KeyValueRequest RentReleaseProposal(
        string key,
        KeyValueFlags flags,
        KeyValueDurability durability,
        int proposalId,
        int partitionId,
        TaskCompletionSource<KeyValueResponse?> promise)
    {
        KeyValueRequest request = Rent(
            KeyValueRequestType.ReleaseProposal,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            -1,
            flags,
            0,
            HLCTimestamp.Zero,
            durability,
            proposalId,
            partitionId,
            promise);

        request.ReturnToPoolOnReceive = true;

        return request;
    }

    /// <summary>
    /// Rents a <c>FlushAck</c> message: the background writer notifying the owning actor that
    /// <paramref name="key"/> has been durably stored up to <paramref name="revision"/>. The revision
    /// travels in <see cref="KeyValueRequest.CompareRevision"/> (unused by this message type); routing
    /// keys on <paramref name="key"/> so it reaches the same actor that holds the entry.
    /// Fire-and-forget with ownership transfer: the receiving actor returns the request after handling
    /// it. That is only sound while flush acks have a single producer and a single consistent-hash
    /// delivery — a second producer that kept a reference after sending would race the recycled object.
    /// </summary>
    public static KeyValueRequest RentFlushAck(string key, long revision)
    {
        KeyValueRequest request = Rent(
            KeyValueRequestType.FlushAck,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            revision,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            0,
            null);

        request.ReturnToPoolOnReceive = true;

        return request;
    }

    public static void Return(KeyValueRequest obj)
    {
        obj.Clear();

        // Bounded retention: beyond the cap the object is dropped rather than pooled. Pooling is
        // only an optimization, so discarding an excess request is always safe.
        if (Volatile.Read(ref pooledCount) >= MaxPooled)
            return;

        Interlocked.Increment(ref pooledCount);
        pooledRequests.Enqueue(obj);
    }
}