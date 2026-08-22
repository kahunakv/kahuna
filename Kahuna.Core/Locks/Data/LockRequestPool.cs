
using System.Collections.Concurrent;

using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// Bounded shared pool for <see cref="LockRequest"/> instances. Ownership follows the dispatch
/// style: an awaited <c>Ask</c> caller keeps <see cref="LockRequest.ReturnToPoolOnReceive"/>
/// false and calls <see cref="Return"/> itself after the reply arrives; a fire-and-forget
/// <c>Send</c> caller rents through a helper that sets the flag (and must keep no reference after
/// the send) so the receiving actor recycles the request. Mixing the two on one request would
/// return it twice.
/// </summary>
internal static class LockRequestPool
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
    private static readonly ConcurrentQueue<LockRequest> pooledRequests = new();

    /// <summary>
    /// Approximate pooled count used only to enforce <see cref="MaxPooled"/>. Checked before the
    /// enqueue, so brief overshoot by the number of concurrent returners is possible and harmless.
    /// </summary>
    private static int pooledCount;

    public static LockRequest Rent(
        LockRequestType type,
        string resource,
        byte[]? owner,
        int expiresMs,
        LockDurability durability,
        int proposalId,
        int partitionId,
        TaskCompletionSource<LockResponse?>? promise,
        long proposalLogIndex = -1,
        LockInvalidateOrApplyData? invalidateOrApplyData = null
    )
    {
        if (pooledRequests.TryDequeue(out LockRequest? request))
        {
            Interlocked.Decrement(ref pooledCount);

            request.Reset(
                type,
                resource,
                owner,
                expiresMs,
                durability,
                proposalId,
                partitionId,
                promise,
                proposalLogIndex,
                invalidateOrApplyData
            );

            return request;
        }

        return new(
            type,
            resource,
            owner,
            expiresMs,
            durability,
            proposalId,
            partitionId,
            promise,
            proposalLogIndex,
            invalidateOrApplyData
        );
    }

    /// <summary>
    /// Rents a <c>CompleteProposal</c> message: the proposal actor notifying the owning lock actor
    /// that its proposal committed, so the actor installs it and resolves
    /// <paramref name="promise"/>. Fire-and-forget with ownership transfer — the sender keeps no
    /// reference after the send and the receiving actor returns the request to the pool.
    /// </summary>
    public static LockRequest RentCompleteProposal(
        string resource,
        LockDurability durability,
        int proposalId,
        int partitionId,
        TaskCompletionSource<LockResponse?>? promise,
        long proposalLogIndex)
    {
        LockRequest request = Rent(
            LockRequestType.CompleteProposal,
            resource,
            null,
            0,
            durability,
            proposalId,
            partitionId,
            promise,
            proposalLogIndex);

        request.ReturnToPoolOnReceive = true;

        return request;
    }

    /// <summary>
    /// Rents a <c>ReleaseProposal</c> message: the proposal actor notifying the owning lock actor
    /// that its proposal did not commit, so the actor unwinds the replication intent and resolves
    /// <paramref name="promise"/>. Fire-and-forget with ownership transfer — the sender keeps no
    /// reference after the send and the receiving actor returns the request to the pool.
    /// </summary>
    public static LockRequest RentReleaseProposal(
        string resource,
        LockDurability durability,
        int proposalId,
        int partitionId,
        TaskCompletionSource<LockResponse?>? promise)
    {
        LockRequest request = Rent(
            LockRequestType.ReleaseProposal,
            resource,
            null,
            0,
            durability,
            proposalId,
            partitionId,
            promise);

        request.ReturnToPoolOnReceive = true;

        return request;
    }

    /// <summary>
    /// Rents an <c>InvalidateOrApply</c> message; the payload record is still allocated fresh.
    /// Fire-and-forget with ownership transfer — the sender keeps no reference after the send and
    /// the receiving actor returns the request to the pool.
    /// </summary>
    public static LockRequest RentInvalidateOrApply(
        string resource,
        byte[]? owner,
        int partitionId,
        long fencingToken,
        HLCTimestamp expires,
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        LockState state)
    {
        LockRequest request = Rent(
            LockRequestType.InvalidateOrApply,
            resource,
            owner,
            0,
            LockDurability.Persistent,
            0,
            partitionId,
            null,
            invalidateOrApplyData: new(fencingToken, expires, lastUsed, lastModified, state));

        request.ReturnToPoolOnReceive = true;

        return request;
    }

    public static void Return(LockRequest obj)
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
