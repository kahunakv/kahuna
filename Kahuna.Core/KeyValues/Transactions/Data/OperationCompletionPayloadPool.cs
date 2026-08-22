
using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Bounded shared pool for <see cref="OperationCompletionPayload"/> shells. Ownership is
/// creator-recycled and single-owner-only: the site that rents a shell returns it exactly once,
/// and only on a path where nothing else can still hold the reference. Concretely:
/// a completion acknowledged by the coordinator recycles (the shell was never shared); a failed
/// completion hands the shell to the participant retry cache, which becomes its final owner —
/// cache-owned shells are never recycled because recovery can give the reference to concurrent
/// same-id retries. The inter-node landing point recycles the shell it decoded from the wire
/// after the fold returns. A shell that misses its return is simply reclaimed by the GC.
/// </summary>
internal static class OperationCompletionPayloadPool
{
    /// <summary>
    /// Upper bound on retained shells. Bounded so a traffic burst cannot pin its peak object
    /// population indefinitely; returns beyond the cap drop the object for the GC to reclaim.
    /// </summary>
    private const int MaxPooled = 4096;

    /// <summary>
    /// Single shared pool rather than per-thread stacks: a shell is rented on the request thread
    /// and returned on a continuation thread after the completion round-trip, so thread-local
    /// storage would strand returned shells on threads that never rent.
    /// </summary>
    private static readonly ConcurrentQueue<OperationCompletionPayload> pooledPayloads = new();

    /// <summary>
    /// Approximate pooled count used only to enforce <see cref="MaxPooled"/>. Checked before the
    /// enqueue, so brief overshoot by the number of concurrent returners is possible and harmless.
    /// </summary>
    private static int pooledCount;

    /// <summary>Rents a cleared shell; every property holds its post-construction default.</summary>
    public static OperationCompletionPayload Rent()
    {
        if (pooledPayloads.TryDequeue(out OperationCompletionPayload? payload))
        {
            Interlocked.Decrement(ref pooledCount);
            return payload;
        }

        return new();
    }

    /// <summary>
    /// Clears and recycles a shell. Call only while holding the sole reference — never for a shell
    /// that was stored in the participant retry cache or received from another owner.
    /// </summary>
    public static void Return(OperationCompletionPayload payload)
    {
        payload.Clear();

        // Bounded retention: beyond the cap the object is dropped rather than pooled. Pooling is
        // only an optimization, so discarding an excess shell is always safe.
        if (Volatile.Read(ref pooledCount) >= MaxPooled)
            return;

        Interlocked.Increment(ref pooledCount);
        pooledPayloads.Enqueue(payload);
    }
}
