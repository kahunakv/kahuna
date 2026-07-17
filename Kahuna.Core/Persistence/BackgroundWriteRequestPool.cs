
using System.Collections.Concurrent;
using Kommander.Time;

namespace Kahuna.Server.Persistence;

/// <summary>
/// A bounded, thread-safe pool of <see cref="BackgroundWriteRequest"/> objects for the high-frequency
/// <c>QueueStore*</c> write path. One request is created per persistent mutation, so pooling removes
/// that per-mutation allocation and the GC pressure it drives under write-heavy load.
///
/// <para>
/// <b>Ownership rule.</b> Producers (KV/lock write handlers, replicators, restorers) <see cref="Rent"/>
/// on their own threads, fill the request, and send it to the writer actor. The single-threaded writer
/// actor is the only <see cref="Return"/> caller, and it returns a request <i>only after</i> it has
/// copied the request into a <see cref="PersistenceRequestItem"/> during a flush — at which point
/// nothing references the request (the retry path holds the self-contained item structs, not the
/// request). Each request is therefore rented once and returned at most once, so no instance is ever
/// aliased between two owners. Reusing a returned request only reassigns its own references; the
/// already-built item keeps its own (immutable) key/value references, so it is unaffected.
/// </para>
///
/// <para><see cref="ConcurrentQueue{T}"/> makes concurrent rent/return safe; the count is bounded so a
/// burst cannot pin an unbounded population, and returns beyond the cap drop the object for the GC.</para>
/// </summary>
internal static class BackgroundWriteRequestPool
{
    /// <summary>Upper bound on pooled instances. A drained flush returns up to a full batch at a time.</summary>
    private const int MaxPooled = 4096;

    private static readonly ConcurrentQueue<BackgroundWriteRequest> pool = new();

    private static int count;

    public static BackgroundWriteRequest Rent(
        BackgroundWriteType type,
        int partitionId,
        string key,
        byte[]? value,
        long revision,
        HLCTimestamp expires,
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        int state,
        bool noRevision = false)
    {
        if (pool.TryDequeue(out BackgroundWriteRequest? request))
        {
            Interlocked.Decrement(ref count);
            request.Reset(type, partitionId, key, value, revision, expires, lastUsed, lastModified, state, noRevision);
            return request;
        }

        return new BackgroundWriteRequest(type, partitionId, key, value, revision, expires, lastUsed, lastModified, state, noRevision);
    }

    /// <summary>
    /// Returns a fully-consumed request to the pool. Must only be called once the writer has copied the
    /// request into a persistence item and dropped every other reference to it.
    /// </summary>
    public static void Return(BackgroundWriteRequest request)
    {
        request.Clear();

        // Racy check-then-increment: the count may briefly exceed the cap under contention, which is
        // harmless — the bound only exists to stop unbounded growth, not to be exact.
        if (Volatile.Read(ref count) >= MaxPooled)
            return;

        Interlocked.Increment(ref count);
        pool.Enqueue(request);
    }
}
