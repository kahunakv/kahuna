
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
/// <para>
/// <b>Bound.</b> The writer returns requests only while it drains at a flush tick, so between two ticks
/// the producers rent a whole interval's population while nothing comes back, and a bound below that
/// population makes every rent past it allocate. The writer therefore sizes the pool at the start of
/// every tick from its dirty-queue depth (<see cref="SizeToQueued"/>): the queues at a tick start hold
/// exactly what was rented since the previous tick, and the drain that follows returns it. One tick later
/// the pool holds the steady-state demand and <see cref="Rent"/> stops allocating. The bound is clamped to
/// [<see cref="MinCapacity"/>, <see cref="MaxCapacity"/>], and sizing down trims the excess at once, so an
/// idle process never retains a past peak.
/// </para>
///
/// <para>
/// <see cref="Shared"/> is the process-wide pool every producer and the writer use. The sizing assumes one
/// writer per process. An in-process cluster (tests, the embedded in-memory cluster) runs one writer per
/// node and the last tick wins; that only lowers how much the pool retains, never correctness.
/// </para>
///
/// <para><see cref="ConcurrentQueue{T}"/> makes concurrent rent/return safe; the count is approximate and
/// only enforces the bound, so returns beyond it drop the object for the GC.</para>
/// </summary>
internal sealed class BackgroundWriteRequestPool
{
    /// <summary>Smallest bound: the initial capacity, and the floor the sizing never goes below.</summary>
    internal const int MinCapacity = 4096;

    /// <summary>Largest bound: about 16 MB of pooled requests, no more than the writer's own dirty queue
    /// holds at that depth.</summary>
    internal const int MaxCapacity = 131_072;

    /// <summary>The process-wide pool.</summary>
    public static readonly BackgroundWriteRequestPool Shared = new();

    private readonly ConcurrentQueue<BackgroundWriteRequest> pool = new();

    private int count;

    private int capacity = MinCapacity;

    /// <summary>The current bound on pooled instances.</summary>
    internal int Capacity => Volatile.Read(ref capacity);

    /// <summary>Approximate number of pooled instances.</summary>
    internal int PooledCount => Volatile.Read(ref count);

    public BackgroundWriteRequest Rent(
        BackgroundWriteType type,
        int partitionId,
        string key,
        byte[]? value,
        long revision,
        HLCTimestamp expires,
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        int state,
        bool noRevision = false,
        long logIndex = -1)
    {
        if (pool.TryDequeue(out BackgroundWriteRequest? request))
        {
            Interlocked.Decrement(ref count);
            request.Reset(type, partitionId, key, value, revision, expires, lastUsed, lastModified, state, noRevision, logIndex);
            return request;
        }

        return new BackgroundWriteRequest(type, partitionId, key, value, revision, expires, lastUsed, lastModified, state, noRevision, logIndex);
    }

    /// <summary>
    /// Returns a fully-consumed request to the pool. Must only be called once the writer has copied the
    /// request into a persistence item and dropped every other reference to it.
    /// </summary>
    public void Return(BackgroundWriteRequest request)
    {
        request.Clear();

        // Racy check-then-increment: the count may briefly exceed the bound under contention, which is
        // harmless — the bound only exists to stop unbounded growth, not to be exact.
        if (Volatile.Read(ref count) >= Volatile.Read(ref capacity))
            return;

        Interlocked.Increment(ref count);
        pool.Enqueue(request);
    }

    /// <summary>
    /// Sets the bound to <paramref name="queued"/>, the number of requests the writer found queued at the
    /// start of a flush tick, clamped to [<see cref="MinCapacity"/>, <see cref="MaxCapacity"/>]. The drain
    /// that follows returns that many, so the pool then holds what the producers rent until the next tick.
    /// A smaller bound trims the pooled excess now: without load nothing rents, and a pool left above the
    /// bound would keep the previous peak for good.
    /// </summary>
    public void SizeToQueued(int queued)
    {
        int bound = Math.Clamp(queued, MinCapacity, MaxCapacity);
        Volatile.Write(ref capacity, bound);

        while (Volatile.Read(ref count) > bound && pool.TryDequeue(out _))
            Interlocked.Decrement(ref count);
    }
}
