using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Bounds the items and serialized bytes admitted to the shared write scheduler, both <b>per partition</b> and
/// <b>node-globally</b> — including work still waiting behind an in-flight batch — so a burst cannot retain
/// unbounded memory in lane inboxes before the lanes account for it. The facade reserves synchronously on
/// admission (rejecting with retryable backpressure when full); the lane releases per item as batches complete.
///
/// <para>A <see cref="WriteAdmissionClass.Terminal"/> submission may draw on reserve headroom above the base
/// item/byte budget that an <see cref="WriteAdmissionClass.Ordinary"/> submission can never touch — at both the
/// per-partition and global levels. Because ordinary admission can never push usage past the base budget, the
/// full reserve always remains available for the decision/settle that finishes an already-prepared transaction,
/// even when ordinary writes have saturated the partition or the node. Accounting only — ordering and lifecycle
/// stay lane-owned.</para>
/// </summary>
internal sealed class PartitionAdmissionRegistry
{
    private sealed class Counters
    {
        public int Items;
        public long Bytes;
        // Set under the lock when this object is retired from the dictionary at zero, so a concurrent
        // TryReserve that already fetched it detects the retirement and fetches a fresh one.
        public bool Removed;
    }

    private readonly ConcurrentDictionary<int, Counters> byPartition = new();

    private readonly int maxItems;

    private readonly long maxBytes;

    private readonly int reserveItems;

    private readonly long reserveBytes;

    private readonly long maxItemsGlobal;

    private readonly long maxBytesGlobal;

    private readonly long reserveItemsGlobal;

    private readonly long reserveBytesGlobal;

    private readonly long maxOperationBytes;

    // Node-global totals across every partition, guarded by their own lock. A reservation takes this lock first
    // and the per-partition lock second (consistent order everywhere), so the two accounts never deadlock.
    private readonly object globalLock = new();

    private long globalItems;

    private long globalBytes;

    public PartitionAdmissionRegistry(
        int maxItems,
        long maxBytes,
        int reserveItems = 0,
        long reserveBytes = 0,
        long maxItemsGlobal = 0,
        long maxBytesGlobal = 0,
        long reserveItemsGlobal = 0,
        long reserveBytesGlobal = 0,
        long maxOperationBytes = 0)
    {
        this.maxItems = maxItems;
        this.maxBytes = maxBytes;
        this.reserveItems = Math.Max(0, reserveItems);
        this.reserveBytes = Math.Max(0, reserveBytes);
        this.maxItemsGlobal = maxItemsGlobal;
        this.maxBytesGlobal = maxBytesGlobal;
        this.reserveItemsGlobal = Math.Max(0, reserveItemsGlobal);
        this.reserveBytesGlobal = Math.Max(0, reserveBytesGlobal);
        this.maxOperationBytes = maxOperationBytes;
    }

    /// <summary>
    /// Reserves one item and <paramref name="bytes"/> for the partition, or returns false if that would exceed a
    /// per-partition or node-global cap (the class-appropriate cap: ordinary uses the base budget, terminal may
    /// use base + reserve). A submission larger than <see cref="maxOperationBytes"/> is rejected outright — the
    /// hard per-operation ceiling — rather than admitted alone. Below that ceiling, an item larger than the whole
    /// per-partition byte budget is admitted only when the partition is otherwise empty, so a single legitimately
    /// large value proceeds (dispatched alone) while concurrent oversized items stay bounded to one. A rejection
    /// reserves nothing on either account.
    /// </summary>
    public bool TryReserve(int partitionId, int bytes, WriteAdmissionClass cls)
    {
        // Hard per-operation ceiling: a pathological value is rejected, never dispatched alone.
        if (maxOperationBytes > 0 && bytes > maxOperationBytes)
            return false;

        // Node-global account first. Ordinary is bounded to the base budget; terminal may use base + reserve, so
        // the reserve headroom is always available to settlement no matter how much ordinary work is outstanding.
        if (!TryReserveGlobal(bytes, cls))
            return false;

        if (!TryReservePartition(partitionId, bytes, cls))
        {
            ReleaseGlobal(bytes);
            return false;
        }

        return true;
    }

    private bool TryReserveGlobal(int bytes, WriteAdmissionClass cls)
    {
        long itemCap = maxItemsGlobal <= 0 ? long.MaxValue
            : cls == WriteAdmissionClass.Terminal ? maxItemsGlobal + reserveItemsGlobal : maxItemsGlobal;
        long byteCap = maxBytesGlobal <= 0 ? long.MaxValue
            : cls == WriteAdmissionClass.Terminal ? maxBytesGlobal + reserveBytesGlobal : maxBytesGlobal;

        lock (globalLock)
        {
            if (globalItems + 1 > itemCap || globalBytes + bytes > byteCap)
                return false;

            globalItems++;
            globalBytes += bytes;
            return true;
        }
    }

    private void ReleaseGlobal(int bytes)
    {
        lock (globalLock)
        {
            globalItems--;
            globalBytes -= bytes;
        }
    }

    private bool TryReservePartition(int partitionId, int bytes, WriteAdmissionClass cls)
    {
        int itemCap = cls == WriteAdmissionClass.Terminal ? maxItems + reserveItems : maxItems;
        long byteCap = cls == WriteAdmissionClass.Terminal ? maxBytes + reserveBytes : maxBytes;

        while (true)
        {
            Counters counters = byPartition.GetOrAdd(partitionId, static _ => new Counters());

            lock (counters)
            {
                // Retired between GetOrAdd and the lock (a concurrent Release pruned it at zero); fetch again.
                if (counters.Removed)
                    continue;

                if (bytes > byteCap)
                {
                    // Oversized (but within the hard ceiling): admit only into an empty partition, then it
                    // occupies the partition alone.
                    if (counters.Items != 0 || counters.Bytes != 0)
                        return false;
                }
                else if (counters.Items + 1 > itemCap || counters.Bytes + bytes > byteCap)
                {
                    return false;
                }

                counters.Items++;
                counters.Bytes += bytes;
                return true;
            }
        }
    }

    /// <summary>Releases one item and <paramref name="bytes"/> previously reserved for the partition, pruning
    /// the partition's counters once both reach zero so historical/split-churned partitions do not accumulate
    /// for the node's lifetime. Also decrements the node-global account. Release is class-agnostic: the caps are
    /// enforced at reserve time against the running totals, so an ordinary and a terminal release are identical.</summary>
    public void Release(int partitionId, int bytes)
    {
        ReleaseGlobal(bytes);

        if (!byPartition.TryGetValue(partitionId, out Counters? counters))
            return;

        lock (counters)
        {
            counters.Items--;
            counters.Bytes -= bytes;

            if (counters.Items <= 0 && counters.Bytes <= 0)
            {
                counters.Removed = true;
                // Remove only if the dictionary still maps to this exact object (never a replacement).
                byPartition.TryRemove(new KeyValuePair<int, Counters>(partitionId, counters));
            }
        }
    }

    /// <summary>Number of partitions currently tracked; returns to zero once every reservation is released
    /// (test/observability).</summary>
    public int TrackedPartitionCount => byPartition.Count;

    /// <summary>Currently reserved items for the partition (test/observability).</summary>
    public int ReservedItems(int partitionId) =>
        byPartition.TryGetValue(partitionId, out Counters? c) ? Volatile.Read(ref c.Items) : 0;

    /// <summary>Sum of currently reserved items across all partitions (observable-gauge callback).</summary>
    public long TotalReservedItems()
    {
        lock (globalLock)
            return globalItems;
    }

    /// <summary>Sum of currently reserved bytes across all partitions (observable-gauge callback).</summary>
    public long TotalReservedBytes()
    {
        lock (globalLock)
            return globalBytes;
    }

    private int inFlightPartitions;

    public void IncInFlight() => Interlocked.Increment(ref inFlightPartitions);

    public void DecInFlight() => Interlocked.Decrement(ref inFlightPartitions);

    public int InFlightPartitions => Volatile.Read(ref inFlightPartitions);
}
