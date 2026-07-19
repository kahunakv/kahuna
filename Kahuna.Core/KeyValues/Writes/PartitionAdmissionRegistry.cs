using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Bounds the items and serialized bytes admitted per partition — including those still waiting behind an
/// in-flight batch — so a burst cannot retain unbounded memory in lane inboxes before the lane accounts for
/// it. The facade reserves synchronously on admission (rejecting with retryable backpressure when full); the
/// lane releases per item as batches complete. Accounting only — ordering and lifecycle stay lane-owned.
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

    public PartitionAdmissionRegistry(int maxItems, long maxBytes)
    {
        this.maxItems = maxItems;
        this.maxBytes = maxBytes;
    }

    /// <summary>Reserves one item and <paramref name="bytes"/> for the partition, or returns false if that
    /// would exceed either cap. An item larger than the whole per-partition byte cap is admitted only when the
    /// partition is otherwise empty — so a single legitimately-large value proceeds (dispatched alone) instead
    /// of being rejected forever — while concurrent oversized items are still bounded to one. A rejection
    /// reserves nothing.</summary>
    public bool TryReserve(int partitionId, int bytes)
    {
        while (true)
        {
            Counters counters = byPartition.GetOrAdd(partitionId, static _ => new Counters());

            lock (counters)
            {
                // Retired between GetOrAdd and the lock (a concurrent Release pruned it at zero); fetch again.
                if (counters.Removed)
                    continue;

                if (bytes > maxBytes)
                {
                    // Oversized: admit only into an empty partition, then it occupies the partition alone.
                    if (counters.Items != 0 || counters.Bytes != 0)
                        return false;
                }
                else if (counters.Items + 1 > maxItems || counters.Bytes + bytes > maxBytes)
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
    /// for the node's lifetime.</summary>
    public void Release(int partitionId, int bytes)
    {
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
        long total = 0;
        foreach (Counters c in byPartition.Values)
            lock (c)
                total += c.Items;
        return total;
    }

    /// <summary>Sum of currently reserved bytes across all partitions (observable-gauge callback).</summary>
    public long TotalReservedBytes()
    {
        long total = 0;
        foreach (Counters c in byPartition.Values)
            lock (c)
                total += c.Bytes;
        return total;
    }

    private int inFlightPartitions;

    public void IncInFlight() => Interlocked.Increment(ref inFlightPartitions);

    public void DecInFlight() => Interlocked.Decrement(ref inFlightPartitions);

    public int InFlightPartitions => Volatile.Read(ref inFlightPartitions);
}
