using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Carries the outcome of a durable record/intent apply between the consumer apply that Raft drives and the write
/// scheduler's completion for the same log entry, so the entry's delta is deserialized and applied once instead of
/// twice.
///
/// <para>On the leader every locally produced durable entry is applied twice: Raft delivers the committed entry to
/// the consumer, and the scheduler's completion applies the very same delta again because the producer's prepare
/// acknowledgement is that apply's result. The transitions are idempotent so the second apply changes nothing — it
/// only costs a full deserialization. Either side can run first (the fast-path proposal-ticket release lets the
/// completion overtake the consumer apply), so whichever applies records its result and the other consumes it
/// instead of re-applying.</para>
///
/// <para>A miss always means "apply it yourself". Nothing here can cause a missed apply — pruning, a lost race where
/// both sides apply, and a restart all degrade to the original double apply, never to a lost one. That is what makes
/// the ledger safe to keep bounded and lock-free.</para>
/// </summary>
internal sealed class DurableApplyResultLedger
{
    // Results are consumed almost immediately, so a window this size is far larger than the real in-flight depth. It
    // exists only to bound the residue left when a completion never claims its result (it applied first, or its
    // batch was released), which would otherwise accumulate for the process's lifetime.
    private const int WindowSize = 1024;

    // One ring per partition, addressed by log index modulo the window, so recording is a single slot write and the
    // window is bounded by construction: entry N silently displaces entry N-1024, which is the same residue bound the
    // window has always had. Nothing is allocated per entry, and there is no scan to drop stale results.
    private readonly ConcurrentDictionary<int, long[]> partitions = new();

    /// <summary>Records what the consumer apply of this entry produced: the prepare acknowledgement for an intent
    /// delta, or the apply result for a record delta. A non-positive index carries no entry identity.</summary>
    public void RecordApplied(int partitionId, long logIndex, bool result)
    {
        if (logIndex <= 0)
            return;

        long[] ring = partitions.TryGetValue(partitionId, out long[]? existing)
            ? existing
            : partitions.GetOrAdd(partitionId, static _ => new long[WindowSize]);

        Volatile.Write(ref ring[Slot(logIndex)], Encode(logIndex, result));
    }

    /// <summary>Takes the recorded result for an entry, meaning its apply already happened and must not be repeated.
    /// False means no result is available and the caller must apply the entry itself.</summary>
    public bool TryConsume(int partitionId, long logIndex, out bool result)
    {
        result = false;

        if (logIndex <= 0 || !partitions.TryGetValue(partitionId, out long[]? ring))
            return false;

        ref long slot = ref ring[Slot(logIndex)];

        // Claim the slot only if it still holds this exact entry: a displaced or already consumed slot reads as some
        // other index and the caller applies for itself. Clearing it makes the take single-shot under concurrency.
        long expected = Encode(logIndex, true);
        long observed = Interlocked.CompareExchange(ref slot, 0, expected);

        if (observed != expected)
        {
            expected = Encode(logIndex, false);

            if (Interlocked.CompareExchange(ref slot, 0, expected) != expected)
                return false;
        }
        else
            result = true;

        DurableTransactionMetrics.RedundantApplySkipped();
        return true;
    }

    private static int Slot(long logIndex) => (int)((ulong)logIndex % WindowSize);

    // The index identifies the entry occupying the slot and the low bit carries its result, so a slot is claimed and
    // read in one atomic word. Zero is never a valid encoding, which makes it the empty/consumed marker.
    private static long Encode(long logIndex, bool result) => (logIndex << 1) | (result ? 1L : 0L);
}
