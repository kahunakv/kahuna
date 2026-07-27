using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Carries the outcome of a durable record/intent apply from the consumer apply that Raft drives to the write
/// scheduler's completion for the same log entry, so the entry's delta is deserialized and applied once instead of
/// twice.
///
/// <para>On the leader every locally produced durable entry is applied twice: Raft delivers the committed entry to
/// the consumer, and the scheduler's completion applies the very same delta again because the producer's prepare
/// acknowledgement is that apply's result. The transitions are idempotent so the second apply changes nothing — it
/// only costs a full deserialization. The consumer apply runs first (the commit path applies to the consumer after
/// releasing the proposal ticket but before the scheduler's completion can run), so it records what it applied and
/// the completion consumes that result instead of re-applying.</para>
///
/// <para>A miss always means "apply it yourself". Nothing here can cause a missed apply — pruning, a completion that
/// somehow overtakes the consumer, and a restart all degrade to the original double apply, never to a lost one. That
/// is what makes the ledger safe to keep bounded and lock-free.</para>
/// </summary>
internal sealed class DurableApplyResultLedger
{
    // Results are consumed almost immediately, so a window this size is far larger than the real in-flight depth. It
    // exists only to bound the residue left when a completion never claims its result (it applied first, or its
    // batch was released), which would otherwise accumulate for the process's lifetime.
    private const int MaxRecords = 1024;

    private readonly ConcurrentDictionary<(int PartitionId, long LogIndex), bool> results = new();

    /// <summary>Records what the consumer apply of this entry produced: the prepare acknowledgement for an intent
    /// delta, or the apply result for a record delta. A non-positive index carries no entry identity.</summary>
    public void RecordApplied(int partitionId, long logIndex, bool result)
    {
        if (logIndex <= 0)
            return;

        results[(partitionId, logIndex)] = result;

        if (results.Count > MaxRecords)
            PruneBelow(partitionId, logIndex - MaxRecords);
    }

    /// <summary>Takes the recorded result for an entry, meaning its apply already happened and must not be repeated.
    /// False means no result is available and the caller must apply the entry itself.</summary>
    public bool TryConsume(int partitionId, long logIndex, out bool result)
    {
        if (logIndex > 0 && results.TryRemove((partitionId, logIndex), out result))
        {
            DurableTransactionMetrics.RedundantApplySkipped();
            return true;
        }

        result = false;
        return false;
    }

    // Entries are committed and applied in ascending index order, so a result this far behind the newest one was
    // never going to be claimed. Dropping it only means its (already completed) submission applies for itself.
    private void PruneBelow(int partitionId, long logIndex)
    {
        foreach ((int PartitionId, long LogIndex) key in results.Keys)
        {
            if (key.PartitionId == partitionId && key.LogIndex <= logIndex)
                results.TryRemove(key, out _);
        }
    }
}
