
using System.Collections.Concurrent;

using Kommander.Time;

using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Node-local store of persistent-participant completion receipts, keyed by
/// <c>(transactionId, key)</c>. A receipt records that this participant durably committed the
/// persistent value for a transaction on a given key.
/// <para>
/// It exists to answer the one question the in-memory prepare state cannot survive: after a
/// participant's write intent and MVCC entry are gone — including after a leader change that erases
/// them — was a re-delivered commit for that key already applied here? The receipt is recorded at
/// every apply/restore site of the committed value (leader commit, follower replication apply, and
/// log restore on restart/leader change), so a re-commit finds it and answers <c>Committed</c>
/// instead of the ambiguous <c>MustRetry</c>. This is the durable replacement the bounded in-memory
/// recent-commit FIFO cannot provide (the FIFO evicts and never crosses a leader change).
/// </para>
/// <para>
/// Receipts are not evicted by a size bound: dropping a receipt still needed for recovery would
/// reintroduce the ambiguity. They are removed only by <see cref="Forget"/>, which a durable
/// coordinator acknowledgement drives once the decision is safely recorded elsewhere.
/// </para>
/// </summary>
internal sealed class CompletionReceiptStore
{
    private readonly ConcurrentDictionary<ReceiptKey, CompletionReceipt> receipts = new();

    /// <summary>
    /// Records a completion receipt for a committed persistent participant. Idempotent: a replayed
    /// apply (Raft re-delivery, log replay twice) keeps the first receipt rather than overwriting it.
    /// </summary>
    public void Record(HLCTimestamp transactionId, string key, string? recordAnchorKey, KeyValueDurability durability)
    {
        if (transactionId == HLCTimestamp.Zero || string.IsNullOrEmpty(key))
            return;

        receipts.TryAdd(new ReceiptKey(transactionId, key), new CompletionReceipt(recordAnchorKey, durability));
    }

    /// <summary>
    /// True when a completion receipt exists for the transaction's mutation on <paramref name="key"/>.
    /// </summary>
    public bool Contains(HLCTimestamp transactionId, string key)
        => receipts.ContainsKey(new ReceiptKey(transactionId, key));

    /// <summary>
    /// Idempotently forgets a receipt once a durable coordinator acknowledgement confirms the
    /// decision is safely recorded and the participant no longer needs to prove completion. Returns
    /// true when a receipt was removed. Not called until the durable coordinator decision exists.
    /// </summary>
    public bool Forget(HLCTimestamp transactionId, string key)
        => receipts.TryRemove(new ReceiptKey(transactionId, key), out _);

    /// <summary>Current number of retained receipts. Diagnostic only.</summary>
    public int Count => receipts.Count;

    /// <summary>
    /// Snapshot of the current receipts, ordered arbitrarily. Used by whole-partition state transfer
    /// so receipts survive a follower repaired below the WAL compaction floor.
    /// </summary>
    public IReadOnlyCollection<(HLCTimestamp TransactionId, string Key, string? RecordAnchorKey, KeyValueDurability Durability)> Snapshot()
    {
        List<(HLCTimestamp, string, string?, KeyValueDurability)> result = new(receipts.Count);

        foreach (KeyValuePair<ReceiptKey, CompletionReceipt> receipt in receipts)
            result.Add((receipt.Key.TransactionId, receipt.Key.Key, receipt.Value.RecordAnchorKey, receipt.Value.Durability));

        return result;
    }

    /// <summary>Composite receipt key. String keys compare with ordinal semantics (default for string).</summary>
    private readonly record struct ReceiptKey(HLCTimestamp TransactionId, string Key);

    private readonly record struct CompletionReceipt(string? RecordAnchorKey, KeyValueDurability Durability);
}
