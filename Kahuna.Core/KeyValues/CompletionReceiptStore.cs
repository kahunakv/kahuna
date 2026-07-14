
using System.Collections.Concurrent;

using Google.Protobuf;
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
    /// On-disk snapshot path, or null when persistence is disabled (memory-only stores and the
    /// no-arg fallback). Holds the full receipt set so a receipt survives a cold restart even when
    /// its committed log entry has been compacted past the WAL retention floor.
    /// </summary>
    private readonly string? snapshotPath;

    private readonly ILogger<IKahuna>? logger;

    /// <summary>Memory-only store: no durable snapshot. Used by the no-persistence fallback and tests.</summary>
    public CompletionReceiptStore()
    {
    }

    /// <summary>
    /// Durable store: reloads any persisted receipts from disk immediately, and writes the full set back
    /// on <see cref="PersistSnapshot"/> (driven at checkpoint time, before the WAL retention floor advances).
    /// </summary>
    public CompletionReceiptStore(string? storagePath, string storageRevision, ILogger<IKahuna> logger)
    {
        this.logger = logger;
        snapshotPath = string.IsNullOrEmpty(storagePath)
            ? null
            : Path.Combine(storagePath, $"completionreceipts_{storageRevision}.snapshot");

        LoadFromDisk();
    }

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

    /// <summary>Records a batch of receipts (used by split/merge routing on the destination leader).</summary>
    public void ImportRange(IEnumerable<CompletionReceiptRecord> records)
    {
        foreach (CompletionReceiptRecord record in records)
            Record(record.TransactionId, record.Key, record.RecordAnchorKey, record.Durability);
    }

    /// <summary>
    /// Snapshot of the receipts whose key falls in <c>[startKey, endKey)</c> (ordinal), or all receipts
    /// when both bounds are null. Used to route receipts by key to a destination partition during a
    /// split or merge, mirroring the range-lock transfer.
    /// </summary>
    public IReadOnlyCollection<CompletionReceiptRecord> SnapshotRange(string? startKey, string? endKey)
    {
        List<CompletionReceiptRecord> result = new();

        foreach (KeyValuePair<ReceiptKey, CompletionReceipt> receipt in receipts)
        {
            string key = receipt.Key.Key;

            if (startKey is not null && string.CompareOrdinal(key, startKey) < 0)
                continue;
            if (endKey is not null && string.CompareOrdinal(key, endKey) >= 0)
                continue;

            result.Add(new CompletionReceiptRecord(receipt.Key.TransactionId, key, receipt.Value.RecordAnchorKey, receipt.Value.Durability));
        }

        return result;
    }

    /// <summary>
    /// Writes the full current receipt set to the on-disk snapshot (atomic tmp-then-move). A no-op when
    /// persistence is disabled. Called at checkpoint time — after every dirty key-value write has flushed
    /// and before the WAL retention floor advances — so a receipt whose committed log entry is about to be
    /// compacted is captured durably. An over-inclusive snapshot is safe: a stale receipt only ever answers
    /// <c>Committed</c> for a value that genuinely committed, and recovery forgets it when the decision
    /// reconciles.
    /// </summary>
    public void PersistSnapshot()
    {
        if (snapshotPath is null)
            return;

        GrpcImportCompletionReceiptsRequest message = new();
        foreach (KeyValuePair<ReceiptKey, CompletionReceipt> receipt in receipts)
        {
            GrpcCompletionReceiptEntry entry = new()
            {
                TransactionIdNode     = receipt.Key.TransactionId.N,
                TransactionIdPhysical = receipt.Key.TransactionId.L,
                TransactionIdCounter  = receipt.Key.TransactionId.C,
                Key                   = receipt.Key.Key,
                Durability            = (int)receipt.Value.Durability,
            };
            if (receipt.Value.RecordAnchorKey is not null)
                entry.RecordAnchorKey = receipt.Value.RecordAnchorKey;

            message.Receipts.Add(entry);
        }

        try
        {
            byte[] data = message.ToByteArray();
            string tmp = snapshotPath + ".tmp";
            File.WriteAllBytes(tmp, data);
            File.Move(tmp, snapshotPath, overwrite: true);
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "Failed to persist completion-receipt snapshot to {Path}", snapshotPath);
        }
    }

    private void LoadFromDisk()
    {
        if (snapshotPath is null || !File.Exists(snapshotPath))
            return;

        try
        {
            byte[] data = File.ReadAllBytes(snapshotPath);
            GrpcImportCompletionReceiptsRequest message = GrpcImportCompletionReceiptsRequest.Parser.ParseFrom(data);
            foreach (GrpcCompletionReceiptEntry entry in message.Receipts)
                Record(
                    new HLCTimestamp(entry.TransactionIdNode, entry.TransactionIdPhysical, entry.TransactionIdCounter),
                    entry.Key,
                    entry.HasRecordAnchorKey ? entry.RecordAnchorKey : null,
                    (KeyValueDurability)entry.Durability);
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "Failed to load completion-receipt snapshot from {Path}", snapshotPath);
        }
    }

    /// <summary>Composite receipt key. String keys compare with ordinal semantics (default for string).</summary>
    private readonly record struct ReceiptKey(HLCTimestamp TransactionId, string Key);

    private readonly record struct CompletionReceipt(string? RecordAnchorKey, KeyValueDurability Durability);
}

/// <summary>A transferable completion receipt: the full tuple used when routing receipts across split/merge.</summary>
public readonly record struct CompletionReceiptRecord(
    HLCTimestamp TransactionId,
    string Key,
    string? RecordAnchorKey,
    KeyValueDurability Durability);
