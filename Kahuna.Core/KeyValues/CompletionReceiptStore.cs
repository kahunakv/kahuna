
using System.Collections.Concurrent;

using Google.Protobuf;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Replication;
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
/// reintroduce the ambiguity. They are removed either by <see cref="Forget"/>, which a durable
/// coordinator acknowledgement drives once the decision is safely recorded elsewhere, or by the age
/// backstop <see cref="CollectExpired"/> once no re-delivery can still arrive.
/// </para>
/// </summary>
internal sealed class CompletionReceiptStore
{
    private readonly ConcurrentDictionary<ReceiptKey, CompletionReceipt> receipts = new();

    // Monotonic change stamp for the receipt set, bumped whenever a receipt is actually added or removed. A
    // partition's checkpoint snapshot is skipped when the set hasn't changed since that partition's last durable
    // write — the file on disk already reflects exactly this content — which turns the common quiet checkpoint
    // from a full scan + serialize + rewrite into a counter comparison. Routing changes always arrive together
    // with replicated receipt handoffs (which bump the stamp), so a skip can never hide a receipt that moved
    // partitions: until every partition has re-persisted past the handoff, none of them compares equal.
    private long version;

    // Per-partition value of <see cref="version"/> captured just before that partition's last successful
    // snapshot write. Captured before the scan, so a mutation racing the scan leaves the stamp ahead and the
    // next checkpoint rewrites the file.
    private readonly ConcurrentDictionary<int, long> persistedVersion = new();

    /// <summary>
    /// Directory + filename prefix for the per-partition on-disk snapshots, or null when persistence is
    /// disabled (memory-only stores and the no-arg fallback). Each data partition writes the receipts whose key
    /// routes to it into "{prefix}_p{partitionId}.snapshot", so a per-partition WAL checkpoint can gate on the
    /// matching snapshot being durable before it compacts the receipt-bearing log entry.
    /// </summary>
    private readonly string? snapshotDirectory;
    private readonly string? snapshotPrefix;

    private readonly object fileLock = new();

    // Resolves a receipt key to its current owning data partition (RangeRouting.Locate). Attached after
    // construction, once the locator exists; a per-partition snapshot needs it, load does not.
    private Func<string, int>? keyToPartition;

    private readonly ILogger<IKahuna>? logger;

    /// <summary>
    /// Test-only injection point: when set and it returns true for a partition, <see cref="PersistSnapshot"/>
    /// reports failure without writing, so the checkpoint gate must not advance the WAL retention floor.
    /// </summary>
    internal Func<int, bool>? PersistSnapshotFault { get; set; }

    /// <summary>Memory-only store: no durable snapshot. Used by the no-persistence fallback and tests.</summary>
    public CompletionReceiptStore()
    {
    }

    /// <summary>
    /// Durable store: reloads any persisted receipts from disk immediately, and writes the per-partition sets
    /// back on <see cref="PersistSnapshot"/> (driven at checkpoint time, before the WAL retention floor advances).
    /// </summary>
    public CompletionReceiptStore(string? storagePath, string storageRevision, ILogger<IKahuna> logger)
    {
        this.logger = logger;
        if (string.IsNullOrEmpty(storagePath))
        {
            snapshotDirectory = null;
            snapshotPrefix = null;
        }
        else
        {
            snapshotDirectory = storagePath;
            snapshotPrefix = $"completionreceipts_{storageRevision}";
        }

        LoadFromDisk();
    }

    /// <summary>
    /// Wires the receipt-key → data-partition resolver once the locator is constructed. Called during manager
    /// construction, before any checkpoint can run.
    /// </summary>
    public void AttachPartitionResolver(Func<string, int> resolver) => keyToPartition = resolver;

    /// <summary>
    /// Records a completion receipt for a committed persistent participant. Idempotent: a replayed
    /// apply (Raft re-delivery, log replay twice) keeps the first receipt rather than overwriting it.
    /// </summary>
    public void Record(HLCTimestamp transactionId, string key, string? recordAnchorKey, KeyValueDurability durability)
    {
        if (transactionId == HLCTimestamp.Zero || string.IsNullOrEmpty(key))
            return;

        if (receipts.TryAdd(new ReceiptKey(transactionId, key), new CompletionReceipt(recordAnchorKey, durability)))
            Interlocked.Increment(ref version);
    }

    /// <summary>
    /// True when a completion receipt exists for the transaction's mutation on <paramref name="key"/> whose
    /// stored identity matches the request. The full immutable tuple is validated: a receipt proves a specific
    /// <paramref name="durability"/> committed, so a persistent receipt must not satisfy an ephemeral request for
    /// the same logical key. When <paramref name="expectedAnchor"/> is supplied it must also equal the stored
    /// record anchor; callers that do not carry the anchor pass null and validate on transaction/key/durability.
    /// </summary>
    public bool Contains(HLCTimestamp transactionId, string key, KeyValueDurability durability, string? expectedAnchor = null)
    {
        if (!receipts.TryGetValue(new ReceiptKey(transactionId, key), out CompletionReceipt receipt))
            return false;
        if (receipt.Durability != durability)
            return false;
        if (expectedAnchor is not null && !string.Equals(receipt.RecordAnchorKey, expectedAnchor, StringComparison.Ordinal))
            return false;
        return true;
    }

    /// <summary>
    /// Idempotently forgets a receipt once a durable coordinator acknowledgement confirms the
    /// decision is safely recorded and the participant no longer needs to prove completion. Returns
    /// true when a receipt was removed. Not called until the durable coordinator decision exists.
    /// </summary>
    public bool Forget(HLCTimestamp transactionId, string key)
    {
        if (!receipts.TryRemove(new ReceiptKey(transactionId, key), out _))
            return false;

        Interlocked.Increment(ref version);
        return true;
    }

    /// <summary>
    /// Drops every receipt older than <paramref name="ttl"/> and returns how many were removed. This is the age
    /// backstop that bounds the store independently of the coordinator-driven <see cref="Forget"/> path.
    /// <para>
    /// A receipt can outlive the transaction whose completion it proves: every replay of a committed persistent
    /// mutation re-records one — on a follower applying the log, and on a cold restart or partition leader change
    /// replaying the retained log — including for transactions whose canonical record was already reclaimed. Those
    /// receipts have no owner left to release them, so without an age bound they accumulate for the node's
    /// lifetime. Sweeping by age reclaims them, and needs no replication: a receipt is node-local state that every
    /// replica derives from the same log, so each node ages out its own copy and they converge without a log entry.
    /// </para>
    /// <para>
    /// Age is measured from the receipt's transaction id — the HLC minted when the transaction began, which is
    /// necessarily earlier than the commit the receipt attests to. The measured age therefore <i>overstates</i> the
    /// true age by at most one transaction's lifetime (itself bounded by the decision deadline), so
    /// <paramref name="ttl"/> must exceed the transaction-record retention window by a comfortable margin for this
    /// to stay a backstop rather than preempt the ordinary release.
    /// </para>
    /// </summary>
    public int CollectExpired(HLCTimestamp now, TimeSpan ttl)
    {
        if (ttl <= TimeSpan.Zero || now == HLCTimestamp.Zero)
            return 0;

        int removed = 0;

        foreach (KeyValuePair<ReceiptKey, CompletionReceipt> receipt in receipts)
        {
            if (now - receipt.Key.TransactionId < ttl)
                continue;

            // Remove only if the value is still the one just observed, so a receipt re-recorded concurrently
            // under the same key is left for the next pass rather than dropped on stale information.
            if (receipts.TryRemove(receipt))
                removed++;
        }

        if (removed > 0)
            Interlocked.Increment(ref version);

        return removed;
    }

    /// <summary>Current number of retained receipts. Diagnostic only.</summary>
    public int Count => receipts.Count;

    /// <summary>Records a batch of receipts (used by split/merge routing on the destination leader).</summary>
    public void ImportRange(IEnumerable<CompletionReceiptRecord> records)
    {
        foreach (CompletionReceiptRecord record in records)
            Record(record.TransactionId, record.Key, record.RecordAnchorKey, record.Durability);
    }

    /// <summary>
    /// Serializes a batch of receipts as one replicated log entry, either to <b>record</b> them on a destination
    /// partition (split/merge handoff) or to <b>forget</b> them on a participant partition (a durable coordinator
    /// acknowledgement releasing the proof on every replica). The wire type is shared with the on-disk snapshot
    /// container; <paramref name="destinationPartitionId"/> is carried so the apply side knows routing is resolved,
    /// and <paramref name="forget"/> selects record vs. remove.
    /// </summary>
    public static byte[] SerializeImport(IReadOnlyCollection<CompletionReceiptRecord> records, int destinationPartitionId, bool forget = false)
    {
        GrpcImportCompletionReceiptsRequest message = new() { DestinationPartitionId = destinationPartitionId, Forget = forget };
        foreach (CompletionReceiptRecord record in records)
        {
            GrpcCompletionReceiptEntry entry = new()
            {
                TransactionIdNode     = record.TransactionId.N,
                TransactionIdPhysical = record.TransactionId.L,
                TransactionIdCounter  = record.TransactionId.C,
                Key                   = record.Key,
                Durability            = (int)record.Durability,
            };
            if (record.RecordAnchorKey is not null)
                entry.RecordAnchorKey = record.RecordAnchorKey;
            message.Receipts.Add(entry);
        }
        return message.ToByteArray();
    }

    /// <summary>Applies a batch of receipts replayed from a WAL restore of a split/merge handoff entry.</summary>
    public bool Restore(int partitionId, RaftLog log) => Apply(partitionId, log);

    /// <summary>Applies a batch of receipts committed to a destination partition's Raft log (follower / leader echo).</summary>
    public bool Replicate(int partitionId, RaftLog log) => Apply(partitionId, log);

    // Applies a receipt batch committed to a partition's Raft log: records each receipt (split/merge handoff) or
    // forgets each (durable-acknowledgement release), on every replica. Idempotent: Record keeps the first
    // receipt and Forget of an absent receipt is a no-op, so a re-delivery or a replay after restart converges.
    private bool Apply(int partitionId, RaftLog log)
    {
        if (log.LogType != ReplicationTypes.CompletionReceipt || log.LogData is null)
            return true;

        try
        {
            // Stream-decode the batch instead of materializing the generated message tree: on a busy durable
            // workload this apply runs on every replica for every receipt record/forget, and ParseFrom would
            // allocate the container, the repeated-field array, and one GrpcCompletionReceiptEntry per receipt
            // only to read six scalars and throw them away. Reading fields straight off a CodedInputStream keeps
            // the strings that become dictionary keys and drops the rest of the garbage.
            //
            // Field numbers are pinned to GrpcImportCompletionReceiptsRequest / GrpcCompletionReceiptEntry in
            // keyvalues.proto; if that message changes, this decoder must change with it.
            CodedInputStream input = new(log.LogData);

            bool forget = false;
            List<PendingReceipt> pending = new();

            uint tag;
            while ((tag = input.ReadTag()) != 0)
            {
                switch (WireFormat.GetTagFieldNumber(tag))
                {
                    case 1: // repeated GrpcCompletionReceiptEntry Receipts
                        pending.Add(ParseReceiptEntry(input));
                        break;
                    case 2: // int32 DestinationPartitionId — routing already resolved; irrelevant on apply
                        input.ReadInt32();
                        break;
                    case 3: // bool Forget — record vs. remove; wire order is not guaranteed, hence the buffer
                        forget = input.ReadBool();
                        break;
                    default:
                        input.SkipLastField();
                        break;
                }
            }

            foreach (PendingReceipt receipt in pending)
            {
                if (forget)
                    Forget(receipt.TransactionId, receipt.Key);
                else
                    Record(receipt.TransactionId, receipt.Key, receipt.RecordAnchorKey, receipt.Durability);
            }

            return true;
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "Failed to apply completion-receipt batch on partition {Partition}", partitionId);
            return false;
        }
    }

    // Reads one GrpcCompletionReceiptEntry off the stream into a value tuple, without allocating the generated
    // message object. An absent RecordAnchorKey (field 5) stays null, matching HasRecordAnchorKey == false.
    private static PendingReceipt ParseReceiptEntry(CodedInputStream input)
    {
        // The entry is a length-delimited sub-message; bound the read by its byte length rather than the
        // internal PushLimit helper (not part of the public surface in this protobuf version).
        long end = input.Position + input.ReadLength();

        int node = 0;
        long physical = 0;
        uint counter = 0;
        string key = string.Empty;
        string? recordAnchorKey = null;
        int durability = 0;

        while (input.Position < end)
        {
            uint tag = input.ReadTag();
            switch (WireFormat.GetTagFieldNumber(tag))
            {
                case 1: node = input.ReadInt32(); break;            // TransactionIdNode
                case 2: physical = input.ReadInt64(); break;        // TransactionIdPhysical
                case 3: counter = input.ReadUInt32(); break;        // TransactionIdCounter
                case 4: key = input.ReadString(); break;            // Key
                case 5: recordAnchorKey = input.ReadString(); break; // RecordAnchorKey (optional)
                case 6: durability = input.ReadInt32(); break;      // Durability
                default: input.SkipLastField(); break;
            }
        }

        return new PendingReceipt(new HLCTimestamp(node, physical, counter), key, recordAnchorKey, (KeyValueDurability)durability);
    }

    /// <summary>
    /// Applies a receipt-handoff batch through the follower/restore apply path, for tests that assert the
    /// handoff reaches every replica. Mirrors exactly what a replicated receipt entry does on a follower.
    /// </summary>
    internal bool ApplyImportForTest(int partitionId, IReadOnlyCollection<CompletionReceiptRecord> records) =>
        Replicate(partitionId, new RaftLog
        {
            LogType = ReplicationTypes.CompletionReceipt,
            LogData = SerializeImport(records, partitionId)
        });

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
    /// Writes the receipts whose key routes to <paramref name="partitionId"/> to that partition's on-disk
    /// snapshot (atomic tmp-then-move) and reports whether the write is durable. Called at checkpoint time —
    /// after every dirty key-value write has flushed and before the partition's WAL retention floor advances —
    /// so a receipt whose committed log entry is about to be compacted is captured durably; only on a
    /// <c>true</c> return may the checkpoint proceed. A no-op returning <c>true</c> when persistence is disabled
    /// or the resolver is not yet attached. An over-inclusive snapshot is safe: a stale receipt only ever answers
    /// <c>Committed</c> for a value that genuinely committed, and recovery forgets it when the decision reconciles.
    /// </summary>
    public bool PersistSnapshot(int partitionId)
    {
        if (snapshotDirectory is null || snapshotPrefix is null)
            return true;

        if (PersistSnapshotFault is not null && PersistSnapshotFault(partitionId))
            return false;

        if (keyToPartition is null)
            return true;

        // Unchanged since this partition's last durable write: the file already holds exactly this content, so
        // the checkpoint may proceed without scanning or rewriting anything. The stamp is captured before the
        // scan and recorded only after a successful write, so a faulted write or a mutation racing the scan
        // always leaves the partition due for a rewrite.
        long observedVersion = Interlocked.Read(ref version);
        if (persistedVersion.TryGetValue(partitionId, out long lastPersisted) && lastPersisted == observedVersion)
            return true;

        GrpcImportCompletionReceiptsRequest message = new();
        foreach (KeyValuePair<ReceiptKey, CompletionReceipt> receipt in receipts)
        {
            if (keyToPartition(receipt.Key.Key) != partitionId)
                continue;

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

        string path = Path.Combine(snapshotDirectory, $"{snapshotPrefix}_p{partitionId}.snapshot");

        try
        {
            byte[] data = message.ToByteArray();
            lock (fileLock)
            {
                string tmp = path + ".tmp";
                File.WriteAllBytes(tmp, data);
                File.Move(tmp, path, overwrite: true);
            }

            persistedVersion[partitionId] = observedVersion;
            return true;
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "Failed to persist completion-receipt snapshot to {Path}", path);
            return false;
        }
    }

    // Loads every per-partition snapshot present in the storage directory. A snapshot file that exists but
    // cannot be parsed is corruption the receipt set cannot silently recover from — a below-floor receipt may be
    // gone from the WAL — so it fails closed (throws) rather than starting empty and losing proof of a commit.
    private void LoadFromDisk()
    {
        if (snapshotDirectory is null || snapshotPrefix is null || !Directory.Exists(snapshotDirectory))
            return;

        string[] files;
        lock (fileLock)
            files = Directory.GetFiles(snapshotDirectory, $"{snapshotPrefix}_p*.snapshot");

        foreach (string path in files)
        {
            byte[] data;
            try
            {
                lock (fileLock)
                    data = File.ReadAllBytes(path);
            }
            catch (Exception ex)
            {
                throw new IOException($"Failed to read completion-receipt snapshot {path}; refusing to start with a possibly incomplete receipt set", ex);
            }

            GrpcImportCompletionReceiptsRequest message;
            try
            {
                message = GrpcImportCompletionReceiptsRequest.Parser.ParseFrom(data);
            }
            catch (Exception ex)
            {
                throw new InvalidDataException($"Corrupt completion-receipt snapshot {path}; refusing to start empty and lose proof of a commit", ex);
            }

            foreach (GrpcCompletionReceiptEntry entry in message.Receipts)
                Record(
                    new HLCTimestamp(entry.TransactionIdNode, entry.TransactionIdPhysical, entry.TransactionIdCounter),
                    entry.Key,
                    entry.HasRecordAnchorKey ? entry.RecordAnchorKey : null,
                    (KeyValueDurability)entry.Durability);
        }
    }

    /// <summary>Composite receipt key. String keys compare with ordinal semantics (default for string).</summary>
    private readonly record struct ReceiptKey(HLCTimestamp TransactionId, string Key);

    private readonly record struct CompletionReceipt(string? RecordAnchorKey, KeyValueDurability Durability);

    // A receipt decoded from a replicated batch, buffered until the batch's Forget flag is known so the
    // record-vs-remove action can be applied regardless of the flag's wire position.
    private readonly record struct PendingReceipt(
        HLCTimestamp TransactionId,
        string Key,
        string? RecordAnchorKey,
        KeyValueDurability Durability);
}

/// <summary>A transferable completion receipt: the full tuple used when routing receipts across split/merge.</summary>
public readonly record struct CompletionReceiptRecord(
    HLCTimestamp TransactionId,
    string Key,
    string? RecordAnchorKey,
    KeyValueDurability Durability);
