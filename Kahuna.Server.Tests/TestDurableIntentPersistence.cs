using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Durable-persistence and state-transfer tests for the durable-intent 2PC stores: a per-partition on-disk
/// snapshot reloads into an identical record/intent set on a fresh store (the cold-restart path), and the
/// split/merge transfer primitives (SnapshotRange → serialize → deserialize → import) reconstruct the moved set.
/// </summary>
public sealed class TestDurableIntentPersistence : IDisposable
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private const int PartitionId = 7;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-durable-" + Guid.NewGuid().ToString("N"));

    public TestDurableIntentPersistence() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
    }

    // ── transaction record store ────────────────────────────────────────────────

    private static (InitializeTransactionCommand, CommitTransactionCommand) CommittedTxn(long txn, long epoch, string anchor)
    {
        IReadOnlyList<TransactionParticipantRef> manifest = [new(anchor, KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(Ts(txn), epoch, anchor, Ts(txn + 100), manifest);
        InitializeTransactionCommand init = new(Ts(txn), epoch, "coord", anchor, Ts(txn + 100), Ts(txn + 9000), hash, manifest, Ts(txn + 5), Ts(txn));
        CommitTransactionCommand commit = new(Ts(txn), epoch, hash, Ts(txn + 50), Ts(txn + 50));
        return (init, commit);
    }

    [Fact]
    public void RecordStore_SnapshotSurvivesColdRestart()
    {
        (InitializeTransactionCommand init, CommitTransactionCommand commit) = CommittedTxn(1000, 2, "acct/1");

        TransactionRecordStore store = new(dir, "rev", null);
        store.AttachAnchorResolver(_ => (PartitionId, 0));
        store.Apply(init);
        store.Apply(commit);
        Assert.True(store.PersistSnapshot(PartitionId));

        // Fresh store over the same directory reconstructs the committed record.
        TransactionRecordStore reloaded = new(dir, "rev", null);
        TransactionRecord? rec = reloaded.Get(Ts(1000), 2);
        Assert.NotNull(rec);
        Assert.Equal(TransactionDecision.Commit, rec!.Decision);
        Assert.Equal(Ts(1050), rec.WinningOpId);
        Assert.Equal("acct/1", rec.RecordAnchorKey);
    }

    [Fact]
    public void RecordStore_StateTransfer_RoundTrips()
    {
        (InitializeTransactionCommand init, CommitTransactionCommand commit) = CommittedTxn(1000, 1, "m/5");

        TransactionRecordStore source = new();
        source.Apply(init);
        source.Apply(commit);

        // Export the records whose anchor is in [m, n), serialize, deserialize, import into a fresh store.
        IReadOnlyList<TransactionRecord> moved = source.SnapshotRange("m", "n");
        Assert.Single(moved);
        byte[] blob = TransactionRecordStore.SerializeRecords(moved);
        IReadOnlyList<TransactionRecord> decoded = TransactionRecordStore.DeserializeRecords(blob);

        TransactionRecordStore destination = new();
        destination.ImportRecords(decoded);

        Assert.Equal(TransactionDecision.Commit, destination.Get(Ts(1000), 1)!.Decision);
    }

    // ── prepared intent store ────────────────────────────────────────────────────

    private static PreparedIntent Intent(long txn, long epoch, string key) =>
        new(Ts(txn), epoch, key, ManifestHash: 4242, RecordAnchorKey: "anchor", CommitTimestamp: Ts(txn + 100),
            State: KeyValueState.Set, Value: [7, 8, 9], Bucket: "b", Revision: 5, Expires: Ts(50000),
            NoRevision: false, BaseRevision: 4, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(txn + 6000),
            Resolution: PreparedIntentResolution.Pending);

    [Fact]
    public void IntentStore_SnapshotSurvivesColdRestart_WithResolution()
    {
        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => PartitionId);
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "row/1")));
        store.Apply(new ResolveIntentCommand(Ts(1000), 1, "row/1", Commit: true));
        Assert.True(store.PersistSnapshot(PartitionId));

        PreparedIntentStore reloaded = new(dir, "rev", null);
        PreparedIntent? intent = reloaded.Get("row/1");
        Assert.NotNull(intent);
        Assert.Equal(PreparedIntentResolution.Committed, intent!.Resolution);
        Assert.Equal(new byte[] { 7, 8, 9 }, intent.Value);
        Assert.Equal("b", intent.Bucket);
        Assert.Equal(Ts(50000), intent.Expires);
    }

    [Fact]
    public void IntentStore_StateTransfer_RoundTrips()
    {
        PreparedIntentStore source = new();
        source.Apply(new PrepareIntentCommand(Intent(1000, 1, "row/5")));

        IReadOnlyList<PreparedIntent> moved = source.SnapshotRange("row/", "row0"); // "row/5" sorts inside
        Assert.Single(moved);
        byte[] blob = PreparedIntentStore.SerializeIntents(moved);
        IReadOnlyList<PreparedIntent> decoded = PreparedIntentStore.DeserializeIntents(blob);

        PreparedIntentStore destination = new();
        destination.ImportIntents(decoded);

        Assert.NotNull(destination.Get("row/5"));
        Assert.Equal(5, destination.Get("row/5")!.Revision);
    }

    [Fact]
    public void RecordStore_UnchangedSet_SkipsSnapshotRewrite_AndMutationRearmsIt()
    {
        (InitializeTransactionCommand init, CommitTransactionCommand commit) = CommittedTxn(1000, 2, "acct/1");

        TransactionRecordStore store = new(dir, "rev", null);
        store.AttachAnchorResolver(_ => (PartitionId, 0));
        store.Apply(init);
        Assert.True(store.PersistSnapshot(PartitionId));

        // Nothing changed since the last durable write, so the checkpoint must not rewrite the file. Deleting it
        // makes the skip observable: a rewrite would recreate it.
        string path = Directory.GetFiles(dir, "transactionrecord_rev_p*.snapshot").Single();
        File.Delete(path);
        Assert.True(store.PersistSnapshot(PartitionId));
        Assert.False(File.Exists(path));

        // Any mutation re-arms the rewrite, and the fresh file reflects the mutated set.
        store.Apply(commit);
        Assert.True(store.PersistSnapshot(PartitionId));
        Assert.True(File.Exists(path));

        TransactionRecordStore reloaded = new(dir, "rev", null);
        Assert.Equal(TransactionDecision.Commit, reloaded.Get(Ts(1000), 2)!.Decision);
    }

    [Fact]
    public void IntentStore_UnchangedSet_SkipsSnapshotRewrite_AndMutationRearmsIt()
    {
        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => PartitionId);
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "row/1")));
        Assert.True(store.PersistSnapshot(PartitionId));

        string path = Directory.GetFiles(dir, "preparedintent_rev_p*.snapshot").Single();
        File.Delete(path);
        Assert.True(store.PersistSnapshot(PartitionId));
        Assert.False(File.Exists(path));

        store.Apply(new ResolveIntentCommand(Ts(1000), 1, "row/1", Commit: true));
        Assert.True(store.PersistSnapshot(PartitionId));
        Assert.True(File.Exists(path));

        PreparedIntentStore reloaded = new(dir, "rev", null);
        Assert.Equal(PreparedIntentResolution.Committed, reloaded.Get("row/1")!.Resolution);
    }

    [Fact]
    public void ReceiptStore_UnchangedSet_SkipsSnapshotRewrite_AndMutationRearmsIt()
    {
        CompletionReceiptStore store = new(dir, "rev", NullLogger<IKahuna>.Instance);
        store.AttachPartitionResolver(_ => PartitionId);
        store.Record(Ts(1000), "row/1", "anchor", KeyValueDurability.Persistent);
        Assert.True(store.PersistSnapshot(PartitionId));

        string path = Directory.GetFiles(dir, "completionreceipts_rev_p*.snapshot").Single();
        File.Delete(path);
        Assert.True(store.PersistSnapshot(PartitionId));
        Assert.False(File.Exists(path));

        Assert.True(store.Forget(Ts(1000), "row/1"));
        Assert.True(store.PersistSnapshot(PartitionId));
        Assert.True(File.Exists(path));

        CompletionReceiptStore reloaded = new(dir, "rev", NullLogger<IKahuna>.Instance);
        Assert.False(reloaded.Contains(Ts(1000), "row/1", KeyValueDurability.Persistent));
    }

    [Fact]
    public void Stores_WithoutStoragePath_PersistIsNoopAndSucceeds()
    {
        TransactionRecordStore rec = new();
        PreparedIntentStore intent = new();
        // No configured directory: persist is a durable no-op (true), so the checkpoint gate is never blocked.
        Assert.True(rec.PersistSnapshot(PartitionId));
        Assert.True(intent.PersistSnapshot(PartitionId));
    }

    // ── streamed snapshot wire fidelity ─────────────────────────────────────────

    [Fact]
    public void ReceiptStore_StreamedSnapshot_ByteIdenticalToMessageSerialization()
    {
        // With anchor set: the streamed per-entry write must produce exactly the bytes of the whole-message
        // serialization the loader parses (SerializeImport with default routing/forget fields).
        CompletionReceiptStore withAnchor = new(dir, "revA", NullLogger<IKahuna>.Instance);
        withAnchor.AttachPartitionResolver(_ => PartitionId);
        withAnchor.Record(Ts(1000), "row/1", "anchor/1", KeyValueDurability.Persistent);
        Assert.True(withAnchor.PersistSnapshot(PartitionId));

        byte[] expected = CompletionReceiptStore.SerializeImport(
            [new CompletionReceiptRecord(Ts(1000), "row/1", "anchor/1", KeyValueDurability.Persistent)], 0);
        Assert.Equal(expected, File.ReadAllBytes(Path.Combine(dir, $"completionreceipts_revA_p{PartitionId}.snapshot")));

        // With the optional anchor absent, so the not-set field shape is covered too.
        CompletionReceiptStore withoutAnchor = new(dir, "revB", NullLogger<IKahuna>.Instance);
        withoutAnchor.AttachPartitionResolver(_ => PartitionId);
        withoutAnchor.Record(Ts(1001), "row/2", null, KeyValueDurability.Ephemeral);
        Assert.True(withoutAnchor.PersistSnapshot(PartitionId));

        expected = CompletionReceiptStore.SerializeImport(
            [new CompletionReceiptRecord(Ts(1001), "row/2", null, KeyValueDurability.Ephemeral)], 0);
        Assert.Equal(expected, File.ReadAllBytes(Path.Combine(dir, $"completionreceipts_revB_p{PartitionId}.snapshot")));
    }

    [Fact]
    public void IntentStore_StreamedSnapshot_ByteIdenticalToMessageSerialization()
    {
        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => PartitionId);
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "row/1")));
        Assert.True(store.PersistSnapshot(PartitionId));

        byte[] expected = PreparedIntentStore.SerializeIntents([store.Get("row/1")!]);
        Assert.Equal(expected, File.ReadAllBytes(Path.Combine(dir, $"preparedintent_rev_p{PartitionId}.snapshot")));
    }

    [Fact]
    public void ReceiptStore_ReusedSnapshotEntry_ClearsAbsentAnchor()
    {
        // The snapshot writer reuses one entry message; a fill after an anchor-bearing receipt must clear the
        // optional anchor, or the next anchor-less receipt would leak the previous value into the file.
        GrpcCompletionReceiptEntry entry = new();

        CompletionReceiptStore.FillSnapshotEntry(entry,
            new CompletionReceiptRecord(Ts(1000), "row/1", "anchor/1", KeyValueDurability.Persistent));
        Assert.True(entry.HasRecordAnchorKey);

        CompletionReceiptStore.FillSnapshotEntry(entry,
            new CompletionReceiptRecord(Ts(1001), "row/2", null, KeyValueDurability.Ephemeral));
        Assert.False(entry.HasRecordAnchorKey);

        GrpcCompletionReceiptEntry fresh = new();
        CompletionReceiptStore.FillSnapshotEntry(fresh,
            new CompletionReceiptRecord(Ts(1001), "row/2", null, KeyValueDurability.Ephemeral));
        Assert.Equal(fresh, entry);
    }

    // ── per-partition dirty stamps and routing re-arm ───────────────────────────

    [Fact]
    public void ReceiptStore_MutationOnOtherPartition_SkipsRewrite_RoutingChangeRearmsAll()
    {
        long routing = 0;
        CompletionReceiptStore store = new(dir, "rev", NullLogger<IKahuna>.Instance);
        store.AttachPartitionResolver(
            key => key.StartsWith("a/", StringComparison.Ordinal) ? 1 : 2,
            () => Interlocked.Read(ref routing));

        store.Record(Ts(1000), "a/1", null, KeyValueDurability.Persistent);
        store.Record(Ts(1001), "b/1", null, KeyValueDurability.Persistent);
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));

        string p1 = Path.Combine(dir, "completionreceipts_rev_p1.snapshot");
        string p2 = Path.Combine(dir, "completionreceipts_rev_p2.snapshot");
        File.Delete(p1);
        File.Delete(p2);

        // A mutation routed to partition 1 re-arms only partition 1's rewrite.
        store.Record(Ts(1002), "a/2", null, KeyValueDurability.Persistent);
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));
        Assert.True(File.Exists(p1));
        Assert.False(File.Exists(p2));

        // A routing change re-arms every partition, with no mutation at all: a key may have silently moved.
        File.Delete(p1);
        Interlocked.Increment(ref routing);
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));
        Assert.True(File.Exists(p1));
        Assert.True(File.Exists(p2));
    }

    [Fact]
    public void IntentStore_MutationOnOtherPartition_SkipsRewrite_RoutingChangeRearmsAll()
    {
        long routing = 0;
        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(
            key => key.StartsWith("a/", StringComparison.Ordinal) ? 1 : 2,
            () => Interlocked.Read(ref routing));

        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "a/1")));
        store.Apply(new PrepareIntentCommand(Intent(1001, 1, "b/1")));
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));

        string p1 = Path.Combine(dir, "preparedintent_rev_p1.snapshot");
        string p2 = Path.Combine(dir, "preparedintent_rev_p2.snapshot");
        File.Delete(p1);
        File.Delete(p2);

        store.Apply(new PrepareIntentCommand(Intent(1002, 1, "a/2")));
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));
        Assert.True(File.Exists(p1));
        Assert.False(File.Exists(p2));

        File.Delete(p1);
        Interlocked.Increment(ref routing);
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));
        Assert.True(File.Exists(p1));
        Assert.True(File.Exists(p2));
    }

    [Fact]
    public void ReceiptStore_ResolverFailureDuringMutation_DoesNotThrow_AndMarksAllPartitionsDirty()
    {
        // Mutations arrive on the replicated apply path, where routing can be unavailable (a restart replays
        // data-partition entries before the meta partition rebuilt the range map, and the resolver throws).
        // The mutation must still apply, and the checkpoint guard must treat it as dirt on every partition.
        bool routingHealthy = true;
        CompletionReceiptStore store = new(dir, "rev", NullLogger<IKahuna>.Instance);
        store.AttachPartitionResolver(key =>
        {
            if (!routingHealthy)
                throw new KahunaServerException("no descriptor covers the key");
            return key.StartsWith("a/", StringComparison.Ordinal) ? 1 : 2;
        });

        store.Record(Ts(1000), "a/1", null, KeyValueDurability.Persistent);
        store.Record(Ts(1001), "b/1", null, KeyValueDurability.Persistent);
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));

        string p1 = Path.Combine(dir, "completionreceipts_rev_p1.snapshot");
        string p2 = Path.Combine(dir, "completionreceipts_rev_p2.snapshot");
        File.Delete(p1);
        File.Delete(p2);

        routingHealthy = false;
        store.Record(Ts(1002), "a/2", null, KeyValueDurability.Persistent);
        routingHealthy = true;

        Assert.True(store.Contains(Ts(1002), "a/2", KeyValueDurability.Persistent));

        // The unattributable mutation re-armed both partitions, so neither file may be skipped.
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));
        Assert.True(File.Exists(p1));
        Assert.True(File.Exists(p2));
    }

    [Fact]
    public void RecordStore_MutationOnOtherPartition_SkipsRewrite_RoutingChangeRearmsAll()
    {
        long routing = 0;
        TransactionRecordStore store = new(dir, "rev", null);
        store.AttachAnchorResolver(
            anchor => (anchor.StartsWith("a/", StringComparison.Ordinal) ? 1 : 2, 0L),
            () => Interlocked.Read(ref routing));

        (InitializeTransactionCommand initA, _) = CommittedTxn(1000, 1, "a/1");
        (InitializeTransactionCommand initB, _) = CommittedTxn(2000, 1, "b/1");
        store.Apply(initA);
        store.Apply(initB);
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));

        string p1 = Path.Combine(dir, "transactionrecord_rev_p1.snapshot");
        string p2 = Path.Combine(dir, "transactionrecord_rev_p2.snapshot");
        File.Delete(p1);
        File.Delete(p2);

        (InitializeTransactionCommand initA2, _) = CommittedTxn(3000, 1, "a/2");
        store.Apply(initA2);
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));
        Assert.True(File.Exists(p1));
        Assert.False(File.Exists(p2));

        File.Delete(p1);
        Interlocked.Increment(ref routing);
        Assert.True(store.PersistSnapshot(1));
        Assert.True(store.PersistSnapshot(2));
        Assert.True(File.Exists(p1));
        Assert.True(File.Exists(p2));
    }
}
