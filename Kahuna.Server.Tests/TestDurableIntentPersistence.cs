using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

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
    public void Stores_WithoutStoragePath_PersistIsNoopAndSucceeds()
    {
        TransactionRecordStore rec = new();
        PreparedIntentStore intent = new();
        // No configured directory: persist is a durable no-op (true), so the checkpoint gate is never blocked.
        Assert.True(rec.PersistSnapshot(PartitionId));
        Assert.True(intent.PersistSnapshot(PartitionId));
    }
}
