using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Round-trip replication tests for the durable-intent 2PC stores: a batch of transitions serialized to a
/// <see cref="RaftLog"/> and applied via Replicate/Restore reconstructs the same state a direct apply would, and
/// a replayed log is idempotent — proving the command↔protobuf mapping is faithful and the deterministic apply
/// converges (the property follower apply, WAL replay, and state transfer all rely on).
/// </summary>
public sealed class TestDurableIntentReplication
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private const int PartitionId = 3;

    // ── transaction record store ────────────────────────────────────────────────

    private static RaftLog RecordLog(params TransactionRecordCommand[] commands) =>
        new() { LogType = ReplicationTypes.TransactionRecord, LogData = TransactionRecordStore.SerializeDelta(commands) };

    [Fact]
    public void RecordStore_InitializeThenCommit_RoundTripsThroughReplicate()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 2;
        IReadOnlyList<TransactionParticipantRef> manifest = [new("k1", KeyValueDurability.Persistent), new("k2", KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(txId, epoch, "k1", Ts(1100), manifest);

        InitializeTransactionCommand init = new(txId, epoch, "coord", "k1", Ts(1100), Ts(9000), hash, manifest, OpId: Ts(500), CreatedAt: Ts(400));
        CommitTransactionCommand commit = new(txId, epoch, hash, OpId: Ts(1500), AttemptHlc: Ts(1500));

        TransactionRecordStore store = new();
        Assert.True(store.Replicate(PartitionId, RecordLog(init, commit)));

        TransactionRecord? rec = store.Get(txId, epoch);
        Assert.NotNull(rec);
        Assert.Equal(TransactionDecision.Commit, rec!.Decision);
        Assert.Equal(Ts(1500), rec.WinningOpId);
        Assert.Equal("k1", rec.RecordAnchorKey);
        Assert.Equal(Ts(1100), rec.CommitTimestamp);
        Assert.Equal(hash, rec.ManifestHash);
        Assert.Equal(2, rec.Participants.Count);
    }

    [Fact]
    public void RecordStore_ReplayOfSameLog_IsIdempotent()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 1;
        long hash = TransactionManifest.ComputeHash(txId, epoch, "k", Ts(1100), [new("k", KeyValueDurability.Persistent)]);
        InitializeTransactionCommand init = new(txId, epoch, "coord", "k", Ts(1100), Ts(9000), hash, [new("k", KeyValueDurability.Persistent)], Ts(500), Ts(400));
        CommitTransactionCommand commit = new(txId, epoch, hash, Ts(1500), Ts(1500));

        TransactionRecordStore store = new();
        RaftLog log = RecordLog(init, commit);
        store.Replicate(PartitionId, log);
        store.Replicate(PartitionId, log); // replay
        store.Restore(PartitionId, log);   // restore path too

        Assert.Equal(1, store.Count);
        Assert.Equal(TransactionDecision.Commit, store.Get(txId, epoch)!.Decision);
        Assert.Equal(Ts(1500), store.Get(txId, epoch)!.WinningOpId);
    }

    [Fact]
    public void RecordStore_AbortFromAbsence_RoundTrips()
    {
        HLCTimestamp txId = Ts(2000);
        const long epoch = 1;
        AbortTransactionCommand abort = new(txId, epoch, ManifestHash: 77, TransactionAbortClass.PresumedAbort,
            OpId: Ts(2500), AttemptHlc: Ts(2500), RecordAnchorKey: "k", CommitTimestamp: Ts(2100), DecisionDeadline: Ts(9000), CreatedAt: Ts(2000));

        TransactionRecordStore store = new();
        store.Replicate(PartitionId, RecordLog(abort));

        TransactionRecord? rec = store.Get(txId, epoch);
        Assert.NotNull(rec);
        Assert.Equal(TransactionDecision.Abort, rec!.Decision);
        Assert.False(rec.ManifestPresent);
        Assert.Equal(TransactionAbortClass.PresumedAbort, rec.AbortClass);
    }

    // ── prepared intent store ────────────────────────────────────────────────────

    private static RaftLog IntentLog(params PreparedIntentCommand[] commands) =>
        new() { LogType = ReplicationTypes.PreparedIntent, LogData = PreparedIntentStore.SerializeDelta(commands) };

    private static PreparedIntent Intent(HLCTimestamp txId, long epoch, string key) =>
        new(txId, epoch, key, ManifestHash: 4242, RecordAnchorKey: "anchor", CommitTimestamp: Ts(1100),
            State: KeyValueState.Set, Value: [7, 8, 9], Bucket: null, Revision: 5, Expires: Ts(50000),
            NoRevision: false, BaseRevision: 4, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(6000),
            Resolution: PreparedIntentResolution.Pending);

    [Fact]
    public void IntentStore_PrepareThenResolve_RoundTripsWithMutationFidelity()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 1;
        PrepareIntentCommand prepare = new(Intent(txId, epoch, "row/1"));
        ResolveIntentCommand resolve = new(txId, epoch, "row/1", Commit: true);

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, IntentLog(prepare, resolve)));

        PreparedIntent? intent = store.Get("row/1");
        Assert.NotNull(intent);
        Assert.Equal(PreparedIntentResolution.Committed, intent!.Resolution);
        // Mutation fidelity across the proto round trip.
        Assert.Equal(KeyValueState.Set, intent.State);
        Assert.Equal(new byte[] { 7, 8, 9 }, intent.Value);
        Assert.Null(intent.Bucket);
        Assert.Equal(5, intent.Revision);
        Assert.Equal(Ts(50000), intent.Expires);
        Assert.Equal(4, intent.BaseRevision);
        Assert.Equal(Ts(6000), intent.RecoveryDeadline);
        Assert.Equal(4242, intent.ManifestHash);
    }

    [Fact]
    public void IntentStore_PrepareResolveRemove_LeavesStoreEmpty()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 1;
        PreparedIntentStore store = new();
        store.Replicate(PartitionId, IntentLog(
            new PrepareIntentCommand(Intent(txId, epoch, "row/1")),
            new ResolveIntentCommand(txId, epoch, "row/1", Commit: false),
            new RemoveIntentCommand(txId, epoch, "row/1")));

        Assert.Equal(0, store.Count);
        Assert.Null(store.Get("row/1"));
    }

    [Fact]
    public void IntentStore_ReplayOfSameLog_IsIdempotent()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 1;
        PreparedIntentStore store = new();
        RaftLog log = IntentLog(new PrepareIntentCommand(Intent(txId, epoch, "row/1")), new ResolveIntentCommand(txId, epoch, "row/1", Commit: true));

        store.Replicate(PartitionId, log);
        store.Replicate(PartitionId, log);
        store.Restore(PartitionId, log);

        Assert.Equal(1, store.Count);
        Assert.Equal(PreparedIntentResolution.Committed, store.Get("row/1")!.Resolution);
    }

    [Fact]
    public void IntentStore_TombstoneValueNull_RoundTrips()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 1;
        PreparedIntent deletion = Intent(txId, epoch, "row/1") with { State = KeyValueState.Deleted, Value = null };

        PreparedIntentStore store = new();
        store.Replicate(PartitionId, IntentLog(new PrepareIntentCommand(deletion)));

        PreparedIntent? intent = store.Get("row/1");
        Assert.NotNull(intent);
        Assert.Equal(KeyValueState.Deleted, intent!.State);
        Assert.Null(intent.Value);
    }
}
