using System.Collections.Concurrent;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Protocol tests for <see cref="DurableTransactionFinalizer"/>: the durable-intent finalize sequencing
/// (initialize → prepare barrier → validate → canonical decision → resolve) drives the right terminal record and
/// intent resolutions, maps outcomes to the MustRetry/Aborted contract, and is idempotent under re-finalize. The
/// replicate seam applies to the in-memory stores on success, so post-finalize store state is asserted directly.
/// </summary>
public sealed class TestDurableTransactionFinalizer
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private sealed class Seam
    {
        // Predicate returning true for a (partitionId, logType) pair that should fail to replicate.
        public Func<int, string, bool>? Fail { get; set; }
        public readonly ConcurrentQueue<(int Partition, string Type)> Calls = new();

        public Task<bool> Replicate(int partitionId, string logType, byte[] data, CancellationToken ct)
        {
            Calls.Enqueue((partitionId, logType));
            bool fail = Fail is not null && Fail(partitionId, logType);
            return Task.FromResult(!fail);
        }
    }

    private static PreparedIntent Intent(HLCTimestamp txId, long epoch, string key) =>
        new(txId, epoch, key, ManifestHash: 0, RecordAnchorKey: "anchor", CommitTimestamp: Ts(1100),
            State: KeyValueState.Set, Value: [1], Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(6000),
            Resolution: PreparedIntentResolution.Pending);

    private static DurableFinalizeInput Input(HLCTimestamp txId, long epoch, params (int Partition, string Key)[] participants)
    {
        List<TransactionParticipantRef> manifest = participants.Select(p => new TransactionParticipantRef(p.Key, KeyValueDurability.Persistent)).ToList();
        long hash = TransactionManifest.ComputeHash(txId, epoch, participants[0].Key, Ts(1100), manifest);

        List<DurablePartitionPrepare> partitions = participants
            .GroupBy(p => p.Partition)
            .Select(g => new DurablePartitionPrepare(g.Key, g.Select(p => Intent(txId, epoch, p.Key) with { ManifestHash = hash }).ToList()))
            .ToList();

        return new DurableFinalizeInput(txId, epoch, "coord", participants[0].Key, participants[0].Partition,
            Ts(1100), DecisionDeadline: Ts(9000), hash, manifest, partitions, CreatedAt: Ts(1000));
    }

    private static (DurableTransactionFinalizer, TransactionRecordStore, PreparedIntentStore) Build(Seam seam)
    {
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        return (new DurableTransactionFinalizer(records, intents, seam.Replicate), records, intents);
    }

    private static Func<CancellationToken, Task<bool>> Validate(bool ok) => _ => Task.FromResult(ok);

    [Fact]
    public async Task SingleParticipant_Commit()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, PreparedIntentStore intents) = Build(seam);

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(Input(txId, 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.Committed, outcome.Result);
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
        Assert.Null(intents.Get("acct/1")); // resolved and garbage-collected in one atomic settle
    }

    [Fact]
    public async Task MultiParticipant_MultiPartition_Commit()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, PreparedIntentStore intents) = Build(seam);

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(
            Input(txId, 1, (5, "acct/1"), (8, "idx/name/bob")), Validate(true), opId: Ts(2000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.Committed, outcome.Result);
        Assert.Null(intents.Get("acct/1"));
        Assert.Null(intents.Get("idx/name/bob"));
    }

    [Fact]
    public async Task ReadSetConflict_AbortsAsConflict()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, PreparedIntentStore intents) = Build(seam);

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(Input(txId, 1, (5, "acct/1")), Validate(false), opId: Ts(2000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.Aborted, outcome.Result);
        Assert.Equal(TransactionAbortClass.Conflict, outcome.AbortClass);
        Assert.Equal(TransactionDecision.Abort, records.Get(txId, 1)!.Decision);
        Assert.Null(intents.Get("acct/1"));
    }

    [Fact]
    public async Task PartialPrepareFailure_AbortsRetryable_ResolvesPreparedAsAborted()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new() { Fail = (partition, type) => partition == 8 && type == ReplicationTypes.PreparedIntent };
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, PreparedIntentStore intents) = Build(seam);

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(
            Input(txId, 1, (5, "acct/1"), (8, "idx/name/bob")), Validate(true), opId: Ts(2000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.MustRetry, outcome.Result);
        Assert.Equal(TransactionDecision.Abort, records.Get(txId, 1)!.Decision);
        // Partition 5's intent prepared, then aborted and removed by settlement; partition 8's never landed.
        Assert.Null(intents.Get("acct/1"));
        Assert.Null(intents.Get("idx/name/bob"));
    }

    [Fact]
    public async Task InitializeFailure_MustRetry_NothingDurable()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new() { Fail = (_, type) => type == ReplicationTypes.TransactionRecord };
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, PreparedIntentStore intents) = Build(seam);

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(Input(txId, 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.MustRetry, outcome.Result);
        Assert.Null(records.Get(txId, 1));
        Assert.Equal(0, intents.Count);
    }

    [Fact]
    public async Task CommitPastDeadline_MustRetry_RecordStaysUndecided()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, _) = Build(seam);

        // opId (== attempt HLC) is past the frozen decision deadline (9000): the commit CAS is rejected.
        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(Input(txId, 1, (5, "acct/1")), Validate(true), opId: Ts(10000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.MustRetry, outcome.Result);
        Assert.Equal(TransactionDecision.Undecided, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public async Task ReFinalize_IsIdempotent()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, _) = Build(seam);
        DurableFinalizeInput input = Input(txId, 1, (5, "acct/1"));

        DurableFinalizeOutcome first = await finalizer.FinalizeAsync(input, Validate(true), opId: Ts(2000), CancellationToken.None);
        DurableFinalizeOutcome second = await finalizer.FinalizeAsync(input, Validate(true), opId: Ts(2000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.Committed, first.Result);
        Assert.Equal(DurableFinalizeResult.Committed, second.Result);
        Assert.Equal(1, records.Count);
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public async Task SequentialTransactions_SameKey_BothCommit_NoLingeringIntent()
    {
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, _, PreparedIntentStore intents) = Build(seam);

        DurableFinalizeOutcome first = await finalizer.FinalizeAsync(Input(Ts(1000), 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);
        Assert.Equal(DurableFinalizeResult.Committed, first.Result);
        Assert.Null(intents.Get("acct/1")); // first transaction's intent is GC'd on commit

        // A second, later transaction writes the same key; without GC its prepare would conflict with the first's
        // lingering intent (one live intent per key) and this key would be stuck.
        DurableFinalizeOutcome second = await finalizer.FinalizeAsync(Input(Ts(3000), 1, (5, "acct/1")), Validate(true), opId: Ts(4000), CancellationToken.None);
        Assert.Equal(DurableFinalizeResult.Committed, second.Result);
        Assert.Null(intents.Get("acct/1"));
        Assert.Equal(0, intents.Count);
    }

    [Fact]
    public async Task Commit_MaterializesCommittedValueAsKeyValueRecord()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, _, _) = Build(seam);

        await finalizer.FinalizeAsync(Input(txId, 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);

        // Commit resolution replays the intent as an ordinary key/value record so the existing replicator applies it.
        Assert.Contains(seam.Calls, c => c.Type == ReplicationTypes.KeyValues);
    }

    [Fact]
    public async Task Abort_DoesNotMaterializeAnyKeyValueRecord()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, _, _) = Build(seam);

        await finalizer.FinalizeAsync(Input(txId, 1, (5, "acct/1")), Validate(false), opId: Ts(2000), CancellationToken.None);

        Assert.DoesNotContain(seam.Calls, c => c.Type == ReplicationTypes.KeyValues);
    }

    [Fact]
    public void Materializer_ProducesKeyValueRecordStampedWithCommitTimestamp()
    {
        PreparedIntent intent = Intent(Ts(1000), 1, "acct/1") with { Value = [4, 5, 6], Revision = 9, CommitTimestamp = Ts(1234) };

        byte[] bytes = PreparedIntentMaterializer.ToKeyValueRecord(intent);
        Replication.Protos.KeyValueMessage message = ReplicationSerializer.UnserializeKeyValueMessage(bytes);

        Assert.Equal("acct/1", message.Key);
        Assert.Equal(new byte[] { 4, 5, 6 }, message.Value.ToByteArray());
        Assert.Equal(9, message.Revision);
        // One canonical commit timestamp stamps last-modified.
        Assert.Equal(Ts(1234), new HLCTimestamp(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter));
    }
}
