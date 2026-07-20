using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
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

    private static (DurableTransactionFinalizer, TransactionRecordStore, PreparedIntentStore) Build(
        Seam seam, DurableTransactionFinalizer.ResolutionScheduler scheduler)
    {
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        return (new DurableTransactionFinalizer(records, intents, seam.Replicate, scheduler), records, intents);
    }

    [Fact]
    public async Task DeferredResolution_ReturnsCommittedBeforeSettle_ThenSettlesWhenRun()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        Func<CancellationToken, Task>? captured = null;
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, PreparedIntentStore intents) =
            Build(seam, resolution => captured = resolution);

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(
            Input(txId, 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);

        // The decision is durable and the finalize returned committed, but resolution has NOT run: the intent is
        // still prepared (pending), and the canonical record says Commit. This is the deferred-settlement window.
        Assert.Equal(DurableFinalizeResult.Committed, outcome.Result);
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
        PreparedIntent? stillPending = intents.Get("acct/1");
        Assert.NotNull(stillPending);
        Assert.Equal(PreparedIntentResolution.Pending, stillPending!.Resolution);
        Assert.NotNull(captured);

        // Running the deferred resolution settles it: the intent is materialized and garbage-collected.
        await captured!(CancellationToken.None);
        Assert.Null(intents.Get("acct/1"));
    }

    private static Func<CancellationToken, Task<bool>> Validate(bool ok) => _ => Task.FromResult(ok);

    [Fact]
    public async Task Commit_InvokesCommitApplyPerIntent_AbortInvokesRollbackPerIntent()
    {
        // Commit path: the leader commit-apply seam fires for each committed intent.
        Seam commitSeam = new();
        List<(int Partition, string Key)> commits = [];
        DurableTransactionFinalizer commitFinalizer = new(
            new TransactionRecordStore(), new PreparedIntentStore(), commitSeam.Replicate,
            applyCommitLocally: (p, i) => { commits.Add((p, i.Key)); return Task.CompletedTask; });

        await commitFinalizer.FinalizeAsync(
            Input(Ts(1000), 1, (5, "acct/1"), (8, "idx/name/bob")), Validate(true), opId: Ts(2000), CancellationToken.None);

        Assert.Contains((5, "acct/1"), commits);
        Assert.Contains((8, "idx/name/bob"), commits);

        // Abort path (validation fails): the leader rollback seam fires for each staged intent; commit seam does not.
        Seam abortSeam = new();
        List<(int Partition, string Key)> rollbacks = [];
        List<(int Partition, string Key)> abortCommits = [];
        DurableTransactionFinalizer abortFinalizer = new(
            new TransactionRecordStore(), new PreparedIntentStore(), abortSeam.Replicate,
            applyCommitLocally: (p, i) => { abortCommits.Add((p, i.Key)); return Task.CompletedTask; },
            applyRollbackLocally: (p, i) => { rollbacks.Add((p, i.Key)); return Task.CompletedTask; });

        DurableFinalizeOutcome outcome = await abortFinalizer.FinalizeAsync(
            Input(Ts(3000), 1, (5, "acct/1"), (8, "idx/name/bob")), Validate(false), opId: Ts(4000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.Aborted, outcome.Result);
        Assert.Contains((5, "acct/1"), rollbacks);
        Assert.Contains((8, "idx/name/bob"), rollbacks);
        Assert.Empty(abortCommits);
    }

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
    public async Task CommitPastDeadline_MustRetry_RecordStaysUndecided_AndCountsLateRejection()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, _) = Build(seam);

        // opId (== attempt HLC) is past the frozen decision deadline (9000): the commit CAS is rejected, the record
        // stays Undecided, and the late-commit-rejection metric fires so a too-tight deadline is observable.
        long lateRejections = await MeasureCounter("kahuna.durable_tx.late_commit_rejections", async () =>
        {
            DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(Input(txId, 1, (5, "acct/1")), Validate(true), opId: Ts(10000), CancellationToken.None);
            Assert.Equal(DurableFinalizeResult.MustRetry, outcome.Result);
        });

        Assert.Equal(TransactionDecision.Undecided, records.Get(txId, 1)!.Decision);
        Assert.Equal(1, lateRejections);
    }

    [Fact]
    public async Task CommitWithinDeadline_DoesNotCountLateRejection()
    {
        Seam seam = new();
        (DurableTransactionFinalizer finalizer, _, _) = Build(seam);

        long lateRejections = await MeasureCounter("kahuna.durable_tx.late_commit_rejections", async () =>
        {
            DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(Input(Ts(1000), 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);
            Assert.Equal(DurableFinalizeResult.Committed, outcome.Result);
        });

        Assert.Equal(0, lateRejections);
    }

    // Sums the increments of a named counter on the "Kahuna" meter emitted while <paramref name="action"/> runs.
    private static async Task<long> MeasureCounter(string instrumentName, Func<Task> action)
    {
        long total = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Kahuna" && instrument.Name == instrumentName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => Interlocked.Add(ref total, measurement));
        listener.Start();

        await action();

        listener.Dispose();
        return Interlocked.Read(ref total);
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
