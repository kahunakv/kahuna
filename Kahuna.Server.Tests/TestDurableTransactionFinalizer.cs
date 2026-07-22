using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
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

        // The stores the seam applies to, mirroring the production scheduler-completion apply owner: this single
        // ordered call applies each record/intent delta and reports prepare acknowledgement, so the finalizer no
        // longer applies itself. Set by Build; null for tests that only assert scheduling.
        public TransactionRecordStore? Records;
        public PreparedIntentStore? Intents;

        public Task<bool> Replicate(int partitionId, string logType, byte[] data, WriteAdmissionClass admissionClass, CancellationToken ct)
        {
            Calls.Enqueue((partitionId, logType));
            if (Fail is not null && Fail(partitionId, logType))
                return Task.FromResult(false);

            RaftLog log = new() { LogType = logType, LogData = data };
            if (logType == ReplicationTypes.TransactionRecord)
                Records?.Replicate(partitionId, log);
            else if (logType == ReplicationTypes.PreparedIntent)
                return Task.FromResult(Intents is null || Intents.ApplyDeltaAckPrepares(log));

            return Task.FromResult(true);
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
            .Select(g => new DurablePartitionPrepare(g.Key, 0L, g.Select(p => Intent(txId, epoch, p.Key) with { ManifestHash = hash }).ToList()))
            .ToList();

        return new DurableFinalizeInput(txId, epoch, "coord", participants[0].Key, participants[0].Partition,
            AnchorGeneration: 0L, Ts(1100), DecisionDeadline: Ts(9000), hash, manifest, partitions, CreatedAt: Ts(1000));
    }

    private static (DurableTransactionFinalizer, TransactionRecordStore, PreparedIntentStore) Build(Seam seam)
    {
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
        return (new DurableTransactionFinalizer(records, intents, seam.Replicate), records, intents);
    }

    private static (DurableTransactionFinalizer, TransactionRecordStore, PreparedIntentStore) Build(
        Seam seam, DurableTransactionFinalizer.ResolutionScheduler scheduler)
    {
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
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
        TransactionRecordStore commitRecords = new();
        PreparedIntentStore commitIntents = new();
        Seam commitSeam = new() { Records = commitRecords, Intents = commitIntents };
        ConcurrentQueue<(int Partition, string Key)> commits = new();
        DurableTransactionFinalizer commitFinalizer = new(
            commitRecords, commitIntents, commitSeam.Replicate,
            applyCommitLocally: (p, i) => { commits.Enqueue((p, i.Key)); return Task.FromResult(true); });

        await commitFinalizer.FinalizeAsync(
            Input(Ts(1000), 1, (5, "acct/1"), (8, "idx/name/bob")), Validate(true), opId: Ts(2000), CancellationToken.None);

        Assert.Contains((5, "acct/1"), commits);
        Assert.Contains((8, "idx/name/bob"), commits);

        // Abort path (validation fails): the leader rollback seam fires for each staged intent; commit seam does not.
        TransactionRecordStore abortRecords = new();
        PreparedIntentStore abortIntents = new();
        Seam abortSeam = new() { Records = abortRecords, Intents = abortIntents };
        ConcurrentQueue<(int Partition, string Key)> rollbacks = new();
        ConcurrentQueue<(int Partition, string Key)> abortCommits = new();
        DurableTransactionFinalizer abortFinalizer = new(
            abortRecords, abortIntents, abortSeam.Replicate,
            applyCommitLocally: (p, i) => { abortCommits.Enqueue((p, i.Key)); return Task.FromResult(true); },
            applyRollbackLocally: (p, i) => { rollbacks.Enqueue((p, i.Key)); return Task.FromResult(true); });

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
    public async Task PrepareBarrier_SubmitsEveryParticipantBeforeAwaitingResults()
    {
        Seam seam = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
        TaskCompletionSource preparedStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releasePrepares = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int preparesStarted = 0;

        async Task<bool> Replicate(int partitionId, string logType, byte[] data, WriteAdmissionClass admissionClass, CancellationToken cancellationToken)
        {
            if (logType == ReplicationTypes.PreparedIntent && Interlocked.Increment(ref preparesStarted) == 2)
                preparedStarted.TrySetResult();

            if (logType == ReplicationTypes.PreparedIntent)
                await releasePrepares.Task.WaitAsync(cancellationToken);

            return await seam.Replicate(partitionId, logType, data, admissionClass, cancellationToken);
        }

        DurableTransactionFinalizer finalizer = new(records, intents, Replicate);
        Task<DurableFinalizeOutcome> finalize = finalizer.FinalizeAsync(
            Input(Ts(1000), 1, (5, "acct/1"), (8, "idx/name/bob")), Validate(true), Ts(2000), CancellationToken.None);

        await preparedStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        Assert.False(finalize.IsCompleted);

        releasePrepares.TrySetResult();
        Assert.Equal(DurableFinalizeResult.Committed, (await finalize).Result);
    }

    [Fact]
    public async Task Commit_SubmitsMaterializationsInCappedWindows()
    {
        Seam seam = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
        TaskCompletionSource firstWindowStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirstWindow = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int materializationsStarted = 0;

        async Task<bool> Replicate(int partitionId, string logType, byte[] data, WriteAdmissionClass admissionClass, CancellationToken cancellationToken)
        {
            if (logType == ReplicationTypes.KeyValues)
            {
                int started = Interlocked.Increment(ref materializationsStarted);
                if (started == 3)
                    firstWindowStarted.TrySetResult();

                if (started <= 3)
                    await releaseFirstWindow.Task.WaitAsync(cancellationToken);
            }

            return await seam.Replicate(partitionId, logType, data, admissionClass, cancellationToken);
        }

        using DurableTransactionFinalizer finalizer = new(
            records, intents, Replicate, maxMaterializationBatchItems: 3);
        Task<DurableFinalizeOutcome> finalize = finalizer.FinalizeAsync(
            Input(Ts(1000), 1,
                (5, "acct/1"), (5, "acct/2"), (5, "acct/3"), (5, "acct/4"),
                (5, "acct/5"), (5, "acct/6"), (5, "acct/7")),
            Validate(true), Ts(2000), CancellationToken.None);

        await firstWindowStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        Assert.Equal(3, Volatile.Read(ref materializationsStarted));
        Assert.False(finalize.IsCompleted);

        releaseFirstWindow.TrySetResult();
        Assert.Equal(DurableFinalizeResult.Committed, (await finalize).Result);
        Assert.Equal(7, Volatile.Read(ref materializationsStarted));
    }

    [Fact]
    public async Task Commit_SubmitsOversizedMaterializationsAlone()
    {
        Seam seam = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
        int activeMaterializations = 0;
        int maxActiveMaterializations = 0;
        int materializations = 0;

        async Task<bool> Replicate(
            int partitionId,
            string logType,
            byte[] data,
            WriteAdmissionClass admissionClass,
            CancellationToken cancellationToken)
        {
            if (logType != ReplicationTypes.KeyValues)
                return await seam.Replicate(partitionId, logType, data, admissionClass, cancellationToken);

            Interlocked.Increment(ref materializations);
            int active = Interlocked.Increment(ref activeMaterializations);
            UpdateMaximum(ref maxActiveMaterializations, active);
            try
            {
                await Task.Delay(10, cancellationToken);
                return await seam.Replicate(partitionId, logType, data, admissionClass, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref activeMaterializations);
            }
        }

        using DurableTransactionFinalizer finalizer = new(
            records,
            intents,
            Replicate,
            maxMaterializationBatchItems: 10,
            maxMaterializationBatchBytes: 1);

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(
            Input(Ts(1000), 1, (5, "large/1"), (5, "large/2"), (5, "large/3")),
            Validate(true),
            Ts(2000),
            CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.Committed, outcome.Result);
        Assert.Equal(3, Volatile.Read(ref materializations));
        Assert.Equal(1, Volatile.Read(ref maxActiveMaterializations));
    }

    [Fact]
    public async Task ConcurrentTransactions_ShareLocalApplyLimit()
    {
        Seam seam = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
        TaskCompletionSource limitReached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseApplies = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int appliesStarted = 0;
        int activeApplies = 0;
        int maxActiveApplies = 0;

        async Task<bool> Apply(int _, PreparedIntent __)
        {
            int active = Interlocked.Increment(ref activeApplies);
            UpdateMaximum(ref maxActiveApplies, active);
            if (Interlocked.Increment(ref appliesStarted) == 32)
                limitReached.TrySetResult();

            try
            {
                await releaseApplies.Task.WaitAsync(TestContext.Current.CancellationToken);
                return true;
            }
            finally
            {
                Interlocked.Decrement(ref activeApplies);
            }
        }

        using DurableTransactionFinalizer finalizer = new(
            records, intents, seam.Replicate, applyCommitLocally: Apply);
        (int Partition, string Key)[] firstParticipants = Enumerable.Range(0, 40)
            .Select(i => (5, $"apply/first/{i}"))
            .ToArray();
        (int Partition, string Key)[] secondParticipants = Enumerable.Range(0, 40)
            .Select(i => (5, $"apply/second/{i}"))
            .ToArray();

        Task<DurableFinalizeOutcome> first = finalizer.FinalizeAsync(
            Input(Ts(1000), 1, firstParticipants), Validate(true), Ts(2000), CancellationToken.None);
        Task<DurableFinalizeOutcome> second = finalizer.FinalizeAsync(
            Input(Ts(3000), 1, secondParticipants), Validate(true), Ts(4000), CancellationToken.None);

        await limitReached.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Equal(32, Volatile.Read(ref appliesStarted));
        Assert.Equal(32, Volatile.Read(ref activeApplies));
        Assert.Equal(32, Volatile.Read(ref maxActiveApplies));

        releaseApplies.TrySetResult();
        DurableFinalizeOutcome[] outcomes = await Task.WhenAll(first, second);
        Assert.All(outcomes, outcome => Assert.Equal(DurableFinalizeResult.Committed, outcome.Result));
        Assert.Equal(80, Volatile.Read(ref appliesStarted));
    }

    private static void UpdateMaximum(ref int maximum, int candidate)
    {
        int observed = Volatile.Read(ref maximum);
        while (candidate > observed)
        {
            int prior = Interlocked.CompareExchange(ref maximum, candidate, observed);
            if (prior == observed)
                return;
            observed = prior;
        }
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
    public async Task Commit_MaterializationFails_PreservesIntentForRecovery_DoesNotSettle()
    {
        HLCTimestamp txId = Ts(1000);
        // The committed value's key/value materialization fails to replicate (leadership loss, routing, shutdown).
        Seam seam = new() { Fail = (_, type) => type == ReplicationTypes.KeyValues };
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, PreparedIntentStore intents) = Build(seam);

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(
            Input(txId, 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);

        // The decision is durably Commit, but the intent is the only durable copy of the committed value: it must
        // be preserved (still pending) for the recovery sweep, never resolved-and-removed — removing it would lose
        // an already-committed value irreversibly.
        Assert.Equal(DurableFinalizeResult.Committed, outcome.Result);
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
        PreparedIntent? preserved = intents.Get("acct/1");
        Assert.NotNull(preserved);
        Assert.Equal(PreparedIntentResolution.Pending, preserved!.Resolution);
    }

    [Fact]
    public async Task Commit_PartialMaterializationFailure_SettlesOnlyTheMaterializedIntent()
    {
        HLCTimestamp txId = Ts(1000);
        // Only partition 8's materialization fails; partition 5's succeeds.
        Seam seam = new() { Fail = (partition, type) => partition == 8 && type == ReplicationTypes.KeyValues };
        (DurableTransactionFinalizer finalizer, _, PreparedIntentStore intents) = Build(seam);

        await finalizer.FinalizeAsync(
            Input(txId, 1, (5, "acct/1"), (8, "idx/name/bob")), Validate(true), opId: Ts(2000), CancellationToken.None);

        // The materialized intent is settled and gone; the un-materialized one is preserved for recovery.
        Assert.Null(intents.Get("acct/1"));
        Assert.NotNull(intents.Get("idx/name/bob"));
    }

    [Fact]
    public async Task ConcurrentTransactions_SameKey_SecondPrepareRejected_Aborts()
    {
        Seam seam = new();
        // Defer settlement so the first transaction's prepared intent stays live on the key while the second runs.
        Func<CancellationToken, Task>? firstResolution = null;
        (DurableTransactionFinalizer finalizer, TransactionRecordStore records, PreparedIntentStore intents) =
            Build(seam, r => firstResolution ??= r);

        DurableFinalizeOutcome first = await finalizer.FinalizeAsync(
            Input(Ts(1000), 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);
        Assert.Equal(DurableFinalizeResult.Committed, first.Result);
        Assert.NotNull(intents.Get("acct/1")); // still live: settlement deferred

        // A different transaction prepares the same key while the first still holds a live intent. The state
        // machine rejects the prepare (one live intent per key), so this transaction owns no recoverable intent
        // and must NOT commit — it aborts. Before this fix the rejected prepare was reported as acknowledged and
        // the transaction committed a mutation recovery could never complete.
        DurableFinalizeOutcome second = await finalizer.FinalizeAsync(
            Input(Ts(3000), 1, (5, "acct/1")), Validate(true), opId: Ts(4000), CancellationToken.None);

        Assert.NotEqual(DurableFinalizeResult.Committed, second.Result);
        Assert.Equal(TransactionDecision.Abort, records.Get(Ts(3000), 1)!.Decision);
    }

    [Fact]
    public async Task ConcurrentPrepares_SameKey_ExactlyOneOwns()
    {
        PreparedIntentStore store = new();
        const int n = 64;
        const string key = "hot/key";

        int accepted = 0;
        await Parallel.ForAsync(0, n, (i, _) =>
        {
            PreparedIntent intent = Intent(new HLCTimestamp(0, 5000 + i, 0), 1, key);
            PreparedIntentApplyResult result = store.Apply(new PrepareIntentCommand(intent));
            if (result.Outcome == TransactionApplyOutcome.Applied)
                Interlocked.Increment(ref accepted);
            return ValueTask.CompletedTask;
        });

        // The read-decide-write is atomic: exactly one prepare installs the single live intent; every other is
        // rejected. Without the transition lock two racing prepares could both observe the empty key and both
        // install, leaving one transaction believing it owns an intent it does not.
        Assert.Equal(1, accepted);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task Commit_ResolutionApplyThrows_StillReturnsCommitted_PreservesIntent()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
        // The leader commit-apply throws after the canonical decision is durable Commit.
        DurableTransactionFinalizer finalizer = new(
            records, intents, seam.Replicate,
            applyCommitLocally: (_, _) => throw new InvalidOperationException("materialize boom"));

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(
            Input(txId, 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);

        // A post-decision failure must never surface as an abort or escape: the transaction is committed, and the
        // intent is preserved for the recovery sweep to materialize.
        Assert.Equal(DurableFinalizeResult.Committed, outcome.Result);
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
        Assert.NotNull(intents.Get("acct/1"));
    }

    [Fact]
    public async Task Abort_ResolutionApplyThrows_StillReturnsAborted_DoesNotEscape()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
        // The abort resolution's leader rollback-apply throws; it must not escape and must not change the outcome.
        DurableTransactionFinalizer finalizer = new(
            records, intents, seam.Replicate,
            applyRollbackLocally: (_, _) => throw new InvalidOperationException("rollback boom"));

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(
            Input(txId, 1, (5, "acct/1")), Validate(false), opId: Ts(2000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.Aborted, outcome.Result);
        Assert.Equal(TransactionDecision.Abort, records.Get(txId, 1)!.Decision);
        Assert.NotNull(intents.Get("acct/1"));
    }

    [Fact]
    public async Task Commit_FreshAttemptHlcPastDeadline_YieldsToRecovery()
    {
        HLCTimestamp txId = Ts(1000);
        Seam seam = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
        // A fresh attempt clock returns a time past the frozen decision deadline (9000), even though the operation
        // id (2000) is well within it. The commit compare-and-set is rejected and the record stays Undecided,
        // yielding to presumed-abort recovery — proving the deadline is checked against a fresh attempt HLC, not
        // the commit timestamp that is always in-window.
        DurableTransactionFinalizer finalizer = new(
            records, intents, seam.Replicate, attemptClock: () => Ts(10000));

        DurableFinalizeOutcome outcome = await finalizer.FinalizeAsync(
            Input(txId, 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);

        Assert.Equal(DurableFinalizeResult.MustRetry, outcome.Result);
        Assert.Equal(TransactionDecision.Undecided, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public async Task DecisionLatency_RecordedAtDecision_BeforeResolution()
    {
        Seam seam = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        seam.Records = records;
        seam.Intents = intents;
        bool latencyRecorded = false;
        Func<CancellationToken, Task>? deferredResolution = null;
        DurableTransactionFinalizer finalizer = new(
            records, intents, seam.Replicate,
            resolutionScheduler: r => deferredResolution = r,
            recordDecisionLatencyMs: _ => latencyRecorded = true);

        await finalizer.FinalizeAsync(Input(Ts(1000), 1, (5, "acct/1")), Validate(true), opId: Ts(2000), CancellationToken.None);

        // Latency is recorded once the decision is durable, before resolution runs (here it is only scheduled),
        // so post-decision settlement time never inflates the deadline estimator.
        Assert.True(latencyRecorded);
        Assert.NotNull(deferredResolution);
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
        Assert.Equal(intent.TransactionId, new HLCTimestamp(
            message.TransactionIdNode, message.TransactionIdPhysical, message.TransactionIdCounter));
        Assert.Equal(intent.RecordAnchorKey, message.RecordAnchorKey);
    }
}
