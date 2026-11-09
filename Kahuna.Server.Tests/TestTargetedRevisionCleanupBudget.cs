using System.Diagnostics;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// The targeted revision cleanup runs on the single background writer between flush passes, so its
/// cost is subtracted from flush throughput. It used to hand every queued key to the backend in one
/// call with no bound on how long that call could take; on a hot key-set with deep revision blocks
/// that call took seconds per cycle, the flush ran a fraction of the time, and the unflushed backlog
/// grew until the heap limit killed the replica. The cleanup now works in chunks under a wall-clock
/// budget and re-queues what it did not reach.
/// </summary>
public sealed class TestTargetedRevisionCleanupBudget
{
    private static KahunaConfiguration Config(TimeSpan pruneBudget, TimeSpan? sweepInterval = null) => ConfigurationValidator.Validate(new()
    {
        LocksWorkers = 1, KeyValueWorkers = 1, BackgroundWriterWorkers = 1, Storage = "memory",
        // Keeps the periodic timer out of the way so every tick is driven explicitly through Receive.
        DirtyObjectsWriterDelay = 600_000,
        CheckpointInterval = TimeSpan.FromMinutes(10),
        PersistentRevisionRetentionAge = TimeSpan.FromHours(1),
        PersistentRevisionCleanupOnWrite = true,
        PersistentRevisionCleanupTimeBudget = pruneBudget,
        PersistentRevisionCleanupInterval = sweepInterval ?? TimeSpan.FromMinutes(5)
    });

    private static TestBackupService.StubRaft MakeLeaderRaft(params int[] partitionIds) => new(
        new InMemoryWAL(NullLogger<IRaft>.Instance),
        [.. partitionIds.Select(static id => new RaftPartitionRange { PartitionId = id, State = RaftPartitionState.Active })])
    {
        IsLeader = true
    };

    private static BackgroundWriteRequest KeyValueWrite(int partitionId, string key, long logIndex) => new(
        BackgroundWriteType.QueueStoreKeyValue, partitionId, key, [1, 2, 3], revision: 1,
        expires: HLCTimestamp.Zero, lastUsed: HLCTimestamp.Zero, lastModified: new HLCTimestamp(0, logIndex, 0),
        state: 1, noRevision: false, logIndex: logIndex);

    /// <summary>Memory backend whose prune costs a fixed delay per key and records every key it was
    /// handed, standing in for a store with deep revision blocks.</summary>
    private sealed class SlowPruneBackend(MemoryPersistenceBackend inner, TimeSpan perKey) : IPersistenceBackend
    {
        public readonly List<int> ChunkSizes = [];
        public int KeysPruned;
        public bool Fail;

        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public LockRow? GetLock(string resource) => inner.GetLock(resource);
        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);

        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge,
            int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result)
        {
            int count = keys?.Count ?? 0;
            ChunkSizes.Add(count);

            if (Fail)
            {
                result = default;
                return false;
            }

            Thread.Sleep(perKey * count);
            KeysPruned += count;
            result = new(count, 0, BatchLimitReached: false);
            return true;
        }
    }

    /// <summary>
    /// Backend that records the time budget the writer hands to each prune call. Targeted calls cost a
    /// fixed delay per key and ignore the budget (like a backend that only honours it between keys);
    /// the first <see cref="SweepPausesLeft"/> sweep calls report a time-budget pause.
    /// </summary>
    private sealed class BudgetRecordingBackend(MemoryPersistenceBackend inner, TimeSpan perTargetedKey) : IPersistenceBackend
    {
        public readonly List<TimeSpan> TargetedBudgets = [];
        public readonly List<TimeSpan> SweepBudgets = [];
        public int SweepPausesLeft = 1;
        public int SweepRowsDeletedPerPass;
        public int UnbudgetedCalls;

        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public LockRow? GetLock(string resource) => inner.GetLock(resource);
        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);

        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge,
            int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result)
        {
            UnbudgetedCalls++;
            result = new(keys?.Count ?? 0, 0, BatchLimitReached: false);
            return true;
        }

        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge,
            int batchSize, HLCTimestamp floorTimestamp, TimeSpan timeBudget, out RevisionPruneResult result)
        {
            if (keys is null)
            {
                SweepBudgets.Add(timeBudget);
                bool paused = SweepPausesLeft-- > 0;
                result = new(1, SweepRowsDeletedPerPass, BatchLimitReached: paused, TimeBudgetExhausted: paused);
                return true;
            }

            TargetedBudgets.Add(timeBudget);
            Thread.Sleep(perTargetedKey * keys.Count);
            result = new(keys.Count, 0, BatchLimitReached: false);
            return true;
        }
    }

    private sealed class WriterHarness : IDisposable
    {
        private readonly IDisposable actorLifetime;
        private readonly FairReadScheduler scheduler;
        public readonly BackgroundWriterActor Writer;

        public WriterHarness(TestBackupService.StubRaft raft, KahunaConfiguration config, IPersistenceBackend backend)
        {
            actorLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            scheduler = new FairReadScheduler(NullLogger<IRaft>.Instance, 1, 1024);
            scheduler.Start();
            IActorRef<BackgroundWriterActor, BackgroundWriteRequest> bg = actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "bg-prune-budget-" + Guid.NewGuid().ToString("N"), raft, scheduler, backend,
                null!, null!, null!, null!,
                config, NullLogger<IKahuna>.Instance, new FlushNotificationSink(), new PartitionDurabilityTracker());
            Writer = (bg.Runner.Actor as BackgroundWriterActor)!;
        }

        public void Dispose()
        {
            actorLifetime.Dispose();
            scheduler.Stop();
            scheduler.Dispose();
        }
    }

    [Fact]
    public async Task PruneStopsOnItsTimeBudget_AndRequeuesTheRest()
    {
        const int keyCount = 200;
        TimeSpan perKey = TimeSpan.FromMilliseconds(2);
        TimeSpan budget = TimeSpan.FromMilliseconds(60);

        SlowPruneBackend backend = new(new MemoryPersistenceBackend(), perKey);
        using WriterHarness harness = new(MakeLeaderRaft(1), Config(budget), backend);

        for (int i = 0; i < keyCount; i++)
            await harness.Writer.Receive(KeyValueWrite(1, $"budget/k{i}", logIndex: i + 1));

        Stopwatch tick = Stopwatch.StartNew();
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        tick.Stop();

        // Unbudgeted, the prune alone would take keyCount × perKey = 400 ms. The budget is checked
        // between chunks, so the overrun is at most one chunk.
        Assert.True(backend.KeysPruned < keyCount, $"every key was pruned in one cycle ({backend.KeysPruned}); the time budget did not bound the cycle");
        Assert.True(backend.KeysPruned > 0, "the budget must let at least one chunk through");
        Assert.All(backend.ChunkSizes, size => Assert.InRange(size, 1, 64));
        Assert.Equal(keyCount - backend.KeysPruned, harness.Writer.PendingRevisionCleanupKeyCount);
        Assert.True(tick.Elapsed < TimeSpan.FromMilliseconds(350), $"the flush tick took {tick.ElapsedMilliseconds} ms; the prune ran past its budget");

        // Later cycles drain the remainder without new writes.
        for (int i = 0; i < 20 && harness.Writer.PendingRevisionCleanupKeyCount > 0; i++)
            await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(keyCount, backend.KeysPruned);
        Assert.Equal(0, harness.Writer.PendingRevisionCleanupKeyCount);
    }

    [Fact]
    public async Task PruneFailure_RequeuesTheFailedChunkAndTheUnreachedKeys()
    {
        const int keyCount = 100;
        SlowPruneBackend backend = new(new MemoryPersistenceBackend(), TimeSpan.Zero) { Fail = true };
        using WriterHarness harness = new(MakeLeaderRaft(1), Config(TimeSpan.FromSeconds(5)), backend);

        for (int i = 0; i < keyCount; i++)
            await harness.Writer.Receive(KeyValueWrite(1, $"fail/k{i}", logIndex: i + 1));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        // The first chunk failed; it and everything after it must still be queued.
        Assert.Single(backend.ChunkSizes);
        Assert.Equal(keyCount, harness.Writer.PendingRevisionCleanupKeyCount);

        backend.Fail = false;
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(keyCount, backend.KeysPruned);
        Assert.Equal(0, harness.Writer.PendingRevisionCleanupKeyCount);
    }

    [Fact]
    public async Task WriterBacklogCounters_TrackReceivedAndDrainedRequests()
    {
        SlowPruneBackend backend = new(new MemoryPersistenceBackend(), TimeSpan.Zero);
        using WriterHarness harness = new(MakeLeaderRaft(1), Config(TimeSpan.FromSeconds(1)), backend);

        Assert.Equal(0, harness.Writer.QueuedItems);
        Assert.Equal(0, harness.Writer.QueuedBytes);

        for (int i = 0; i < 10; i++)
            await harness.Writer.Receive(KeyValueWrite(1, $"backlog/k{i}", logIndex: i + 1));

        Assert.Equal(10, harness.Writer.QueuedItems);
        Assert.Equal(30, harness.Writer.QueuedBytes);

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(0, harness.Writer.QueuedItems);
        Assert.Equal(0, harness.Writer.QueuedBytes);
    }

    /// <summary>
    /// Waits until the writer has finished <paramref name="passes"/> catch-up turns. A catch-up turn
    /// runs on the actor's own mailbox, so a test that drives <c>Receive</c> itself must not touch the
    /// writer again before the turn is over.
    /// </summary>
    private static async Task WaitForCatchUpPasses(BackgroundWriterActor writer, int passes)
    {
        Stopwatch waited = Stopwatch.StartNew();

        while (writer.SweepCatchUpPasses < passes)
        {
            Assert.True(waited.Elapsed < TimeSpan.FromSeconds(10),
                $"the writer finished {writer.SweepCatchUpPasses} catch-up pass(es), expected {passes}");
            await Task.Delay(2, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task SweepSharesTheCycleBudget_KeepsAQuarterAtLeast_AndResumesWithoutWaitingForTheInterval()
    {
        TimeSpan budget = TimeSpan.FromMilliseconds(60);
        TimeSpan quarter = TimeSpan.FromTicks(budget.Ticks / 4);

        BudgetRecordingBackend backend = new(new MemoryPersistenceBackend(), TimeSpan.FromMilliseconds(2));
        // A 1 ms interval so the sweep is due on every explicit tick; the resume path is exercised by
        // the pause the backend reports on the first sweep call.
        using WriterHarness harness = new(MakeLeaderRaft(1), Config(budget, sweepInterval: TimeSpan.FromMilliseconds(1)), backend);

        await Task.Delay(5, TestContext.Current.CancellationToken);

        // Nothing queued: the targeted prune costs nothing, the sweep gets the whole cycle budget and
        // reports a pause. The pass deleted nothing, so no pass follows it before the next tick.
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        Assert.Single(backend.SweepBudgets);
        Assert.Equal(budget, backend.SweepBudgets[0]);

        // 200 keys at 2 ms each: the targeted prune overruns the cycle budget on its first chunk, so
        // the sweep's share would be negative — it must still get a quarter, and it must run at all
        // (the previous pass paused, so it resumes this cycle).
        for (int i = 0; i < 200; i++)
            await harness.Writer.Receive(KeyValueWrite(1, $"sweep-share/k{i}", logIndex: i + 1));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(2, backend.SweepBudgets.Count);
        Assert.Equal(quarter, backend.SweepBudgets[1]);

        // Every targeted call carried what was left of the budget, never more than the budget itself;
        // the first chunk gets the budget minus the microseconds the cycle had already spent.
        Assert.NotEmpty(backend.TargetedBudgets);
        Assert.All(backend.TargetedBudgets, b => Assert.InRange(b, TimeSpan.Zero, budget));
        Assert.InRange(backend.TargetedBudgets[0], budget - TimeSpan.FromMilliseconds(10), budget);

        // The writer never falls back to the unbudgeted call.
        Assert.Equal(0, backend.UnbudgetedCalls);
    }

    /// <summary>
    /// A flush cycle runs one sweep pass. A sweep with millions of rows to delete used to move at one
    /// pass per flush period on a writer that had nothing else to do. A pass that deletes rows and
    /// stops with work left is now followed by the next one at once, for as long as the backlog lasts
    /// and no write is queued.
    /// </summary>
    [Fact]
    public async Task SweepBacklogOnAnIdleWriter_RunsPassAfterPassWithoutWaitingForATick()
    {
        TimeSpan budget = TimeSpan.FromMilliseconds(60);

        BudgetRecordingBackend backend = new(new MemoryPersistenceBackend(), TimeSpan.Zero) { SweepPausesLeft = 4, SweepRowsDeletedPerPass = 100 };
        using WriterHarness harness = new(MakeLeaderRaft(1), Config(budget, sweepInterval: TimeSpan.FromMilliseconds(1)), backend);

        await Task.Delay(5, TestContext.Current.CancellationToken);

        // One tick. Four passes pause; the fifth ends the backlog and the chain with it.
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        await WaitForCatchUpPasses(harness.Writer, 4);

        Assert.Equal(5, backend.SweepBudgets.Count);
        Assert.All(backend.SweepBudgets, b => Assert.Equal(budget, b));

        // No backlog is left, so no further pass runs until a tick asks for one.
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(4, harness.Writer.SweepCatchUpPasses);
        Assert.Equal(5, backend.SweepBudgets.Count);
    }

    /// <summary>
    /// A paused pass that deleted nothing only walked keys. On a large store with nothing to prune,
    /// passes run back to back would keep the writer and the disk busy for the whole cycle; such a
    /// sweep stays at one pass per flush cycle.
    /// </summary>
    [Fact]
    public async Task SweepPassThatDeletedNothing_WaitsForTheNextFlushCycle()
    {
        BudgetRecordingBackend backend = new(new MemoryPersistenceBackend(), TimeSpan.Zero) { SweepPausesLeft = 3 };
        using WriterHarness harness = new(MakeLeaderRaft(1), Config(TimeSpan.FromMilliseconds(60), sweepInterval: TimeSpan.FromMilliseconds(1)), backend);

        await Task.Delay(5, TestContext.Current.CancellationToken);

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.Single(backend.SweepBudgets);
        Assert.Equal(0, harness.Writer.SweepCatchUpPasses);

        // The backlog is not forgotten: the next cycle resumes the sweep.
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        Assert.Equal(2, backend.SweepBudgets.Count);
    }

    /// <summary>
    /// The catch-up pass must never take the writer from a flush that has work: with a write queued it
    /// does nothing, and the next flush cycle flushes first and runs the sweep after.
    /// </summary>
    [Fact]
    public async Task SweepCatchUp_WithAWriteQueued_LeavesThePassToTheNextFlushCycle()
    {
        BudgetRecordingBackend backend = new(new MemoryPersistenceBackend(), TimeSpan.Zero) { SweepPausesLeft = 0 };
        using WriterHarness harness = new(MakeLeaderRaft(1), Config(TimeSpan.FromMilliseconds(60), sweepInterval: TimeSpan.FromMilliseconds(1)), backend);

        await Task.Delay(5, TestContext.Current.CancellationToken);

        await harness.Writer.Receive(KeyValueWrite(1, "catch-up/queued", logIndex: 1));
        await harness.Writer.Receive(new(BackgroundWriteType.RevisionSweepCatchUp));

        Assert.Empty(backend.SweepBudgets);
        Assert.Equal(0, harness.Writer.SweepCatchUpPasses);
        Assert.Equal(1, harness.Writer.QueuedItems);

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(0, harness.Writer.QueuedItems);
        Assert.Single(backend.SweepBudgets);
    }

    [Fact]
    public async Task WriterTracksTheAverageValueSizeOfReceivedWrites_ForInboxSizing()
    {
        SlowPruneBackend backend = new(new MemoryPersistenceBackend(), TimeSpan.Zero);
        using WriterHarness harness = new(MakeLeaderRaft(1), Config(TimeSpan.FromMilliseconds(50)), backend);

        Assert.Equal(0, harness.Writer.AverageValueBytes);

        for (int i = 0; i < 10; i++)
            await harness.Writer.Receive(new(
                BackgroundWriteType.QueueStoreKeyValue, 1, $"avg/k{i}", new byte[300], revision: 1,
                expires: HLCTimestamp.Zero, lastUsed: HLCTimestamp.Zero, lastModified: new HLCTimestamp(0, i + 1, 0),
                state: 1, noRevision: false, logIndex: i + 1));

        Assert.Equal(300, harness.Writer.AverageValueBytes);
        Assert.Equal(3000, harness.Writer.QueuedBytes);
        Assert.Equal(10, harness.Writer.QueuedItems);

        // A shift in the value mix moves the average towards the new size without jumping to it.
        for (int i = 0; i < 10; i++)
            await harness.Writer.Receive(new(
                BackgroundWriteType.QueueStoreKeyValue, 1, $"avg/s{i}", new byte[100], revision: 1,
                expires: HLCTimestamp.Zero, lastUsed: HLCTimestamp.Zero, lastModified: new HLCTimestamp(0, 100 + i, 0),
                state: 1, noRevision: false, logIndex: 100 + i));

        Assert.InRange(harness.Writer.AverageValueBytes, 280, 299);
        Assert.Equal(4000, harness.Writer.QueuedBytes);

        // Draining the queues drops the exact byte count to zero; the average is a rate, it stays.
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        Assert.Equal(0, harness.Writer.QueuedBytes);
        Assert.Equal(0, harness.Writer.QueuedItems);
        Assert.InRange(harness.Writer.AverageValueBytes, 280, 299);
    }
}
