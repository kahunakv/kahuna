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
    private static KahunaConfiguration Config(TimeSpan pruneBudget) => ConfigurationValidator.Validate(new()
    {
        LocksWorkers = 1, KeyValueWorkers = 1, BackgroundWriterWorkers = 1, Storage = "memory",
        // Keeps the periodic timer out of the way so every tick is driven explicitly through Receive.
        DirtyObjectsWriterDelay = 600_000,
        CheckpointInterval = TimeSpan.FromMinutes(10),
        PersistentRevisionRetentionAge = TimeSpan.FromHours(1),
        PersistentRevisionCleanupOnWrite = true,
        PersistentRevisionCleanupTimeBudget = pruneBudget
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
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
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
}
