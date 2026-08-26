using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// The below-head coherence reconcile must be an active recovery path, not only an alarm: when the
/// durable current row reads below the committed head (a regressed current marker, or a head flush
/// lost after its overlay entry was removed), the exact head revision normally still exists as
/// retained history — the reconcile recovers it locally and re-promotes it through the persistence
/// path. The settle-time witness cannot arm this repair: it passed, so nothing was parked, which is
/// exactly why the alarm previously had nothing to re-drive. Also covers the background writer's
/// duplicate collapse: the same committed mutation queued by the owning actor and the Raft consumer
/// becomes one physical write.
/// </summary>
public sealed class TestCoherenceHeadRecovery : RaftTrackingTest
{
    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private (RaftManager Raft, FairReadScheduler Scheduler, KahunaConfiguration Config) CreateRaftAndConfig(string nodeName)
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16,
            // Keep the writer's periodic flush far away so each test controls flush timing.
            DirtyObjectsWriterDelay = 30_000
        });

        RaftManager raft = Track(new RaftManager(
            new RaftConfiguration
            {
                NodeName = nodeName,
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance));

        return (raft, (FairReadScheduler)raft.ReadScheduler, config);
    }

    private static async Task WaitUntil(Func<bool> condition, string what, int timeoutMs = 5_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (!condition())
        {
            if (Environment.TickCount64 >= deadline)
                Assert.Fail($"{what} did not happen within {timeoutMs} ms");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    private static async Task FlushAndAwait(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer)
    {
        TaskCompletionSource<bool> flushed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        writer.Send(new(BackgroundWriteType.FlushAndNotify, flushed));
        Assert.True(await flushed.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task BelowHeadReconcile_RepromotesTheHeadFromRetainedHistory()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config) = CreateRaftAndConfig("head-recovery");

        scheduler.Start();
        try
        {
            MemoryPersistenceBackend inner = new();
            UnflushedKeyValueWritesIndex overlay = new();
            UnflushedOverlayPersistenceBackend backend = new(inner, overlay, new UnflushedLockWritesIndex());

            IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
                actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                    "head-recovery-bg", raft, raft.ReadScheduler, backend,
                    null!, null!, new TransactionRecordStore(), new PreparedIntentStore(),
                    config, NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

            // The wedge durable shape, presented through the hydration seams: the current row reads
            // 310 while the committed head is 311, and the head's exact revision is still retained
            // as history. (The store API can no longer fabricate a genuinely regressed backend —
            // the monotonic current-head guard exists — so the below-head read is simulated here.)
            KeyValueEntry staleCurrentRow = new()
                { Revision = 310, Value = "stale"u8.ToArray(), State = KeyValueState.Set, LastModified = Ts(1_000) };
            KeyValueEntry retainedHead = new()
                { Revision = 311, Value = "head"u8.ToArray(), State = KeyValueState.Set, LastModified = Ts(2_000), LastUsed = Ts(2_000) };

            int historyReads = 0;

            // The null router makes the trailing resident-entry reconcile fail after the durable
            // re-promotion already happened; that failure is caught and logged, and the refusal
            // streak owns re-arming — the durable half under test here must complete regardless.
            KeyValueReplicator replicator = new(
                writer, null!, raft, null!, null!, null!, NullLogger<IKahuna>.Instance,
                unflushedWrites: overlay,
                hydrateFromBackend: (_, _) => Task.FromResult<KeyValueEntry?>(staleCurrentRow),
                hydrateRevisionFromBackend: (_, _, revision) =>
                {
                    Interlocked.Increment(ref historyReads);
                    return Task.FromResult<KeyValueEntry?>(revision == 311 ? retainedHead : null);
                });

            replicator.ScheduleCoherenceReconcile(0, "wedge/k", committedHeadRevision: 311);

            // The recovered head is recorded in the overlay before the flush is queued, so reads
            // serve it immediately.
            await WaitUntil(
                () => overlay.TryGet("wedge/k", out UnflushedKeyValueWrite queued) && queued.Revision == 311,
                "overlay re-promotion of the recovered head");
            Assert.Equal(1, historyReads);

            // Draining the writer lands the head through the monotonic store and prunes the overlay.
            await FlushAndAwait(writer);

            Assert.False(overlay.TryGet("wedge/k", out _));
            KeyValueEntry? durable = inner.GetKeyValue("wedge/k");
            Assert.NotNull(durable);
            Assert.Equal(311, durable!.Revision);
            Assert.Equal("head"u8.ToArray(), durable.Value);
        }
        finally
        {
            scheduler.Stop();
        }
    }

    [Fact]
    public async Task BelowHeadReconcile_WithoutRetainedHistory_OnlyAlarms()
    {
        KeyValueEntry staleCurrentRow = new()
            { Revision = 310, Value = "stale"u8.ToArray(), State = KeyValueState.Set, LastModified = Ts(1_000) };

        UnflushedKeyValueWritesIndex overlay = new();
        int historyReads = 0;

        KeyValueReplicator replicator = new(
            null!, null!, null!, null!, null!, null!, NullLogger<IKahuna>.Instance,
            unflushedWrites: overlay,
            hydrateFromBackend: (_, _) => Task.FromResult<KeyValueEntry?>(staleCurrentRow),
            hydrateRevisionFromBackend: (_, _, _) =>
            {
                Interlocked.Increment(ref historyReads);
                return Task.FromResult<KeyValueEntry?>(null);
            });

        replicator.ScheduleCoherenceReconcile(0, "wedge/missing", committedHeadRevision: 311);

        await WaitUntil(() => Volatile.Read(ref historyReads) > 0, "history lookup for the missing head");
        await Task.Delay(100, TestContext.Current.CancellationToken);

        // Nothing to heal from: no re-promotion may be fabricated.
        Assert.False(overlay.TryGet("wedge/missing", out _));
    }

    [Fact]
    public async Task AtHeadReconcile_NeverConsultsHistory()
    {
        KeyValueEntry headCurrentRow = new()
            { Revision = 311, Value = "head"u8.ToArray(), State = KeyValueState.Set, LastModified = Ts(2_000) };

        int currentReads = 0;
        int historyReads = 0;

        KeyValueReplicator replicator = new(
            null!, null!, null!, null!, null!, null!, NullLogger<IKahuna>.Instance,
            hydrateFromBackend: (_, _) =>
            {
                Interlocked.Increment(ref currentReads);
                return Task.FromResult<KeyValueEntry?>(headCurrentRow);
            },
            hydrateRevisionFromBackend: (_, _, _) =>
            {
                Interlocked.Increment(ref historyReads);
                return Task.FromResult<KeyValueEntry?>(null);
            });

        replicator.ScheduleCoherenceReconcile(0, "healthy/k", committedHeadRevision: 311);

        await WaitUntil(() => Volatile.Read(ref currentReads) > 0, "durable current-row read");
        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.Equal(0, Volatile.Read(ref historyReads));
    }

    [Fact]
    public async Task DuplicateQueuedMutations_CollapseToOnePhysicalWrite()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config) = CreateRaftAndConfig("dedup-writer");

        scheduler.Start();
        try
        {
            CountingBackend backend = new();

            IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
                actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                    "dedup-bg", raft, raft.ReadScheduler, backend,
                    null!, null!, new TransactionRecordStore(), new PreparedIntentStore(),
                    config, NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

            // The same committed mutation queued twice — once by the owning actor ahead of the
            // client acknowledgement, once by the Raft consumer apply — plus one distinct commit.
            writer.Send(BackgroundWriteRequestPool.Rent(
                BackgroundWriteType.QueueStoreKeyValue, 0, "dup/k", "v5"u8.ToArray(), 5,
                HLCTimestamp.Zero, Ts(5_000), Ts(5_000), (int)KeyValueState.Set, noRevision: false));
            writer.Send(BackgroundWriteRequestPool.Rent(
                BackgroundWriteType.QueueStoreKeyValue, 0, "dup/k", "v5"u8.ToArray(), 5,
                HLCTimestamp.Zero, Ts(5_000), Ts(5_000), (int)KeyValueState.Set, noRevision: false));
            writer.Send(BackgroundWriteRequestPool.Rent(
                BackgroundWriteType.QueueStoreKeyValue, 0, "dup/k", "v6"u8.ToArray(), 6,
                HLCTimestamp.Zero, Ts(6_000), Ts(6_000), (int)KeyValueState.Set, noRevision: false));

            await FlushAndAwait(writer);

            List<PersistenceRequestItem> stored = backend.SnapshotStoredKeyValueItems();
            Assert.Equal(2, stored.Count);
            Assert.Equal(5, stored[0].Revision);
            Assert.Equal(6, stored[1].Revision);
            Assert.Equal(6, backend.GetKeyValue("dup/k")?.Revision);
        }
        finally
        {
            scheduler.Stop();
        }
    }

    /// <summary>Records every key-value item the writer hands to the store, delegating the actual
    /// persistence (and every read) to a real in-memory backend.</summary>
    private sealed class CountingBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();

        private readonly List<PersistenceRequestItem> storedKeyValueItems = [];

        internal List<PersistenceRequestItem> SnapshotStoredKeyValueItems()
        {
            lock (storedKeyValueItems)
                return [.. storedKeyValueItems];
        }

        public bool StoreKeyValues(List<PersistenceRequestItem> items)
        {
            lock (storedKeyValueItems)
                storedKeyValueItems.AddRange(items);
            return inner.StoreKeyValues(items);
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }
}
