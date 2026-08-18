using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// A partition's Raft WAL only becomes compactable once the partition checkpoints, so a partition
/// that never checkpoints keeps every entry it ever wrote — and whole-partition snapshot seeding,
/// which only happens for a replica that joins below the leader's compaction floor, never runs.
///
/// Two independent defects made that the normal outcome for any partition under continuous write
/// load, and each one alone was enough:
/// <list type="bullet">
/// <item>the checkpoint ran at the <i>start</i> of the flush tick, so its "nothing dirty" guard was
/// evaluated against the queue the flush was about to drain — under load a tick practically always
/// begins with something queued, so it bailed out every time;</item>
/// <item>the checkpoint interval was measured from the partition's last <i>write</i> rather than
/// from the start of its dirty period, which reads as "quiet for that long" — a condition a
/// continuously-written partition never reaches.</item>
/// </list>
///
/// These tests keep writes arriving on every single flush tick, which is what the previous shape
/// could not survive: a test that pauses writing before asserting passes either way.
/// </summary>
public sealed class TestCheckpointCadence
{
    private readonly ILoggerFactory loggerFactory;

    public TestCheckpointCadence(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static KahunaConfiguration Config(TimeSpan checkpointInterval) => ConfigurationValidator.Validate(new()
    {
        LocksWorkers = 1, KeyValueWorkers = 1, BackgroundWriterWorkers = 1, Storage = "memory",
        // Keeps the periodic timer out of the way so every tick is driven explicitly through
        // Receive, with no concurrent mailbox activity; it also sets the flush time budget, which
        // stays effectively unbounded here so each flush drains its queue completely.
        DirtyObjectsWriterDelay = 600_000,
        CheckpointInterval = checkpointInterval
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

    private static BackgroundWriteRequest LockWrite(int partitionId, string resource, long logIndex) => new(
        BackgroundWriteType.QueueStoreLock, partitionId, resource, [1, 2, 3], revision: 1,
        expires: HLCTimestamp.Zero, lastUsed: HLCTimestamp.Zero, lastModified: new HLCTimestamp(0, logIndex, 0),
        state: 1, noRevision: false, logIndex: logIndex);

    private sealed class WriterHarness : IDisposable
    {
        private readonly IDisposable actorLifetime;

        private readonly FairReadScheduler scheduler;

        public readonly BackgroundWriterActor Writer;

        public WriterHarness(TestBackupService.StubRaft raft, KahunaConfiguration config, IPersistenceBackend? backend = null)
        {
            actorLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

            scheduler = new FairReadScheduler(NullLogger<IRaft>.Instance, 1, 1024);
            scheduler.Start();

            IActorRef<BackgroundWriterActor, BackgroundWriteRequest> bg = actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "bg-checkpoint-cadence", raft, scheduler, backend ?? new MemoryPersistenceBackend(),
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

    /// <summary>Fails every key-value batch, so the batch exhausts its retries and is retained for
    /// the next flush cycle — data the WAL is still the only durable copy of.</summary>
    private sealed class KeyValueRejectingBackend(MemoryPersistenceBackend inner) : IPersistenceBackend
    {
        public bool RejectKeyValues { get; set; } = true;

        public bool StoreKeyValues(List<PersistenceRequestItem> items) => !RejectKeyValues && inner.StoreKeyValues(items);
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge,
            int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

    [Fact]
    public async Task ContinuouslyWrittenPartition_ChecksPointsAtTheConfiguredInterval()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        TimeSpan interval = TimeSpan.FromMilliseconds(300);
        TestBackupService.StubRaft raft = MakeLeaderRaft(1);

        using WriterHarness harness = new(raft, Config(interval));

        // Every tick starts with a write already queued — the state the previous shape read as
        // "too busy to checkpoint" — and no gap in the write stream is ever long enough to look
        // like quiescence.
        long start = Environment.TickCount64;
        long writes = 0;

        while (Environment.TickCount64 - start < 2_000)
        {
            await harness.Writer.Receive(KeyValueWrite(1, $"cadence/k{writes}", logIndex: ++writes));
            await harness.Writer.Receive(new(BackgroundWriteType.Flush));

            await Task.Delay(25, ct);
        }

        Assert.True(raft.CheckpointedPartitions.Count > 0,
            $"the partition took {writes} writes across ~2s with a {interval.TotalMilliseconds}ms checkpoint interval and never checkpointed; its WAL can never compact");

        Assert.All(raft.CheckpointedPartitions, partitionId => Assert.Equal(1, partitionId));

        // ~2s at a 300ms period is a handful of checkpoints; one per flush tick would be ~60.
        Assert.True(raft.CheckpointedPartitions.Count <= 12,
            $"{raft.CheckpointedPartitions.Count} checkpoints in ~2s: the interval is not being honoured as a period");
    }

    [Fact]
    public async Task DirtyPartition_IsNotCheckpointedBeforeItsIntervalElapses()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        TestBackupService.StubRaft raft = MakeLeaderRaft(1);

        using WriterHarness harness = new(raft, Config(TimeSpan.FromMinutes(10)));

        for (int i = 0; i < 20; i++)
        {
            await harness.Writer.Receive(KeyValueWrite(1, $"early/k{i}", logIndex: i + 1));
            await harness.Writer.Receive(new(BackgroundWriteType.Flush));

            await Task.Delay(10, ct);
        }

        Assert.Empty(raft.CheckpointedPartitions);
    }

    [Fact]
    public async Task QueuedWrites_AreReportedPerPartition_AndClearOnFlush()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        TestBackupService.StubRaft raft = MakeLeaderRaft(1, 2);

        using WriterHarness harness = new(raft, Config(TimeSpan.FromMilliseconds(20)));

        await harness.Writer.Receive(KeyValueWrite(1, "queued/k1", logIndex: 1));
        await harness.Writer.Receive(LockWrite(2, "queued/lock", logIndex: 2));

        // Checkpointing either partition here would advance its WAL retention floor past entries
        // whose only durable copy is still the log entry itself.
        Assert.Equal([1, 2], harness.Writer.CollectPartitionsWithUnflushedWrites().Order());

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Empty(harness.Writer.CollectPartitionsWithUnflushedWrites());

        // Both partitions went dirty during that flush; once their interval elapses the next tick
        // checkpoints them even though nothing new was written.
        await Task.Delay(60, ct);
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal([1, 2], raft.CheckpointedPartitions.Order());
    }

    [Fact]
    public async Task RetainedFailedBatch_HoldsTheCheckpointBack_UntilTheWriteLands()
    {
        // Driving a batch all the way to "retained for the next cycle" costs the writer's whole
        // retry schedule (5 attempts on a ~1s median decorrelated-jitter backoff), so this test
        // spends roughly fifteen seconds inside the first flush.
        KeyValueRejectingBackend backend = new(new MemoryPersistenceBackend());
        TestBackupService.StubRaft raft = MakeLeaderRaft(1);

        using WriterHarness harness = new(raft, Config(TimeSpan.FromMilliseconds(1)), backend);

        // The lock write lands, which marks partition 1 dirty and immediately due; the key-value
        // write cannot be persisted and is retained.
        await harness.Writer.Receive(LockWrite(1, "retained/lock", logIndex: 1));
        await harness.Writer.Receive(KeyValueWrite(1, "retained/k1", logIndex: 2));
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Contains(1, harness.Writer.CollectPartitionsWithUnflushedWrites());
        Assert.Empty(raft.CheckpointedPartitions);

        // Once the backend accepts it, the retained batch lands in the next flush and the partition
        // checkpoints in that same tick.
        backend.RejectKeyValues = false;

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Empty(harness.Writer.CollectPartitionsWithUnflushedWrites());
        Assert.Equal([1], raft.CheckpointedPartitions);
    }

    /// <summary>
    /// The same property on a real node: a key written to without pause must still produce a
    /// committed WAL checkpoint, which is what makes the partition's log compactable and, in turn,
    /// makes whole-partition snapshot seeding of a lagging replica reachable at all.
    /// </summary>
    [Fact]
    public async Task RealNodeUnderContinuousWrites_CommitsAWalCheckpoint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-ckpt-cadence-store-");
        string walPath = CreateTempDir("kahuna-ckpt-cadence-wal-");

        try
        {
            await using EmbeddedKahunaNode node = new(new()
            {
                InitialPartitions = 1,
                Storage = "sqlite",
                StoragePath = storagePath,
                StorageRevision = "ckpt-cadence",
                WalStorage = "sqlite",
                WalPath = walPath,
                WalRevision = "ckpt-cadence-wal",
                DirtyObjectsWriterDelay = 100,
                CheckpointInterval = TimeSpan.FromSeconds(1)
            }, loggerFactory);

            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("ckpt/k0", ct);

            byte[] value = Encoding.UTF8.GetBytes("v");

            long start = Environment.TickCount64;
            int writes = 0;

            // Writes land faster than the flush tick throughout, so the writer's queues are never
            // observed empty at the start of a tick.
            while (Environment.TickCount64 - start < 4_000)
            {
                (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, $"ckpt/k{writes++}", value, null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

                Assert.Equal(KeyValueResponseType.Set, setType);
            }

            List<(int PartitionId, long Checkpoint)> checkpoints = [.. node.Raft.GetPartitionMap()
                .Select(partition => (partition.PartitionId, node.Raft.WalAdapter.GetLastCheckpoint(partition.PartitionId)))];

            Assert.True(checkpoints.Any(static entry => entry.Checkpoint > 0),
                $"{writes} writes across ~4s at a 1s checkpoint interval produced no committed checkpoint on any partition " +
                $"({string.Join(", ", checkpoints.Select(entry => $"#{entry.PartitionId}={entry.Checkpoint}"))}); the WAL cannot compact");
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task PartitionThisNodeNoLongerLeads_IsDroppedWithoutCheckpointing()
    {
        TestBackupService.StubRaft raft = MakeLeaderRaft(1);
        raft.IsLeader = false;

        using WriterHarness harness = new(raft, Config(TimeSpan.FromMilliseconds(1)));

        await harness.Writer.Receive(KeyValueWrite(1, "follower/k1", logIndex: 1));
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Empty(raft.CheckpointedPartitions);
    }
}
