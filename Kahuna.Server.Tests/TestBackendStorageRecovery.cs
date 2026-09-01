using System.Text;
using Kahuna;
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
/// RocksDB latches a background error after a failed WAL append (for example ENOSPC): every later
/// write returns the cached error without new I/O, so the background writer's retained-batch
/// retries can never succeed — even after the operator frees the disk. The recovery path closes
/// and reopens the engine in place, behind a swap fence, once enough consecutive flush cycles
/// fail. These tests pin three layers of that path:
/// <list type="bullet">
/// <item>the writer's wedge declaration — recovery is requested only after the configured number
/// of all-failure cycles, and the retained batch drains in the same tick once recovery
/// succeeds;</item>
/// <item>a declined recovery (volume still full, or a backend with no reset) keeps the retained
/// batch and repeats the request on later failed cycles;</item>
/// <item>the RocksDB close-and-reopen itself — data survives the swap, new writes land after it,
/// and concurrent readers are fenced rather than crashed.</item>
/// </list>
/// The genuine native latch cannot be produced by a managed double, so the last test exercises a
/// real full filesystem; it runs only when an operator provides one via an environment variable.
/// </summary>
public sealed class TestBackendStorageRecovery
{
    private static KahunaConfiguration Config() => ConfigurationValidator.Validate(new()
    {
        LocksWorkers = 1, KeyValueWorkers = 1, BackgroundWriterWorkers = 1, Storage = "memory",
        // Keeps the periodic timer out of the way so every tick is driven explicitly through
        // Receive; it also sets the flush time budget, which stays effectively unbounded here.
        DirtyObjectsWriterDelay = 600_000,
        CheckpointInterval = TimeSpan.FromMilliseconds(1)
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
                "bg-storage-recovery", raft, scheduler, backend,
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

    /// <summary>
    /// Models a backend wedged by a latched storage error: every key-value store fails until a
    /// recovery request resets the engine, exactly as the RocksDB close-and-reopen does. Whether
    /// a recovery request actually resets is configurable, so a declined recovery (volume still
    /// full) can be modeled too.
    /// </summary>
    private sealed class WedgedBackend(MemoryPersistenceBackend inner, bool recoveryResets) : IPersistenceBackend
    {
        public MemoryPersistenceBackend Inner { get; } = inner;

        public bool Wedged { get; set; } = true;

        public int RecoveryCalls { get; private set; }

        public bool StoreKeyValues(List<PersistenceRequestItem> items) => !Wedged && Inner.StoreKeyValues(items);

        public bool TryRecoverFromStorageFailure()
        {
            RecoveryCalls++;

            if (!recoveryResets)
                return false;

            Wedged = false;
            return true;
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => Inner.StoreLocks(items);
        public LockEntry? GetLock(string resource) => Inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => Inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => Inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            Inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => Inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            Inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge,
            int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            Inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            Inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

    /// <summary>
    /// The wedge declaration must be conservative and then decisive: no recovery request before
    /// the configured number of all-failure cycles, a request exactly at the threshold, and — on
    /// a successful reset — the retained batch drains in the same tick and the checkpoint
    /// follows. Each failed cycle spends the writer's full in-cycle retry schedule, so this test
    /// runs for roughly half a minute.
    /// </summary>
    [Fact]
    public async Task WedgedBackend_RecoversAtTheThreshold_AndDrainsRetainedBatchesInTheSameTick()
    {
        WedgedBackend backend = new(new MemoryPersistenceBackend(), recoveryResets: true);
        TestBackupService.StubRaft raft = MakeLeaderRaft(1);

        using WriterHarness harness = new(raft, Config(), backend);
        harness.Writer.StoreFailureCyclesBeforeRecovery = 2;

        await harness.Writer.Receive(KeyValueWrite(1, "wedge/k1", logIndex: 1));

        // Cycle 1: the store fails, the batch is retained — and one failed cycle is below the
        // threshold, so no recovery request is made yet.
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(0, backend.RecoveryCalls);
        Assert.Equal(1, harness.Writer.ConsecutiveStoreFailureCycles);
        Assert.Contains(1, harness.Writer.CollectPartitionsWithUnflushedWrites());
        Assert.Null(backend.Inner.GetKeyValue("wedge/k1"));

        // Cycle 2: the threshold is reached, recovery resets the engine, and the same tick
        // retries the retained batch — it lands, the wedge counter clears, and the partition
        // checkpoints.
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(1, backend.RecoveryCalls);
        Assert.Equal(0, harness.Writer.ConsecutiveStoreFailureCycles);
        Assert.Empty(harness.Writer.CollectPartitionsWithUnflushedWrites());
        Assert.NotNull(backend.Inner.GetKeyValue("wedge/k1"));
        Assert.Equal([1], raft.CheckpointedPartitions);
    }

    /// <summary>
    /// A declined recovery — the volume is still full, or the backend has no reset capability —
    /// must never drop the retained batch, and the request must repeat on later failed cycles so
    /// the node keeps probing for the moment recovery becomes possible.
    /// </summary>
    [Fact]
    public async Task DeclinedRecovery_KeepsTheRetainedBatch_AndRepeatsTheRequest()
    {
        WedgedBackend backend = new(new MemoryPersistenceBackend(), recoveryResets: false);
        TestBackupService.StubRaft raft = MakeLeaderRaft(1);

        using WriterHarness harness = new(raft, Config(), backend);
        harness.Writer.StoreFailureCyclesBeforeRecovery = 1;

        await harness.Writer.Receive(KeyValueWrite(1, "declined/k1", logIndex: 1));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        Assert.Equal(1, backend.RecoveryCalls);

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        Assert.Equal(2, backend.RecoveryCalls);

        // Nothing was lost across the declined recoveries.
        Assert.Contains(1, harness.Writer.CollectPartitionsWithUnflushedWrites());
        Assert.Empty(raft.CheckpointedPartitions);

        // The moment the fault clears, the retained batch lands without any recovery.
        backend.Wedged = false;
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Empty(harness.Writer.CollectPartitionsWithUnflushedWrites());
        Assert.NotNull(backend.Inner.GetKeyValue("declined/k1"));
    }

    private static PersistenceRequestItem MakeItem(string key, long revision, byte[]? value = null) =>
        new(key,
            value ?? Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    /// <summary>
    /// The close-and-reopen swap itself: data stored before the swap survives it, new writes land
    /// after it, and readers that race the swap block on the fence and then answer correctly —
    /// they never observe a disposed native handle.
    /// </summary>
    [Fact]
    public async Task RocksDbBackend_ReopensInPlace_PreservingDataAndFencingConcurrentReaders()
    {
        string dir = CreateTempDir("kahuna-rocksdb-recovery-");

        try
        {
            using RocksDbPersistenceBackend backend = new(dir, "v1");

            backend.StoreKeyValues([.. Enumerable.Range(0, 64).Select(i => MakeItem($"swap/k{i:D3}", revision: 1))]);
            Assert.True(backend.StoreDurabilityFloors([(0, 41L)]));
            Assert.NotNull(backend.GetKeyValue("swap/k000"));

            using CancellationTokenSource readersDone = new();
            List<Exception> readerFailures = [];

            Task[] readers = [.. Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
            {
                try
                {
                    int i = 0;
                    while (!readersDone.IsCancellationRequested)
                    {
                        KeyValueEntry? entry = backend.GetKeyValue($"swap/k{i++ % 64:D3}");
                        if (entry is null)
                            throw new InvalidOperationException("a pre-swap key vanished during the swap");
                    }
                }
                catch (Exception ex)
                {
                    lock (readerFailures)
                        readerFailures.Add(ex);
                }
            }))];

            // Several swaps under concurrent point reads: each one closes the native handle and
            // reopens it while the readers keep hammering the fence.
            for (int swap = 0; swap < 3; swap++)
            {
                Assert.True(backend.TryRecoverFromStorageFailure());
                await Task.Delay(50, TestContext.Current.CancellationToken);
            }

            readersDone.Cancel();
            await Task.WhenAll(readers);

            Assert.Empty(readerFailures);

            // The reopened instance serves reads, revisions, floors — and accepts new writes.
            Assert.NotNull(backend.GetKeyValue("swap/k063"));
            Assert.NotNull(backend.GetKeyValueRevision("swap/k000", 1));
            Assert.Equal(41L, backend.GetDurabilityFloor(0));

            Assert.True(backend.StoreKeyValues([MakeItem("swap/after", revision: 7)]));
            KeyValueEntry? after = backend.GetKeyValue("swap/after");
            Assert.NotNull(after);
            Assert.Equal(7, after.Revision);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    /// <summary>
    /// The genuine latch, on a genuinely full filesystem. RocksDB caches the background error
    /// inside the native engine, so no managed double can reproduce it — this test needs a small
    /// dedicated filesystem (~256 MB; a loopback mount on Linux, a small disk image elsewhere)
    /// whose directory is supplied via <c>KAHUNA_ENOSPC_FIXTURE_PATH</c>. It fills the volume,
    /// drives the store into the latched failure, frees the space, proves the latch persists,
    /// and then proves the in-place recovery clears it without a restart.
    /// </summary>
    [Fact]
    public void RocksDbBackend_LatchedEnospcError_ClearsOnlyThroughRecovery()
    {
        string? fixture = Environment.GetEnvironmentVariable("KAHUNA_ENOSPC_FIXTURE_PATH");
        Assert.SkipWhen(string.IsNullOrEmpty(fixture),
            "Set KAHUNA_ENOSPC_FIXTURE_PATH to a directory on a small (~256 MB) dedicated filesystem to run this test.");

        string dir = Path.Combine(fixture!, "kahuna-enospc-" + Guid.NewGuid().ToString("N"));
        string fillerPath = Path.Combine(fixture!, "kahuna-enospc-filler-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);

        try
        {
            using RocksDbPersistenceBackend backend = new(dir, "v1");

            Assert.True(backend.StoreKeyValues([MakeItem("enospc/seed", revision: 1)]));

            // Fill the volume: one filler file grown until the filesystem refuses the write.
            byte[] chunk = new byte[1024 * 1024];
            Random.Shared.NextBytes(chunk);

            using (FileStream filler = new(fillerPath, FileMode.CreateNew, FileAccess.Write))
            {
                try
                {
                    while (true)
                    {
                        filler.Write(chunk);
                        filler.Flush(flushToDisk: true);
                    }
                }
                catch (IOException)
                {
                    // The volume is full.
                }
            }

            // Drive the store into the failure: with the volume full, a synced write must fail
            // and the engine latches the background error.
            byte[] payload = new byte[256 * 1024];
            Random.Shared.NextBytes(payload);

            bool sawFailure = false;
            for (int i = 0; i < 64 && !sawFailure; i++)
                sawFailure = !backend.StoreKeyValues([MakeItem($"enospc/fill{i}", revision: 1, payload)]);

            Assert.True(sawFailure, "the store never failed on a full volume; the fixture filesystem may be too large");

            // Free the space. The latch is the defect: the engine still fails every write,
            // because it returns the cached error without new I/O.
            File.Delete(fillerPath);

            Assert.False(backend.StoreKeyValues([MakeItem("enospc/latched", revision: 1)]),
                "the store succeeded right after the space was freed; the latched-error premise did not hold");

            // In-place recovery: close and reopen, then writes flow again and old data is intact.
            Assert.True(backend.TryRecoverFromStorageFailure());
            Assert.True(backend.StoreKeyValues([MakeItem("enospc/after", revision: 2)]));
            Assert.NotNull(backend.GetKeyValue("enospc/seed"));
            Assert.NotNull(backend.GetKeyValue("enospc/after"));
        }
        finally
        {
            try { if (File.Exists(fillerPath)) File.Delete(fillerPath); } catch { /* best-effort */ }
            TryDeleteDir(dir);
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
}
