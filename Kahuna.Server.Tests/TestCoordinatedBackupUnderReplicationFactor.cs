using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Five nodes, six data partitions, replication factor three: no node hosts every partition, so a coordinated
/// backup must be composed from the shares of the nodes that host the partitions the coordinator does not.
/// These cases drive the real entry points (<see cref="IKahuna.TakeCoordinatedBackupAsync"/>,
/// <see cref="IKahuna.TakeIncrementalBackupAsync"/>, chain validation, restore and the bootstrap seed) through
/// the in-memory inter-node transport, on every storage engine.
/// </summary>
public sealed class TestCoordinatedBackupUnderReplicationFactor : BaseCluster, IDisposable
{
    private const int Nodes = 5;

    private const int Partitions = 6;

    private const int ReplicationFactor = 3;

    private const string Revision = "v1";

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly string tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_rf_backup_" + Guid.NewGuid().ToString("N"));

    public TestCoordinatedBackupUnderReplicationFactor(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
        Directory.CreateDirectory(tempRoot);
    }

    public void Dispose()
    {
        try { Directory.Delete(tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private string NewDir(string name)
    {
        string dir = Path.Combine(tempRoot, name + "_" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private async Task<(IRaft[] Rafts, IKahuna[] Kahunas, MemoryInterNodeCommmunication InterComm)> AssembleAsync(
        string storage, string backupDir)
    {
        int ordinal = 0;
        string storageRoot = NewDir("storage");

        (IRaft[] rafts, IKahuna[] kahunas, InMemoryCommunication _, MemoryInterNodeCommmunication interComm) =
            await AssembleClusterWithTransports(
                Nodes, "memory", Partitions, raftLogger, kahunaLogger, ReplicationFactor,
                configureKahuna: c =>
                {
                    int node = Interlocked.Increment(ref ordinal);
                    c.Storage = storage;
                    c.StorageRevision = Revision;

                    // Each node its own path: a restore destination must not overlap a live storage path.
                    c.StoragePath = Path.Combine(storageRoot, "node" + node);
                    Directory.CreateDirectory(c.StoragePath);

                    // One backup directory for the whole cluster, as an RWX volume would give. No periodic sweep:
                    // these cases look at exactly what each backup left behind.
                    c.BackupDir = backupDir;
                    c.BackupGcInterval = TimeSpan.Zero;
                });

        return (rafts, kahunas, interComm);
    }

    private static async Task<int> MetaLeaderIndex(IRaft[] rafts)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        for (int attempt = 0; attempt < 100; attempt++)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(0, ct))
                    return i;
            await Task.Delay(50, ct);
        }
        throw new InvalidOperationException("No node leads the meta partition.");
    }

    private static List<int> NotHosted(IRaft raft) =>
        [.. Enumerable.Range(1, Partitions).Where(p => !raft.HostsPartition(p))];

    private static async Task WriteAsync(IKahuna[] kahunas, string key, string value)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        for (int attempt = 0; attempt < 50; attempt++)
        {
            (KeyValueResponseType type, _, _) = await kahunas[attempt % kahunas.Length].LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            if (type == KeyValueResponseType.Set)
                return;
            await Task.Delay(20, ct);
        }
        throw new InvalidOperationException($"Could not write {key}.");
    }

    // Every key in its own bucket, so the keys hash onto every data partition.
    private static string Key(int i) => $"bucket{i}/key";

    private static async Task<T> RetryRetryable<T>(Func<Task<T>> take)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await take();
            }
            catch (KahunaBackupException ex) when (attempt < 5 && ex.Outcome is KahunaBackupOutcome.CutUnverified
                                                       or KahunaBackupOutcome.TopologyChanged)
            {
                await Task.Delay(100 * attempt, TestContext.Current.CancellationToken);
            }
        }
    }

    private static IPersistenceBackend OpenRestored(string storage, string dir) => storage switch
    {
        "rocksdb" => new RocksDbPersistenceBackend(dir, Revision),
        "sqlite" => new SqlitePersistenceBackend(dir, Revision),
        _ => MemoryPersistenceBackend.OpenCheckpoint(dir)
    };

    private static void AssertRows(IPersistenceBackend backend, IReadOnlyDictionary<string, string> expected)
    {
        foreach ((string key, string value) in expected)
        {
            KeyValueEntry? entry = backend.GetKeyValue(key);
            Assert.True(entry is not null, $"{key} is missing");
            Assert.Equal(value, Encoding.UTF8.GetString(entry.Value!));
        }
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public async Task CoordinatedBackup_CoversEveryPartition_Restores_ExtendsAndSeeds(string storage)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string backupDir = Path.Combine(tempRoot, "backups");

        (IRaft[] rafts, IKahuna[] kahunas, _) = await AssembleAsync(storage, backupDir);

        try
        {
            int leader = await MetaLeaderIndex(rafts);
            Assert.NotEmpty(NotHosted(rafts[leader]));

            Dictionary<string, string> expected = [];
            for (int i = 0; i < 40; i++)
            {
                expected[Key(i)] = "first-" + i;
                await WriteAsync(kahunas, Key(i), expected[Key(i)]);
            }

            // The newest write is on a partition the coordinator does not host. The cut must still hold it: it is
            // agreed from every share's newest committed entry, not from the coordinator's partitions alone.
            Kahuna.Server.KeyValues.Ranges.DataPartitionRouter router = new(rafts[leader]);
            string newest = Enumerable.Range(0, 400).Select(i => $"newest{i}/key").First(k => !rafts[leader].HostsPartition(router.Locate(k)));
            await Task.Delay(20, ct);
            expected[newest] = "newest";
            await WriteAsync(kahunas, newest, "newest");

            KahunaBackupInfo full = await RetryRetryable(() => kahunas[leader].TakeCoordinatedBackupAsync(ct));

            BackupCatalog catalog = new(new LocalDirectoryStorageTarget(backupDir));
            BackupManifest fullManifest = (await catalog.GetAsync(full.BackupId, ct))!;
            List<int> everyPartition = [.. Enumerable.Range(1, Partitions)];
            Assert.Equal(everyPartition, fullManifest.ClusterPartitions!.Order());
            Assert.Equal(everyPartition, fullManifest.CoveredPartitions!.Order());

            // Another node accepts the chain, and the restore holds every row of every partition.
            int other = (leader + 1) % Nodes;
            Assert.Single(await kahunas[other].GetBackupChainAsync(full.BackupId, ct));

            string restoreFull = Path.Combine(NewDir("restore"), "full");
            await kahunas[other].RestoreToAsync(full.BackupId, restoreFull, 0, ct);
            IPersistenceBackend restored = OpenRestored(storage, restoreFull);
            try { AssertRows(restored, expected); }
            finally { (restored as IDisposable)?.Dispose(); }

            // An incremental, asked of a node that neither coordinates nor hosts every partition, covers them all.
            int extender = Enumerable.Range(0, Nodes).First(i => i != leader && NotHosted(rafts[i]).Count > 0);
            for (int i = 0; i < 40; i += 3)
            {
                expected[Key(i)] = "second-" + i;
                await WriteAsync(kahunas, Key(i), expected[Key(i)]);
            }
            for (int i = 40; i < 50; i++)
            {
                expected[Key(i)] = "new-" + i;
                await WriteAsync(kahunas, Key(i), expected[Key(i)]);
            }

            KahunaBackupInfo incremental = await kahunas[extender].TakeIncrementalBackupAsync(full.BackupId, ct);
            Assert.Equal("Incremental", incremental.Type);
            Assert.True(string.IsNullOrEmpty(incremental.SubstitutionReason), incremental.SubstitutionReason);

            BackupManifest incrementalManifest = (await catalog.GetAsync(incremental.BackupId, ct))!;
            Assert.Equal(everyPartition, incrementalManifest.CoveredPartitions!.Order());
            Assert.Equal(2, (await kahunas[leader].GetBackupChainAsync(incremental.BackupId, ct)).Count);

            string restoreChain = Path.Combine(NewDir("restore"), "chain");
            await kahunas[other].RestoreToAsync(incremental.BackupId, restoreChain, 0, ct);
            restored = OpenRestored(storage, restoreChain);
            try { AssertRows(restored, expected); }
            finally { (restored as IDisposable)?.Dispose(); }

            // Only the published backups remain: no share ever entered the shared directory.
            Assert.Equal(
                new[] { full.BackupId, incremental.BackupId }.Order(),
                (await new LocalDirectoryArtifactStore(backupDir).ListBackupIdsAsync(ct)).Order());

            if (storage != "memory")
                await AssertSeedsFromLatest(storage, backupDir, incremental, incrementalManifest, expected);
        }
        finally
        {
            foreach (IRaft raft in rafts)
                await LeaveCluster(raft);
        }
    }

    /// <summary>A joining node with <c>--pitr-bootstrap-from latest</c>: the base image, then the chain replay.</summary>
    private async Task AssertSeedsFromLatest(
        string storage, string backupDir, KahunaBackupInfo leaf, BackupManifest leafManifest,
        IReadOnlyDictionary<string, string> expected)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = NewDir("joiner");
        DateTime now = DateTime.UtcNow;

        PitrBootstrapPlan plan = await BootstrapBaseImage.PrepareAsync(new BootstrapBaseImageRequest(
            storage, storagePath, Revision,
            new BackupStoragePair(new LocalDirectoryStorageTarget(backupDir), new LocalDirectoryArtifactStore(backupDir)),
            null, HLCTimestamp.Zero, TimeSpan.FromHours(1), TimeSpan.FromMinutes(30), now,
            ManifestMacKey: null, null, false, ThrottleBytesPerSec: 0), ct);

        Assert.Equal(PitrBootstrapAction.Seed, plan.Action);
        Assert.Equal(leaf.BackupId, plan.LeafBackupId);

        IReadOnlyList<BackupManifest> chain = await new BackupCatalog(new LocalDirectoryStorageTarget(backupDir))
            .ResolveAndValidateAsync(leaf.BackupId, ct);

        IPersistenceBackend backend = OpenRestored(storage, storagePath);
        try
        {
            InMemoryWAL wal = new(raftLogger);
            await BootstrapHelper.BootstrapNodeAsync(chain, new LocalDirectoryArtifactStore(backupDir), HLCTimestamp.Zero,
                backend, wal, TimeSpan.FromHours(1), now, TimeSpan.FromMinutes(30));

            AssertRows(backend, expected);

            foreach (PartitionBackupRange range in leafManifest.PartitionRanges)
                Assert.Equal(range.ToIndex, wal.GetLastCheckpoint(range.PartitionId));
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A durable transaction over partitions the coordinator does not host is decided but not settled, and a
    /// later write lands on a partition the coordinator hosts. The coordinator sees neither intent, so a cut it
    /// chose alone would sit above the transaction; the share that holds the intents proposes the transaction's
    /// commit timestamp, and the agreed cut sits below it. Once the transaction settles, the next backup holds
    /// both of its writes.
    /// </summary>
    [Fact]
    public async Task CoordinatedBackup_TransactionInFlightOnAnotherNodesShare_CutsBelowIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string backupDir = Path.Combine(tempRoot, "backups");

        (IRaft[] rafts, IKahuna[] kahunas, _) = await AssembleAsync("memory", backupDir);

        try
        {
            int leader = await MetaLeaderIndex(rafts);
            HashSet<int> elsewhere = [.. NotHosted(rafts[leader])];
            Assert.NotEmpty(elsewhere);

            DataPartitionRouterProbe router = new(rafts[leader]);
            List<string> remoteKeys = [.. Enumerable.Range(0, 400).Select(i => $"txn{i}/key").Where(k => elsewhere.Contains(router.Locate(k)))];
            string keyA = remoteKeys[0];
            string keyB = remoteKeys.FirstOrDefault(k => router.Locate(k) != router.Locate(keyA)) ?? remoteKeys[1];
            string localKey = Enumerable.Range(0, 400).Select(i => $"local{i}/key").First(k => !elsewhere.Contains(router.Locate(k)));

            // The transaction runs on a node other than the coordinator, which holds its settlement open.
            int runner = (leader + 1) % Nodes;
            KahunaManager runnerManager = (KahunaManager)kahunas[runner];
            TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);
            runnerManager.TransactionCoordinator.DurableFinalizerForTests.TestBeforeDeferredResolutionHook = hookCt => open.Task.WaitAsync(hookCt);

            HLCTimestamp commitTimestamp;
            try
            {
                KeyValueTransactionResult result = await kahunas[runner].TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes($"BEGIN SET `{keyA}` 'v1' SET `{keyB}` 'v1' COMMIT END"), null, null).WaitAsync(ct);
                Assert.True(result.Type == KeyValueResponseType.Set, $"commit answered {result.Type}: {result.Reason}");

                // Every replica of the partition holds the intent, all with the transaction's one commit timestamp.
                List<PreparedIntent> intents = HeldIntents(kahunas, keyA);
                Assert.NotEmpty(intents);
                commitTimestamp = intents[0].CommitTimestamp;
                Assert.All(intents, i => Assert.Equal(commitTimestamp, i.CommitTimestamp));

                // After the commit, so the coordinator's own view of its partitions reaches past the transaction.
                // Each node keeps its own clock, so the write repeats until its timestamp is past the commit.
                HLCTimestamp laterWrite = HLCTimestamp.Zero;
                for (int attempt = 0; attempt < 100 && laterWrite.CompareTo(commitTimestamp) <= 0; attempt++)
                {
                    (KeyValueResponseType laterType, _, _) = await kahunas[leader].LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, localKey, Encoding.UTF8.GetBytes("later"), null, -1,
                        KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                    Assert.Equal(KeyValueResponseType.Set, laterType);

                    (_, ReadOnlyKeyValueEntry? written) = await kahunas[leader].LocateAndTryGetValue(
                        HLCTimestamp.Zero, localKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                    laterWrite = written?.LastModified ?? HLCTimestamp.Zero;
                    await Task.Delay(5, ct);
                }
                Assert.True(laterWrite.CompareTo(commitTimestamp) > 0, $"later write {laterWrite}, commit {commitTimestamp}");

                KahunaBackupInfo held = await kahunas[leader].TakeCoordinatedBackupAsync(ct);
                BackupManifest heldManifest = (await new BackupCatalog(new LocalDirectoryStorageTarget(backupDir)).GetAsync(held.BackupId, ct))!;

                Assert.True(heldManifest.BaseCut!.Value.CompareTo(commitTimestamp) < 0,
                    $"cut {heldManifest.BaseCut} must sit below the decided, unsettled commit at {commitTimestamp}");

                MemoryPersistenceBackend heldImage = MemoryPersistenceBackend.OpenCheckpoint(
                    Path.Combine(backupDir, held.BackupId.ToString("N"), "checkpoint"));
                Assert.Null(heldImage.GetKeyValue(keyA));
                Assert.Null(heldImage.GetKeyValue(keyB));
            }
            finally
            {
                runnerManager.TransactionCoordinator.DurableFinalizerForTests.TestBeforeDeferredResolutionHook = null;
                open.TrySetResult();
            }

            await WaitUntilAsync(() => HeldIntents(kahunas, keyA).Count == 0 && HeldIntents(kahunas, keyB).Count == 0);

            KahunaBackupInfo settled = await RetryRetryable(() => kahunas[leader].TakeCoordinatedBackupAsync(ct));
            MemoryPersistenceBackend settledImage = MemoryPersistenceBackend.OpenCheckpoint(
                Path.Combine(backupDir, settled.BackupId.ToString("N"), "checkpoint"));
            Assert.Equal("v1", Encoding.UTF8.GetString(settledImage.GetKeyValue(keyA)!.Value!));
            Assert.Equal("v1", Encoding.UTF8.GetString(settledImage.GetKeyValue(keyB)!.Value!));
            Assert.Equal("later", Encoding.UTF8.GetString(settledImage.GetKeyValue(localKey)!.Value!));
        }
        finally
        {
            foreach (IRaft raft in rafts)
                await LeaveCluster(raft);
        }
    }

    private sealed class DataPartitionRouterProbe(IRaft raft)
    {
        private readonly Kahuna.Server.KeyValues.Ranges.DataPartitionRouter router = new(raft);

        public int Locate(string key) => router.Locate(key);
    }

    private static List<PreparedIntent> HeldIntents(IKahuna[] kahunas, string key) =>
        [.. kahunas.SelectMany(k => ((KahunaManager)k).DurablePreparedIntentStore.SnapshotPrefix(key))];

    /// <summary>
    /// One node that hosts a partition the coordinator does not is off the inter-node transport. Every partition
    /// has two other voters, so the next attempt captures that node's partitions from them, and the backup
    /// still covers every partition.
    /// </summary>
    [Fact]
    public async Task CoordinatedBackup_ShareOnAnUnreachableNode_IsTakenFromAnotherReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string backupDir = Path.Combine(tempRoot, "backups");

        (IRaft[] rafts, IKahuna[] kahunas, MemoryInterNodeCommmunication interComm) = await AssembleAsync("memory", backupDir);

        try
        {
            int leader = await MetaLeaderIndex(rafts);
            List<int> elsewhere = NotHosted(rafts[leader]);
            Assert.NotEmpty(elsewhere);

            Dictionary<string, string> expected = [];
            for (int i = 0; i < 30; i++)
            {
                expected[Key(i)] = "v" + i;
                await WriteAsync(kahunas, Key(i), expected[Key(i)]);
            }

            // Every remote node that hosts a partition the coordinator does not is a candidate; take each one
            // off the transport in turn, so whichever the assignment picks first is unreachable.
            int[] candidates = [.. Enumerable.Range(0, Nodes).Where(i => i != leader && elsewhere.Any(rafts[i].HostsPartition))];
            Assert.NotEmpty(candidates);

            foreach (int lost in candidates)
            {
                interComm.SetNodes(Enumerable.Range(0, Nodes).Where(i => i != lost)
                    .ToDictionary(i => rafts[i].GetLocalEndpoint(), i => kahunas[i]));

                KahunaBackupInfo backup = await RetryRetryable(() => kahunas[leader].TakeCoordinatedBackupAsync(ct));

                BackupManifest manifest = (await new BackupCatalog(new LocalDirectoryStorageTarget(backupDir)).GetAsync(backup.BackupId, ct))!;
                Assert.Equal(Enumerable.Range(1, Partitions), manifest.CoveredPartitions!.Order());

                MemoryPersistenceBackend image = MemoryPersistenceBackend.OpenCheckpoint(
                    Path.Combine(backupDir, backup.BackupId.ToString("N"), "checkpoint"));
                AssertRows(image, expected);
            }
        }
        finally
        {
            interComm.SetNodes(Enumerable.Range(0, Nodes).ToDictionary(i => rafts[i].GetLocalEndpoint(), i => kahunas[i]));
            foreach (IRaft raft in rafts)
                await LeaveCluster(raft);
        }
    }

    [Fact]
    public async Task CoordinatedBackup_ShareThatCannotBeReached_PublishesNothing_AndNamesItsPartitions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string backupDir = Path.Combine(tempRoot, "backups");

        (IRaft[] rafts, IKahuna[] kahunas, MemoryInterNodeCommmunication interComm) = await AssembleAsync("memory", backupDir);

        try
        {
            int leader = await MetaLeaderIndex(rafts);
            List<int> elsewhere = NotHosted(rafts[leader]);
            Assert.NotEmpty(elsewhere);

            for (int i = 0; i < 20; i++)
                await WriteAsync(kahunas, Key(i), "v" + i);

            // Every other node drops off the inter-node transport, so every remote share fails.
            interComm.SetNodes(new Dictionary<string, IKahuna> { [rafts[leader].GetLocalEndpoint()] = kahunas[leader] });

            KahunaBackupException ex = await Assert.ThrowsAsync<KahunaBackupException>(
                () => kahunas[leader].TakeCoordinatedBackupAsync(ct));

            Assert.Equal(KahunaBackupOutcome.PartitionCaptureFailed, ex.Outcome);
            foreach (int partitionId in elsewhere)
                Assert.Contains(partitionId.ToString(), ex.Message, StringComparison.Ordinal);
            Assert.Contains("on node localhost:", ex.Message, StringComparison.Ordinal);

            Assert.Empty(await new BackupCatalog(new LocalDirectoryStorageTarget(backupDir)).ListAsync(ct));
            Assert.Empty(await new LocalDirectoryArtifactStore(backupDir).ListBackupIdsAsync(ct));
        }
        finally
        {
            interComm.SetNodes(Enumerable.Range(0, Nodes).ToDictionary(i => rafts[i].GetLocalEndpoint(), i => kahunas[i]));
            foreach (IRaft raft in rafts)
                await LeaveCluster(raft);
        }
    }
}
