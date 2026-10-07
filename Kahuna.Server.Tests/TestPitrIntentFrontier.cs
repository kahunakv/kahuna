using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A durable transaction prepared inside a full backup's range and settled after it. The settle (or the
/// by-reference record) carries no value and the incrementals' replay starts after the full's range, so the restore
/// expands it from the intent frontier the full captured at its range ends. A frontier the backup cannot prove
/// complete refuses the backup instead of publishing a chain base no restore can use.
/// </summary>
public sealed class TestPitrIntentFrontier : IDisposable
{
    private const int PartitionId = 1;

    private readonly ILoggerFactory loggerFactory;

    private readonly string tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_pitrfrontier_" + Guid.NewGuid().ToString("N"));

    public TestPitrIntentFrontier(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    public void Dispose()
    {
        if (Directory.Exists(tempRoot))
            Directory.Delete(tempRoot, recursive: true);
    }

    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private static PreparedIntent Intent(string key, long commitPhysical = 300) =>
        new(
            TransactionId: Ts(1_000), Epoch: 1, Key: key, ManifestHash: 42, RecordAnchorKey: "anchor",
            CommitTimestamp: Ts(commitPhysical),
            State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes("committed:" + key), Bucket: null, Revision: 9,
            Expires: Ts(50_000), NoRevision: false, BaseRevision: 8, BaseState: KeyValueState.Set,
            RecoveryDeadline: Ts(6_000), Resolution: PreparedIntentResolution.Pending);

    private static RaftLog IntentLog(long id, long timeMs, params PreparedIntentCommand[] commands) =>
        new()
        {
            Id = id,
            Type = RaftLogType.Committed,
            Time = Ts(timeMs),
            LogType = ReplicationTypes.PreparedIntent,
            LogData = [.. PreparedIntentStore.SerializeDelta(commands)]
        };

    private static RaftLog ByReferenceLog(long id, long timeMs, PreparedIntent intent) =>
        new()
        {
            Id = id,
            Type = RaftLogType.Committed,
            Time = Ts(timeMs),
            LogType = ReplicationTypes.KeyValues,
            LogData = [.. PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true)]
        };

    private static RaftLog KeyValueLog(long id, long timeMs, string key) =>
        new()
        {
            Id = id,
            Type = RaftLogType.Committed,
            Time = Ts(timeMs),
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(new KeyValueMessage
            {
                Type = (int)KeyValueRequestType.TrySet, Key = key,
                Value = Google.Protobuf.UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(key)),
                Revision = 1, LastModifiedPhysical = timeMs
            })
        };

    private static RaftLog CheckpointLog(long id, long timeMs) =>
        new() { Id = id, Type = RaftLogType.CommittedCheckpoint, Time = Ts(timeMs) };

    private static PreparedIntentCommand[] Settle(bool materialize, params PreparedIntent[] intents) =>
        DurableTransactionFinalizer.BuildSettleCommands(intents, commit: true, materializeOnResolve: materialize);

    private static InMemoryWAL Wal(params RaftLog[] logs)
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        wal.Write([(PartitionId, [.. logs])]);
        return wal;
    }

    private static RaftPartitionRange[] Partitions() => [new() { PartitionId = PartitionId, State = RaftPartitionState.Active }];

    private static string? ValueOf(MemoryPersistenceBackend backend, string key)
    {
        object? entry = backend.GetKeyValue(key);
        byte[]? bytes = entry?.GetType().GetProperty("Value")?.GetValue(entry) as byte[];
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    /// <summary>A store that applied <paramref name="logs"/> through the live path, as the node taking the
    /// backup did.</summary>
    private static PreparedIntentStore StoreThatApplied(params RaftLog[] logs)
    {
        PreparedIntentStore store = new();
        foreach (RaftLog log in logs)
            store.Replicate(PartitionId, log);
        return store;
    }

    private sealed record Backups(string Artifacts, BackupCatalog Catalog)
    {
        public IBackupArtifactStore Store => BackupTestStores.Artifacts(Artifacts);
    }

    private Backups NewBackups(string name) =>
        new(Path.Combine(tempRoot, "artifacts_" + name),
            new BackupCatalog(new LocalDirectoryStorageTarget(Path.Combine(tempRoot, "catalog_" + name))));

    private static Task<BackupManifest> FullAsync(
        Backups backups, InMemoryWAL wal, HLCTimestamp? snapshotT = null, PreparedIntentStore? store = null) =>
        BackupDriver.RunFullAsync(
            wal, Partitions(), new MemoryPersistenceBackend(), backups.Store, backups.Catalog,
            snapshotT: snapshotT,
            walkLiveIntents: store is null ? null : store.WalkLiveIntents,
            ct: TestContext.Current.CancellationToken);

    private static async Task<MemoryPersistenceBackend> IncrementalAndRestoreAsync(
        Backups backups, InMemoryWAL wal, BackupManifest full, HLCTimestamp target)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, Partitions(), full.BackupId, backups.Store, backups.Catalog, ct: ct);

        string checkpointPath = Path.Combine(backups.Artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        IReadOnlyList<BackupManifest> chain = await backups.Catalog.ResolveAndValidateAsync(inc.BackupId, ct);

        await RestoreEngine.RestoreAsync(chain, backups.Store, target, restored, ct: ct);
        return restored;
    }

    // ── a settle after the full's range ────────────────────────────────────────────────────────

    [Fact]
    public async Task MaterializingSettleAfterTheFullRange_ExpandsFromTheFrontier()
    {
        PreparedIntent intent = Intent("acct/1");
        InMemoryWAL wal = Wal(KeyValueLog(1, 100, "seed"), IntentLog(2, 200, new PrepareIntentCommand(intent)));

        Backups backups = NewBackups("settle");
        BackupManifest full = await FullAsync(backups, wal);
        Assert.Contains(IntentFrontier.ArtifactName, full.Checksums.Keys);

        wal.Write([(PartitionId, [IntentLog(3, 310, Settle(materialize: true, intent))])]);

        MemoryPersistenceBackend restored = await IncrementalAndRestoreAsync(backups, wal, full, Ts(400));
        Assert.Equal("committed:acct/1", ValueOf(restored, "acct/1"));
    }

    [Fact]
    public async Task MaterializingSettleAfterTheFullRange_TargetBeforeTheCommit_ExcludesIt()
    {
        PreparedIntent intent = Intent("acct/1");
        InMemoryWAL wal = Wal(KeyValueLog(1, 100, "seed"), IntentLog(2, 200, new PrepareIntentCommand(intent)));

        Backups backups = NewBackups("settle_cut");
        BackupManifest full = await FullAsync(backups, wal);

        wal.Write([(PartitionId, [IntentLog(3, 310, Settle(materialize: true, intent))])]);

        MemoryPersistenceBackend restored = await IncrementalAndRestoreAsync(backups, wal, full, Ts(250));
        Assert.Null(ValueOf(restored, "acct/1"));
    }

    [Fact]
    public async Task ByReferenceRecordAfterTheFullRange_ExpandsFromTheFrontier()
    {
        PreparedIntent intent = Intent("acct/1");
        InMemoryWAL wal = Wal(KeyValueLog(1, 100, "seed"), IntentLog(2, 200, new PrepareIntentCommand(intent)));

        Backups backups = NewBackups("byref");
        BackupManifest full = await FullAsync(backups, wal);

        wal.Write([(PartitionId, [
            IntentLog(3, 305, new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)),
            ByReferenceLog(4, 310, intent),
            IntentLog(5, 320, new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key))
        ])]);

        MemoryPersistenceBackend restored = await IncrementalAndRestoreAsync(backups, wal, full, Ts(400));
        Assert.Equal("committed:acct/1", ValueOf(restored, "acct/1"));
    }

    [Fact]
    public async Task DuplicateSettleAfterTheFullRange_FirstCopyInsideIt_IsTheSameInstall()
    {
        PreparedIntent intent = Intent("acct/1");
        InMemoryWAL wal = Wal(
            KeyValueLog(1, 100, "seed"),
            IntentLog(2, 200, new PrepareIntentCommand(intent)),
            IntentLog(3, 310, Settle(materialize: true, intent)));

        Backups backups = NewBackups("duplicate");
        BackupManifest full = await FullAsync(backups, wal);

        // A second producer's copy of the same settle, replayed only by the incremental.
        wal.Write([(PartitionId, [IntentLog(4, 320, Settle(materialize: true, intent))])]);

        await IncrementalAndRestoreAsync(backups, wal, full, Ts(400));
    }

    // A second producer's copy of a materialization landing after the settle removed the intent: the first copy
    // installed the row, and a live replica treats the late copy as redundant.
    private static RaftLog[] CommitByReferenceWithLateDuplicate(PreparedIntent intent, long firstId) =>
    [
        IntentLog(firstId, 305, new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)),
        ByReferenceLog(firstId + 1, 310, intent),
        IntentLog(firstId + 2, 320, new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key)),
        ByReferenceLog(firstId + 3, 330, intent)
    ];

    [Fact]
    public async Task DuplicateByReferenceRecordAfterTheSettle_InOneIncremental_IsTheSameInstall()
    {
        PreparedIntent intent = Intent("acct/1");
        InMemoryWAL wal = Wal(KeyValueLog(1, 100, "seed"));

        Backups backups = NewBackups("byref_duplicate");
        BackupManifest full = await FullAsync(backups, wal);

        wal.Write([(PartitionId, [IntentLog(2, 200, new PrepareIntentCommand(intent)), .. CommitByReferenceWithLateDuplicate(intent, 3)])]);

        MemoryPersistenceBackend restored = await IncrementalAndRestoreAsync(backups, wal, full, Ts(400));
        Assert.Equal("committed:acct/1", ValueOf(restored, "acct/1"));
    }

    [Fact]
    public async Task DuplicateByReferenceRecordAfterTheFullRange_FirstCopyInsideIt_IsTheSameInstall()
    {
        PreparedIntent intent = Intent("acct/1");
        RaftLog[] commit = CommitByReferenceWithLateDuplicate(intent, 3);
        InMemoryWAL wal = Wal([KeyValueLog(1, 100, "seed"), IntentLog(2, 200, new PrepareIntentCommand(intent)), .. commit[..3]]);

        Backups backups = NewBackups("byref_duplicate_straddle");
        BackupManifest full = await FullAsync(backups, wal);

        wal.Write([(PartitionId, [commit[3]])]);

        await IncrementalAndRestoreAsync(backups, wal, full, Ts(400));
    }

    [Fact]
    public async Task NothingInFlight_TheFullCarriesNoFrontier()
    {
        PreparedIntent intent = Intent("acct/1");
        InMemoryWAL wal = Wal(KeyValueLog(1, 100, "seed"));

        Backups backups = NewBackups("idle");
        BackupManifest full = await FullAsync(backups, wal);

        Assert.DoesNotContain(IntentFrontier.ArtifactName, full.Checksums.Keys);
        Assert.All(full.Checksums.Keys, key => Assert.StartsWith("checkpoint/", key));

        wal.Write([(PartitionId, [IntentLog(2, 200, new PrepareIntentCommand(intent)), IntentLog(3, 310, Settle(materialize: true, intent))])]);

        MemoryPersistenceBackend restored = await IncrementalAndRestoreAsync(backups, wal, full, Ts(400));
        Assert.Equal("committed:acct/1", ValueOf(restored, "acct/1"));
    }

    [Fact]
    public async Task TamperedFrontier_FailsTheRestoreClosed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        PreparedIntent intent = Intent("acct/1");
        InMemoryWAL wal = Wal(KeyValueLog(1, 100, "seed"), IntentLog(2, 200, new PrepareIntentCommand(intent)));

        Backups backups = NewBackups("tampered");
        BackupManifest full = await FullAsync(backups, wal);

        wal.Write([(PartitionId, [IntentLog(3, 310, Settle(materialize: true, intent))])]);
        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, Partitions(), full.BackupId, backups.Store, backups.Catalog, ct: ct);

        string frontierPath = Path.Combine(backups.Artifacts, full.BackupId.ToString("N"), IntentFrontier.ArtifactName);
        byte[] bytes = await File.ReadAllBytesAsync(frontierPath, ct);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(frontierPath, bytes, ct);

        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(
            Path.Combine(backups.Artifacts, full.BackupId.ToString("N"), "checkpoint"));
        IReadOnlyList<BackupManifest> chain = await backups.Catalog.ResolveAndValidateAsync(inc.BackupId, ct);

        await Assert.ThrowsAsync<BackupArtifactException>(() => RestoreEngine.RestoreAsync(chain, backups.Store, Ts(400), restored, ct: ct));
    }

    // ── a cut below the store's applied position ───────────────────────────────────────────────

    /// <summary>
    /// A coordinated cut ends the range before the settle, but the store already applied it, so the walk no
    /// longer holds the intent. The fold reaches the prepare and the frontier still carries it.
    /// </summary>
    [Fact]
    public async Task CutBelowTheStoresAppliedPosition_IntentSettledBeforeTheWalk_FoldCarriesIt()
    {
        PreparedIntent intent = Intent("acct/1");
        RaftLog prepare = IntentLog(2, 200, new PrepareIntentCommand(intent));
        RaftLog resolve = IntentLog(3, 305, new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true));
        RaftLog materialize = ByReferenceLog(4, 310, intent);
        RaftLog remove = IntentLog(5, 320, new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key));
        InMemoryWAL wal = Wal(KeyValueLog(1, 100, "seed"), prepare, resolve, materialize, remove);

        PreparedIntentStore store = StoreThatApplied(prepare, resolve, remove);
        Assert.Equal(0, store.Count);

        Backups backups = NewBackups("coordinated");
        BackupManifest full = await FullAsync(backups, wal, snapshotT: Ts(250), store: store);
        Assert.Equal(2, Assert.Single(full.PartitionRanges).ToIndex);

        MemoryPersistenceBackend restored = await IncrementalAndRestoreAsync(backups, wal, full, Ts(400));
        Assert.Equal("committed:acct/1", ValueOf(restored, "acct/1"));
    }

    // ── a prepare the WAL no longer holds ──────────────────────────────────────────────────────

    // The prepare at index 2 was compacted away (the checkpoint at 3 is the floor); only the store still holds it.
    private static InMemoryWAL WalCompactedPastThePrepare(params RaftLog[] tail)
    {
        InMemoryWAL wal = Wal(KeyValueLog(1, 100, "seed"), CheckpointLog(3, 150), KeyValueLog(4, 180, "other"));
        if (tail.Length > 0)
            wal.Write([(PartitionId, [.. tail])]);
        return wal;
    }

    [Fact]
    public async Task PrepareBelowTheCompactionFloor_TheWalkCarriesIt()
    {
        PreparedIntent intent = Intent("acct/1");
        PreparedIntentStore store = StoreThatApplied(IntentLog(2, 200, new PrepareIntentCommand(intent)));
        InMemoryWAL wal = WalCompactedPastThePrepare();

        Backups backups = NewBackups("below_floor");
        BackupManifest full = await FullAsync(backups, wal, store: store);

        wal.Write([(PartitionId, [IntentLog(5, 310, Settle(materialize: true, intent))])]);

        MemoryPersistenceBackend restored = await IncrementalAndRestoreAsync(backups, wal, full, Ts(400));
        Assert.Equal("committed:acct/1", ValueOf(restored, "acct/1"));
    }

    /// <summary>
    /// The intent was live at the range end, settled before the walk, and its prepare is below the compaction
    /// floor: no source the backup reads holds its value. The backup is refused with the retryable cut outcome and
    /// publishes nothing.
    /// </summary>
    [Fact]
    public async Task PrepareBelowTheFloor_SettledBeforeTheWalk_RefusesTheBackup()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        PreparedIntent intent = Intent("acct/1");
        RaftLog settle = IntentLog(5, 310, Settle(materialize: true, intent));
        InMemoryWAL wal = WalCompactedPastThePrepare(settle);

        // The store applied the prepare and then the settle: the walk finds nothing.
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(intent), PartitionId);
        foreach (PreparedIntentCommand command in Settle(materialize: false, intent))
            store.Apply(command, PartitionId);
        Assert.Equal(0, store.Count);

        Backups backups = NewBackups("refused");
        BackupDriverException error = await Assert.ThrowsAsync<BackupDriverException>(
            () => FullAsync(backups, wal, snapshotT: Ts(200), store: store));

        Assert.True(error.CutUnverified);
        Assert.Contains("acct/1", error.Message);
        Assert.Empty(await backups.Catalog.ListAsync(ct));
        Assert.True(!Directory.Exists(backups.Artifacts) || Directory.GetFileSystemEntries(backups.Artifacts).Length == 0);
    }

    // ── end to end ─────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Durable multi-partition transactions run without pause while full and incremental backups are taken, so
    /// fulls land between transactions' prepares and settles. Every chain must restore, and the last one must
    /// restore exactly the values the node serves once the workload has settled.
    /// </summary>
    [Fact]
    public async Task EndToEnd_FullsTakenUnderDurableLoad_EveryChainRestores()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string backupDir = Path.Combine(tempRoot, "bak_e2e");
        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            BackupDir = backupDir
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("e2e/w0/k0", ct);

        const int workers = 6;
        const int keysPerTransaction = 4;

        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        int committed = 0;

        async Task Worker(int worker)
        {
            for (int iteration = 0; !stop.IsCancellationRequested; iteration++)
            {
                StringBuilder script = new("BEGIN ");
                for (int k = 0; k < keysPerTransaction; k++)
                    script.Append($"SET `e2e/w{worker}/k{k}` 'v{iteration}' ");
                script.Append("COMMIT END");

                KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes(script.ToString()), null, null);
                if (result.Type == KeyValueResponseType.Set)
                    Interlocked.Increment(ref committed);
            }
        }

        Task[] load = new Task[workers];
        for (int w = 0; w < workers; w++)
        {
            int worker = w;
            load[w] = Task.Run(() => Worker(worker), CancellationToken.None);
        }

        List<Guid> chainTips = [];
        Guid lastBackup = Guid.Empty;
        try
        {
            for (int round = 0; round < 4; round++)
            {
                await Task.Delay(150, ct);
                KahunaBackupInfo full = await node.Kahuna.TakeFullBackupAsync(ct);
                await Task.Delay(150, ct);
                KahunaBackupInfo inc = await node.Kahuna.TakeIncrementalBackupAsync(full.BackupId, ct);
                chainTips.Add(inc.BackupId);
                lastBackup = inc.BackupId;
            }
        }
        finally
        {
            await stop.CancelAsync();
            await Task.WhenAll(load);
        }

        Assert.True(committed > 0);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        long deadline = Environment.TickCount64 + 15_000;
        while (manager.DurablePreparedIntentStore.Count > 0)
        {
            Assert.True(Environment.TickCount64 < deadline, "every intent settled");
            await Task.Delay(20, ct);
        }

        KahunaBackupInfo final = await node.Kahuna.TakeIncrementalBackupAsync(lastBackup, ct);
        chainTips.Add(final.BackupId);

        MemoryPersistenceBackend? restoredTip = null;
        for (int i = 0; i < chainTips.Count; i++)
        {
            string targetDir = Path.Combine(tempRoot, "restored_" + i);
            await node.Kahuna.RestoreToAsync(chainTips[i], targetDir, targetTimeMs: 0, ct);
            restoredTip = MemoryPersistenceBackend.OpenCheckpoint(targetDir);
        }

        for (int w = 0; w < workers; w++)
        {
            for (int k = 0; k < keysPerTransaction; k++)
            {
                string key = $"e2e/w{w}/k{k}";
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? live) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

                if (type != KeyValueResponseType.Get)
                {
                    Assert.Null(ValueOf(restoredTip!, key));
                    continue;
                }

                Assert.Equal(Encoding.UTF8.GetString(live!.Value!), ValueOf(restoredTip!, key));
            }
        }
    }
}
