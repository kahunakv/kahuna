using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
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
/// The coordinated backup's cut against durable transactions. A durable transaction's pending mutation lives in
/// the prepared-intent store, not in the actor's write intent, and its commit timestamp is minted on the
/// coordinator before any prepare lands. The cut selection reads the store, and the backup verifies its cut after
/// the capture against an observation of every intent this node held from before the choice: an image that a
/// transaction at or below the cut may have been torn across is discarded, and a new cut is chosen.
/// </summary>
public sealed class TestPitrCutDurableIntents : IDisposable
{
    private readonly ILoggerFactory loggerFactory;

    private readonly string tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_pitrcut_" + Guid.NewGuid().ToString("N"));

    public TestPitrCutDurableIntents(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    public void Dispose()
    {
        if (Directory.Exists(tempRoot))
            Directory.Delete(tempRoot, recursive: true);
    }

    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private static PreparedIntent Intent(string key, long commitPhysical, long transactionPhysical = 50) =>
        new(
            TransactionId: Ts(transactionPhysical), Epoch: 1, Key: key, ManifestHash: 7, RecordAnchorKey: key,
            CommitTimestamp: Ts(commitPhysical),
            State: KeyValueState.Set, Value: [1], Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
            RecoveryDeadline: Ts(1_000_000), Resolution: PreparedIntentResolution.Pending);

    // ── the store: the selection minimum and the observation ───────────────────────────────────

    [Fact]
    public void MinUnsettledCommitTimestamp_CountsPendingAndCommittedButNotAborted()
    {
        PreparedIntentStore store = new();
        Assert.Equal(HLCTimestamp.Zero, store.MinUnsettledCommitTimestamp());

        store.Apply(new PrepareIntentCommand(Intent("a", 300)));
        store.Apply(new PrepareIntentCommand(Intent("b", 200, transactionPhysical: 60)));
        store.Apply(new PrepareIntentCommand(Intent("c", 100, transactionPhysical: 70)));
        store.Apply(new ResolveIntentCommand(Ts(70), 1, "c", Commit: false));
        store.Apply(new ResolveIntentCommand(Ts(60), 1, "b", Commit: true));

        // c aborted: it never installs a row. b committed but is still in the store: its row may not be installed
        // on every partition yet.
        Assert.Equal(Ts(200), store.MinUnsettledCommitTimestamp());
    }

    [Fact]
    public void CommitObservation_SeesLiveIntentsAndEveryInstallUntilDisposed()
    {
        PreparedIntentStore store = new();

        // Settled before the observation opens: not seen.
        store.Apply(new PrepareIntentCommand(Intent("early", 100, transactionPhysical: 10)));
        store.Apply(new ResolveIntentCommand(Ts(10), 1, "early", Commit: true));
        store.Apply(new RemoveIntentCommand(Ts(10), 1, "early"));

        store.Apply(new PrepareIntentCommand(Intent("live", 500, transactionPhysical: 20)));

        using PreparedIntentCommitObservation observation = store.BeginCommitObservation();
        Assert.Equal(Ts(500), observation.MinCommitTimestamp);

        // Installed and settled while the observation is open: still seen.
        store.Apply(new PrepareIntentCommand(Intent("passing", 400, transactionPhysical: 30)));
        store.Apply(new ResolveIntentCommand(Ts(30), 1, "passing", Commit: true));
        store.Apply(new RemoveIntentCommand(Ts(30), 1, "passing"));
        Assert.Equal(Ts(400), observation.MinCommitTimestamp);

        // An intent installed already aborted (a transferred, resolved intent) never installs a row.
        store.ImportIntents([Intent("aborted", 300, transactionPhysical: 40) with { Resolution = PreparedIntentResolution.Aborted }]);
        Assert.Equal(Ts(400), observation.MinCommitTimestamp);

        observation.Dispose();
        store.Apply(new PrepareIntentCommand(Intent("after", 200, transactionPhysical: 45)));
        Assert.Equal(Ts(400), observation.MinCommitTimestamp);
    }

    [Fact]
    public void CommitObservation_SeesAnIntentImportedByAStateTransfer()
    {
        PreparedIntentStore store = new();
        using PreparedIntentCommitObservation observation = store.BeginCommitObservation();

        store.ImportIntents([Intent("moved", 250)]);

        Assert.Equal(Ts(250), observation.MinCommitTimestamp);
    }

    // ── the coordinated backup verifies its cut after the capture ──────────────────────────────

    private string BackupDir(string tag) => Path.Combine(tempRoot, "bak_" + tag);

    private static InMemoryWAL Wal(params long[] times)
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        List<RaftLog> logs = [];
        for (int i = 0; i < times.Length; i++)
            logs.Add(new RaftLog { Id = i + 1, Type = RaftLogType.Committed, Time = Ts(times[i]) });
        wal.Write([(1, logs)]);
        return wal;
    }

    private BackupService Service(
        string tag, InMemoryWAL wal, PreparedIntentStore store, Func<Task<HLCTimestamp>> queryMinInFlight, Func<Task>? flush = null)
    {
        TestBackupService.StubRaft raft = new(wal, [new RaftPartitionRange { PartitionId = 1, State = RaftPartitionState.Active }])
        {
            IsLeader = true
        };

        return new BackupService(
            raft,
            new MemoryPersistenceBackend(),
            BackupDir(tag),
            BackupTestStores.Manifests(BackupDir(tag)),
            BackupTestStores.Artifacts(BackupDir(tag)),
            storageType: "memory",
            storageRevision: "",
            flushBeforeCheckpoint: flush ?? (() => Task.CompletedTask),
            queryMinInFlight: queryMinInFlight,
            beginCommitObservation: store.BeginCommitObservation);
    }

    /// <summary>
    /// The selection misses a durable transaction whose commit timestamp is below the newest committed entry —
    /// the shape of a prepare that applies on this node after the cut is chosen. Every attempt's verification
    /// finds it, so no image is published, and the failure is the retryable cut outcome.
    /// </summary>
    [Fact]
    public async Task CoordinatedBackup_CutPassedByAnInFlightDurableCommit_PublishesNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent("acct/1", 150)));

        int selections = 0;
        BackupService service = Service("overtaken", Wal(100, 200), store, () =>
        {
            Interlocked.Increment(ref selections);
            return Task.FromResult(HLCTimestamp.Zero);
        });

        BackupDriverException error = await Assert.ThrowsAsync<BackupDriverException>(() => service.TakeCoordinatedBackupAsync(ct));

        Assert.True(error.CutUnverified);
        Assert.Equal(BackupService.MaxCoordinatedCutAttempts, selections);
        Assert.Empty(await new BackupCatalog(BackupTestStores.Manifests(BackupDir("overtaken"))).ListAsync(ct));
    }

    /// <summary>
    /// A durable prepare lands between the cut choice and the capture of the first attempt, at a commit timestamp
    /// below that cut. The first image is discarded; the second attempt sees the intent, cuts below it, and
    /// publishes one backup whose cut the transaction cannot reach.
    /// </summary>
    [Fact]
    public async Task CoordinatedBackup_PrepareLandsAfterTheCutChoice_RetriesBelowIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        PreparedIntentStore store = new();
        int flushes = 0;

        BackupService service = Service("retry", Wal(100, 200), store,
            () => Task.FromResult(store.MinUnsettledCommitTimestamp()),
            flush: () =>
            {
                if (Interlocked.Increment(ref flushes) == 1)
                    store.Apply(new PrepareIntentCommand(Intent("acct/1", 150)));
                return Task.CompletedTask;
            });

        KahunaBackupInfo info = await service.TakeCoordinatedBackupAsync(ct);

        Assert.Equal(2, flushes);

        BackupCatalog catalog = new(BackupTestStores.Manifests(BackupDir("retry")));
        BackupManifest manifest = Assert.Single(await catalog.ListAsync(ct));
        Assert.Equal(info.BackupId, manifest.BackupId);
        Assert.NotNull(manifest.BaseCut);
        Assert.True(manifest.BaseCut!.Value.CompareTo(Ts(150)) < 0, $"cut {manifest.BaseCut} must sit below the in-flight commit at {Ts(150)}");
    }

    // ── end to end: a decided durable transaction that has not settled yet ─────────────────────

    private static string? ValueOf(MemoryPersistenceBackend backend, string key)
    {
        object? entry = backend.GetKeyValue(key);
        byte[]? bytes = entry?.GetType().GetProperty("Value")?.GetValue(entry) as byte[];
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    /// <summary>Holds the decision→settlement window of every durable commit open until released.</summary>
    private sealed class ResolutionGate : IDisposable
    {
        private readonly TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly DurableTransactionFinalizer finalizer;

        public ResolutionGate(KahunaManager manager)
        {
            finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
            finalizer.TestBeforeDeferredResolutionHook = ct => open.Task.WaitAsync(ct);
        }

        public void Release()
        {
            finalizer.TestBeforeDeferredResolutionHook = null;
            open.TrySetResult();
        }

        public void Dispose() => Release();
    }

    /// <summary>
    /// A durable transaction over two partitions is decided and acknowledged, but its settlement has not run:
    /// no participant row is installed, and its intents live only in the prepared-intent store. A coordinated
    /// backup must cut below its commit timestamp — an image at or above it would claim a state that includes a
    /// decided commit it does not hold. Once the transaction settles, the next coordinated backup includes both
    /// of its writes.
    /// </summary>
    [Fact]
    public async Task CoordinatedBackup_DecidedUnsettledTransaction_CutsBelowItAndLaterIncludesItWhole()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string backupDir = BackupDir("embedded");

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableDeferredSettlement = true,
            BackupDir = backupDir
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("pitrcut/seed", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        string keyA = $"pitrcut-{Guid.NewGuid().ToString("N")[..8]}/a";
        string keyB = KeyOnAnotherPartition(node, keyA);

        HLCTimestamp commitTimestamp;
        using (ResolutionGate gate = new(manager))
        {
            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN SET `{keyA}` 'v1' SET `{keyB}` 'v1' COMMIT END"), null, null).WaitAsync(ct);
            Assert.True(result.Type == KeyValueResponseType.Set, $"commit answered {result.Type}: {result.Reason}");

            PreparedIntent intentA = Assert.Single(manager.DurablePreparedIntentStore.SnapshotPrefix(keyA));
            PreparedIntent intentB = Assert.Single(manager.DurablePreparedIntentStore.SnapshotPrefix(keyB));
            Assert.Equal(intentA.CommitTimestamp, intentB.CommitTimestamp);
            commitTimestamp = intentA.CommitTimestamp;

            KahunaBackupInfo held = await node.Kahuna.TakeCoordinatedBackupAsync(ct);
            (BackupManifest heldManifest, MemoryPersistenceBackend heldImage) = await OpenAsync(backupDir, held.BackupId, ct);

            Assert.True(heldManifest.BaseCut!.Value.CompareTo(commitTimestamp) < 0,
                $"cut {heldManifest.BaseCut} must sit below the decided, unsettled commit at {commitTimestamp}");
            Assert.Null(ValueOf(heldImage, keyA));
            Assert.Null(ValueOf(heldImage, keyB));

            gate.Release();
        }

        long deadline = Environment.TickCount64 + 10_000;
        while (manager.DurablePreparedIntentStore.SnapshotPrefix(keyA).Count > 0
               || manager.DurablePreparedIntentStore.SnapshotPrefix(keyB).Count > 0)
        {
            Assert.True(Environment.TickCount64 < deadline, "the released transaction did not settle");
            await Task.Delay(20, ct);
        }

        KahunaBackupInfo settled = await node.Kahuna.TakeCoordinatedBackupAsync(ct);
        (BackupManifest settledManifest, MemoryPersistenceBackend settledImage) = await OpenAsync(backupDir, settled.BackupId, ct);

        Assert.True(settledManifest.BaseCut!.Value.CompareTo(commitTimestamp) >= 0);
        Assert.Equal("v1", ValueOf(settledImage, keyA));
        Assert.Equal("v1", ValueOf(settledImage, keyB));
    }

    private static async Task<(BackupManifest, MemoryPersistenceBackend)> OpenAsync(string backupDir, Guid backupId, CancellationToken ct)
    {
        BackupManifest? manifest = await new BackupCatalog(BackupTestStores.Manifests(backupDir)).GetAsync(backupId, ct);
        Assert.NotNull(manifest);
        Assert.NotNull(manifest!.BaseCut);

        string checkpoint = Path.Combine(backupDir, backupId.ToString("N"), "checkpoint");
        return (manifest, MemoryPersistenceBackend.OpenCheckpoint(checkpoint));
    }

    /// <summary>A key on a different partition from <paramref name="key"/>. Keys route by their bucket, so the
    /// candidates vary the bucket.</summary>
    private static string KeyOnAnotherPartition(EmbeddedKahunaNode node, string key)
    {
        int partition = node.Raft.GetPartitionKey(key);
        for (int i = 0; i < 256; i++)
        {
            string candidate = $"pitrcut-other{i}-{Guid.NewGuid().ToString("N")[..6]}/b";
            if (node.Raft.GetPartitionKey(candidate) != partition)
                return candidate;
        }

        throw new InvalidOperationException("no key routes to another partition");
    }
}
