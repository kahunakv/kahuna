using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
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
/// Covers the materializing settle: a committed durable transaction's settle installs every committed value from
/// the replica's own prepared intent at the settle's apply, so no materialization record is written. The tests pin
/// the wire shape, the durability accounting of an entry that installs several rows, the store's install-before-
/// remove ordering, the restart replay, point-in-time recovery and the backup barrier, and end-to-end commits on
/// one node, across a restart, and on every replica of a three-node group.
/// </summary>
public sealed class TestMaterializeOnResolve : BaseCluster, IDisposable
{
    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private const int PartitionId = 3;

    private readonly ILoggerFactory loggerFactory;

    // Counts the convergence repair's warning on this test's own nodes. A settle whose install did not run leaves
    // the node without the row, and the repair then re-drives the commit and heals it — so a missing install
    // shows up here, not as a wrong value. The process-wide metric would also count other tests' nodes.
    private readonly RepairWarningCounter repairs = new();

    private readonly string tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_matresolve_" + Guid.NewGuid().ToString("N"));

    public TestMaterializeOnResolve(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
        loggerFactory.AddProvider(repairs);
    }

    private sealed class RepairWarningCounter : ILoggerProvider
    {
        private int count;

        public int Count => Volatile.Read(ref count);

        public ILogger CreateLogger(string categoryName) => new CountingLogger(this);

        public void Dispose() { }

        private sealed class CountingLogger(RepairWarningCounter owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (logLevel >= LogLevel.Warning
                    && formatter(state, exception).Contains("missing from this node's durable state", StringComparison.Ordinal))
                    Interlocked.Increment(ref owner.count);
            }
        }
    }

    public void Dispose()
    {
        if (Directory.Exists(tempRoot))
            Directory.Delete(tempRoot, recursive: true);
    }

    private static PreparedIntent Intent(string key, long revision, byte[]? value, KeyValueState state = KeyValueState.Set) =>
        new(
            TransactionId: Ts(1_000), Epoch: 1, Key: key, ManifestHash: 42, RecordAnchorKey: "anchor",
            CommitTimestamp: Ts(1_234),
            State: state, Value: value, Bucket: null, Revision: revision, Expires: Ts(50_000),
            NoRevision: false, BaseRevision: revision - 1, BaseState: KeyValueState.Set,
            RecoveryDeadline: Ts(6_000), Resolution: PreparedIntentResolution.Pending);

    // Copies the produced bytes so no consumer can recognize them as this process's own proposal: the decoded
    // path is the one every other replica takes.
    private static RaftLog IntentLog(long id, long timeMs, params PreparedIntentCommand[] commands) =>
        new()
        {
            Id = id,
            Type = RaftLogType.Committed,
            Time = new HLCTimestamp(0, timeMs, 0),
            LogType = ReplicationTypes.PreparedIntent,
            LogData = [.. PreparedIntentStore.SerializeDelta(commands)]
        };

    private static PreparedIntentCommand[] Settle(bool materialize, params PreparedIntent[] intents) =>
        DurableTransactionFinalizer.BuildSettleCommands(intents, commit: true, materializeOnResolve: materialize);

    // ── wire shape ──────────────────────────────────────────────────────────────

    [Fact]
    public void Codec_MaterializingSettle_RoundTripsThroughBothDecoders()
    {
        PreparedIntentCommand[] commands = Settle(
            materialize: true,
            Intent("acct/1", 9, [1, 2]),
            Intent("acct/2", 4, null, KeyValueState.Deleted));

        byte[] bytes = [.. PreparedIntentStore.SerializeDelta(commands)];

        Assert.Equal(commands, PreparedIntentStore.DecodeDelta(bytes));

        PreparedIntentDeltaMessage message = PreparedIntentDeltaMessage.Parser.ParseFrom(bytes);
        PreparedIntentCommand[] reference = [.. message.Commands.Select(m => PreparedIntentStore.ToCommand(m, message.Header))];
        Assert.Equal(commands, reference);

        ResolveIntentCommand resolve = Assert.IsType<ResolveIntentCommand>(commands[0]);
        Assert.True(resolve.MaterializeOnResolve);
        Assert.Equal(Ts(1_234), resolve.CommitTimestamp);
    }

    [Fact]
    public void Codec_PlainSettle_IsByteIdenticalToTheFormOlderNodesRead()
    {
        PreparedIntent intent = Intent("acct/1", 9, [1, 2]);

        byte[] built = PreparedIntentStore.SerializeDelta(Settle(materialize: false, intent));
        byte[] handWritten = PreparedIntentStore.SerializeDelta([
            new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true),
            new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key)]);

        Assert.Equal(handWritten, built);

        foreach (PreparedIntentCommandMessage command in PreparedIntentDeltaMessage.Parser.ParseFrom(built).Commands)
        {
            Assert.False(command.MaterializeOnResolve);
            Assert.Equal(0, command.CommitTimestampPhysical);
        }
    }

    // ── durability accounting ───────────────────────────────────────────────────

    [Fact]
    public void Tracker_EntryWithSeveralRows_StaysBelowTheFloorUntilEveryRowAndReceiptLands()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 5, DurabilityChannel.PreparedIntents);
        Assert.True(tracker.AddPendingFlushRow(1, 5));
        Assert.True(tracker.AddPending(1, 5, DurabilityChannel.Receipts));
        Assert.True(tracker.AddPendingFlushRow(1, 5));

        tracker.MarkApplied(1, 5, DurabilityChannel.PreparedIntents);
        tracker.ResolveUpTo(1, DurabilityChannel.PreparedIntents, 5);
        Assert.Equal(4, tracker.GetWatermark(1));

        // The first row's flush lands; the second row is still only queued.
        tracker.Resolve(1, 5);
        Assert.Equal(4, tracker.GetWatermark(1));

        tracker.Resolve(1, 5);
        Assert.Equal(4, tracker.GetWatermark(1));

        tracker.MarkApplied(1, 5, DurabilityChannel.Receipts);
        tracker.ResolveUpTo(1, DurabilityChannel.Receipts, 5);
        Assert.Equal(5, tracker.GetWatermark(1));
    }

    [Fact]
    public void Tracker_WideningAnIndexThatIsNotPending_ChangesNothing()
    {
        PartitionDurabilityTracker tracker = new();

        Assert.False(tracker.AddPendingFlushRow(1, 5));

        tracker.RegisterPending(1, 5, DurabilityChannel.PreparedIntents);
        tracker.MarkApplied(1, 5, DurabilityChannel.PreparedIntents);
        tracker.ResolveUpTo(1, DurabilityChannel.PreparedIntents, 5);
        Assert.Equal(5, tracker.GetWatermark(1));

        // A redelivery of an index already durable must not re-open it.
        Assert.False(tracker.AddPendingFlushRow(1, 5));
        Assert.False(tracker.AddPending(1, 5, DurabilityChannel.Receipts));
        Assert.Equal(5, tracker.GetWatermark(1));
    }

    // ── store: install before remove ────────────────────────────────────────────

    private sealed class RecordingInstaller(PreparedIntentStore store) : IResolvedIntentInstaller
    {
        public List<(long LogIndex, PreparedIntent Intent, bool Replay, bool LiveAtInstall)> Installs { get; } = [];

        public List<(long LogIndex, bool Replay)> Completed { get; } = [];

        public void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay) =>
            Installs.Add((logIndex, intent, replay, store.GetByIdentity(intent.TransactionId, intent.Epoch, intent.Key) is not null));

        public void CompleteEntry(int partitionId, long logIndex, bool replay) => Completed.Add((logIndex, replay));
    }

    private static (PreparedIntentStore Store, RecordingInstaller Installer) PreparedStore(params PreparedIntent[] intents)
    {
        PreparedIntentStore store = new();
        RecordingInstaller installer = new(store);
        store.AttachResolvedIntentInstaller(installer);

        foreach (PreparedIntent intent in intents)
            Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(intent), PartitionId).Outcome);

        return (store, installer);
    }

    [Fact]
    public void Store_MaterializingSettle_InstallsEveryIntentWhileItIsStillLive()
    {
        PreparedIntent first = Intent("acct/1", 9, [1]);
        PreparedIntent second = Intent("acct/2", 3, [2]);
        (PreparedIntentStore store, RecordingInstaller installer) = PreparedStore(first, second);

        store.ApplyDeltaAckPrepares(PartitionId, IntentLog(10, 100, Settle(materialize: true, first, second)));

        Assert.Equal(2, installer.Installs.Count);
        Assert.All(installer.Installs, install =>
        {
            Assert.Equal(10, install.LogIndex);
            Assert.False(install.Replay);
            Assert.True(install.LiveAtInstall);
        });
        Assert.Equal(["acct/1", "acct/2"], installer.Installs.Select(install => install.Intent.Key));
        Assert.Equal([(10L, false)], installer.Completed);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void Store_PlainSettle_InstallsNothing()
    {
        PreparedIntent intent = Intent("acct/1", 9, [1]);
        (PreparedIntentStore store, RecordingInstaller installer) = PreparedStore(intent);

        store.ApplyDeltaAckPrepares(PartitionId, IntentLog(10, 100, Settle(materialize: false, intent)));

        Assert.Empty(installer.Installs);
        Assert.Empty(installer.Completed);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void Store_MaterializingSettleOverAnAbortedIntent_InstallsNothing()
    {
        PreparedIntent intent = Intent("acct/1", 9, [1]);
        (PreparedIntentStore store, RecordingInstaller installer) = PreparedStore(intent);

        store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: false), PartitionId);

        store.ApplyDeltaAckPrepares(PartitionId, IntentLog(10, 100, Settle(materialize: true, intent)));

        Assert.Empty(installer.Installs);
    }

    [Fact]
    public void Store_LiveDuplicateSettleAfterTheFirst_InstallsNothing()
    {
        PreparedIntent intent = Intent("acct/1", 9, [1]);
        (PreparedIntentStore store, RecordingInstaller installer) = PreparedStore(intent);

        store.ApplyDeltaAckPrepares(PartitionId, IntentLog(10, 100, Settle(materialize: true, intent)));
        store.ApplyDeltaAckPrepares(PartitionId, IntentLog(11, 110, Settle(materialize: true, intent)));

        Assert.Single(installer.Installs);
        Assert.Equal([(10L, false)], installer.Completed);
    }

    [Fact]
    public void Store_ReplayAfterTheSettle_InstallsFromTheSettledIntentStillAwaitingItsFlush()
    {
        PreparedIntent intent = Intent("acct/1", 9, [1]);
        (PreparedIntentStore store, RecordingInstaller installer) = PreparedStore(intent);

        // Every row reads as queued but not yet durable, so the settle retains the intent.
        store.AttachUnflushedRowProbe((_, _) => true);

        RaftLog settle = IntentLog(10, 100, Settle(materialize: true, intent));
        store.ApplyDeltaAckPrepares(PartitionId, settle);
        Assert.Equal(1, store.SettledIntentsAwaitingFlushCount);

        // A restart replays the settle over a snapshot written after it: the live set no longer holds the intent.
        store.Restore(PartitionId, settle);

        Assert.Equal(2, installer.Installs.Count);
        Assert.True(installer.Installs[1].Replay);
        Assert.Equal(intent.Revision, installer.Installs[1].Intent.Revision);
        Assert.Equal([(10L, false), (10L, true)], installer.Completed);
    }

    // ── restorer ────────────────────────────────────────────────────────────────

    /// <summary>Records every live install on one replica and forwards it to the replica's real installer.</summary>
    private sealed class SpyInstaller(IResolvedIntentInstaller inner) : IResolvedIntentInstaller
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> installed = new(StringComparer.Ordinal);

        public bool Installed(string key) => installed.ContainsKey(key);

        public void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay)
        {
            if (!replay)
                installed.TryAdd(intent.Key, 0);

            inner.Install(partitionId, logIndex, intent, replay);
        }

        public void CompleteEntry(int partitionId, long logIndex, bool replay) => inner.CompleteEntry(partitionId, logIndex, replay);
    }

    private sealed class RestorerInstaller(KeyValueRestorer restorer) : IResolvedIntentInstaller
    {
        public void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay) =>
            restorer.RestoreResolvedIntent(partitionId, logIndex, intent);

        public void CompleteEntry(int partitionId, long logIndex, bool replay) =>
            restorer.CompleteResolvedIntentEntry(partitionId, logIndex);
    }

    [Fact]
    public void Restorer_ReplayedMaterializingSettle_RebuildsTheRowAndKeepsTheIntentUntilTheFlush()
    {
        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, PreparedIntentStore intents, IDisposable lifetime) =
            KeyValueRestorerHarness.Build(out _);

        using (lifetime)
        {
            intents.AttachResolvedIntentInstaller(new RestorerInstaller(restorer));
            intents.AttachUnflushedRowProbe((key, revision) =>
                overlay.TryGet(key, out UnflushedKeyValueWrite queued) && queued.Revision >= revision);

            PreparedIntent intent = Intent("acct/1", 9, [4, 5, 6]);
            intents.Apply(new PrepareIntentCommand(intent), PartitionId);

            Assert.True(intents.Restore(PartitionId, IntentLog(10, 100, Settle(materialize: true, intent))));

            Assert.True(overlay.TryGet("acct/1", out UnflushedKeyValueWrite replayed));
            Assert.Equal(9, replayed.Revision);
            Assert.Equal(new byte[] { 4, 5, 6 }, replayed.Value);
            Assert.Equal(Ts(1_234), replayed.LastModified);

            // The settle removed the live intent, and the row is only queued: the intent stays reachable for a
            // replay until the flush lands.
            Assert.Equal(0, intents.Count);
            Assert.Equal(1, intents.SettledIntentsAwaitingFlushCount);
        }
    }

    // ── point-in-time recovery and the backup barrier ───────────────────────────

    private static InMemoryWAL SeededWal()
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        wal.Write([(1, [new RaftLog
        {
            Id = 1, Type = RaftLogType.Committed, Time = Ts(100),
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(new KeyValueMessage
            {
                Type = (int)KeyValueRequestType.TrySet, Key = "seed",
                Value = UnsafeByteOperations.UnsafeWrap("seed"u8.ToArray()), Revision = 1, LastModifiedPhysical = 100
            })
        }])]);
        return wal;
    }

    private static string? ValueOf(MemoryPersistenceBackend backend, string key)
    {
        object? entry = backend.GetKeyValue(key);
        byte[]? bytes = entry?.GetType().GetProperty("Value")?.GetValue(entry) as byte[];
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    /// <summary>Takes a full backup of <paramref name="wal"/>, appends <paramref name="incremental"/>, takes an
    /// incremental, and restores the chain as of <paramref name="target"/> into a fresh backend.</summary>
    private async Task<MemoryPersistenceBackend> RestoreChainAsync(string name, InMemoryWAL wal, RaftLog[] incremental, HLCTimestamp target)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string artifacts = Path.Combine(tempRoot, "artifacts_" + name);
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(Path.Combine(tempRoot, "catalog_" + name)));

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: ct);

        wal.Write([(1, [.. incremental])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(
            wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: ct);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(inc.BackupId, ct);

        await RestoreEngine.RestoreAsync(chain, BackupTestStores.Artifacts(artifacts), target, restored, ct: ct);
        return restored;
    }

    private static PreparedIntent CommittedAt300(string key) =>
        Intent(key, revision: 9, value: Encoding.UTF8.GetBytes("committed:" + key)) with { CommitTimestamp = Ts(300) };

    [Fact]
    public async Task Pitr_ExpandsAMaterializingSettleFromTheReplayedPrepare()
    {
        PreparedIntent first = CommittedAt300("acct/1");
        PreparedIntent second = CommittedAt300("acct/2");

        MemoryPersistenceBackend restored = await RestoreChainAsync("expand", SeededWal(), [
            IntentLog(2, 200, new PrepareIntentCommand(first), new PrepareIntentCommand(second)),
            IntentLog(3, 310, Settle(materialize: true, first, second))
        ], Ts(400));

        Assert.Equal("committed:acct/1", ValueOf(restored, "acct/1"));
        Assert.Equal("committed:acct/2", ValueOf(restored, "acct/2"));
    }

    [Fact]
    public async Task Pitr_CutBeforeTheCommit_ExcludesTheTransaction()
    {
        PreparedIntent intent = CommittedAt300("acct/1");

        MemoryPersistenceBackend restored = await RestoreChainAsync("cut", SeededWal(), [
            IntentLog(2, 200, new PrepareIntentCommand(intent)),
            IntentLog(3, 310, Settle(materialize: true, intent))
        ], Ts(250));

        Assert.Null(ValueOf(restored, "acct/1"));
    }

    [Fact]
    public async Task Pitr_DuplicateMaterializingSettle_IsTheSameInstall()
    {
        PreparedIntent intent = CommittedAt300("acct/1");

        MemoryPersistenceBackend restored = await RestoreChainAsync("duplicate", SeededWal(), [
            IntentLog(2, 200, new PrepareIntentCommand(intent)),
            IntentLog(3, 310, Settle(materialize: true, intent)),
            IntentLog(4, 320, Settle(materialize: true, intent))
        ], Ts(400));

        Assert.Equal("committed:acct/1", ValueOf(restored, "acct/1"));
    }

    [Fact]
    public async Task Pitr_MaterializingSettleWithNoPrepare_FailsTheRestore()
    {
        PreparedIntent intent = CommittedAt300("acct/1");

        BackupDriverException error = await Assert.ThrowsAsync<BackupDriverException>(() =>
            RestoreChainAsync("orphan", SeededWal(), [IntentLog(2, 310, Settle(materialize: true, intent))], Ts(400)));

        Assert.Contains("acct/1", error.Message);
    }

    [Fact]
    public void BackupBarrier_CountsTheCommitTimestampOfAMaterializingSettle()
    {
        PreparedIntent intent = CommittedAt300("acct/1") with { CommitTimestamp = Ts(305) };

        InMemoryWAL wal = SeededWal();
        wal.Write([(1, [
            IntentLog(2, 200, new PrepareIntentCommand(intent)),
            IntentLog(3, 310, Settle(materialize: true, intent))
        ])]);

        Assert.Equal(Ts(305), BackupDriver.MaxCommittedKeyValueCommitHlc(wal, 1, 3, TestContext.Current.CancellationToken));
    }

    // ── end to end ──────────────────────────────────────────────────────────────

    /// <summary>What a partition's log holds for committed transactions: key/value records that carry a
    /// transaction id (materializations), and the keys a materializing settle installed.</summary>
    private static (int Materializations, HashSet<string> InstalledKeys) ScanCommittedShapes(IRaft raft)
    {
        int materializations = 0;
        HashSet<string> installed = new(StringComparer.Ordinal);

        foreach (RaftPartitionRange partition in raft.GetPartitionMap())
        {
            foreach (RaftLog log in raft.WalAdapter.ReadLogsRange(partition.PartitionId, 1))
            {
                if (log.LogData is null || log.LogData.Length == 0)
                    continue;

                if (log.LogType == ReplicationTypes.KeyValues)
                {
                    KeyValueMessage message = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);
                    if (message.TransactionIdNode != 0 || message.TransactionIdPhysical != 0 || message.TransactionIdCounter != 0)
                        materializations++;
                }
                else if (log.LogType == ReplicationTypes.PreparedIntent)
                {
                    foreach (PreparedIntentCommand command in PreparedIntentStore.DecodeDelta(log.LogData))
                    {
                        if (command is ResolveIntentCommand { Commit: true, MaterializeOnResolve: true } resolve)
                            installed.Add(resolve.Key);
                    }
                }
            }
        }

        return (materializations, installed);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string what, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 15_000;
        while (!condition())
        {
            if (Environment.TickCount64 > deadline)
                Assert.Fail("Timed out waiting until " + what);

            await Task.Delay(20, ct);
        }
    }

    private static byte[] Script(string prefix, int count)
    {
        StringBuilder script = new("BEGIN ");
        for (int i = 1; i <= count; i++)
            script.Append($"SET `{prefix}{i}` 'value-{i}' ");
        script.Append("COMMIT END");
        return Encoding.UTF8.GetBytes(script.ToString());
    }

    private static async Task AssertReadsBackAsync(IKahuna kahuna, string prefix, int count, CancellationToken ct)
    {
        for (int i = 1; i <= count; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, $"{prefix}{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal(Encoding.UTF8.GetBytes($"value-{i}"), entry!.Value);
        }
    }

    [Fact]
    public async Task EndToEnd_OnePhaseAndMultiPartitionCommits_WriteNoMaterializationRecord()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableMaterializeOnResolve = true
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("single/row-1", ct);

        // One key: the one-phase bundle. Eight keys: spread over the partitions, the two-phase path.
        KeyValueTransactionResult single = await node.Kahuna.TryExecuteTransactionScript(Script("single/row-", 1), null, null);
        Assert.Equal(KeyValueResponseType.Set, single.Type);

        KeyValueTransactionResult spread = await node.Kahuna.TryExecuteTransactionScript(Script("spread/row-", 8), null, null);
        Assert.Equal(KeyValueResponseType.Set, spread.Type);

        // Read-your-writes right after the commit, before the deferred settlement is known to have landed.
        await AssertReadsBackAsync(node.Kahuna, "single/row-", 1, ct);
        await AssertReadsBackAsync(node.Kahuna, "spread/row-", 8, ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Count == 0, "every intent settled", ct);

        (int materializations, HashSet<string> installed) = ScanCommittedShapes(node.Raft);
        Assert.Equal(0, materializations);
        Assert.Contains("single/row-1", installed);
        for (int i = 1; i <= 8; i++)
            Assert.Contains($"spread/row-{i}", installed);

        // After the settle the value is served from the installed row, not from an intent.
        await AssertReadsBackAsync(node.Kahuna, "single/row-", 1, ct);
        await AssertReadsBackAsync(node.Kahuna, "spread/row-", 8, ct);

        await Task.Delay(500, ct);
        Assert.Equal(0, repairs.Count);
    }

    [Fact]
    public async Task EndToEnd_CommittedValuesSurviveARestart()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = Path.Combine(tempRoot, "store");
        string walPath = Path.Combine(tempRoot, "wal");
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        EmbeddedKahunaOptions Options() => new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            InitialPartitions = 2,
            Storage = "sqlite",
            StoragePath = storagePath,
            StorageRevision = "matresolve",
            WalStorage = "sqlite",
            WalPath = walPath,
            WalRevision = "matresolve-wal",
            DurableMaterializeOnResolve = true
        };

        await using (EmbeddedKahunaNode node = new(Options(), loggerFactory))
        {
            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("restart/row-1", ct);

            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(Script("restart/row-", 6), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);

            KahunaManager manager = (KahunaManager)node.Kahuna;
            await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Count == 0, "every intent settled", ct);
        }

        await using (EmbeddedKahunaNode restarted = new(Options(), loggerFactory))
        {
            await restarted.StartAsync(ct);
            await restarted.WaitForLeaderForKeyAsync("restart/row-1", ct);

            await AssertReadsBackAsync(restarted.Kahuna, "restart/row-", 6, ct);
        }

        Assert.Equal(0, repairs.Count);
    }

    /// <summary>Fails every batch that carries a materializing settle, so a commit is durable but its values are
    /// never installed through the log before the crash.</summary>
    private sealed class SettleBlockingExecutor(IPartitionBatchExecutor inner) : IPartitionBatchExecutor
    {
        private int blocked;

        public int BlockedSettles => Volatile.Read(ref blocked);

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            foreach (RaftProposalEntry entry in entries)
            {
                if (entry.Type != ReplicationTypes.PreparedIntent)
                    continue;

                if (!PreparedIntentStore.DecodeDelta(entry.Data).Any(c => c is ResolveIntentCommand { MaterializeOnResolve: true }))
                    continue;

                Interlocked.Increment(ref blocked);

                List<RaftEntryResult> failed = new(entries.Count);
                for (int i = 0; i < entries.Count; i++)
                    failed.Add(new RaftEntryResult(RaftOperationStatus.Errored, -1, HLCTimestamp.Zero));

                return Task.FromResult(new RaftBatchReplicationResult(false, RaftOperationStatus.Errored, HLCTimestamp.Zero, failed));
            }

            return inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    /// <summary>
    /// A crash between the decision and the settle: the commit is durable, the values exist only in the pending
    /// intents, and no materialization record exists to replay. After a cold restart the recovery sweep settles
    /// the intents with a materializing settle and the committed values are served.
    /// </summary>
    [Fact]
    public async Task CrashBeforeTheSettle_RecoverySettlesAndInstallsTheValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = Path.Combine(tempRoot, "crash-store");
        string walPath = Path.Combine(tempRoot, "crash-wal");
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        EmbeddedKahunaOptions Options(Func<IPartitionBatchExecutor, IPartitionBatchExecutor>? decorator) => new()
        {
            InitialPartitions = 1,
            Storage = "sqlite",
            StoragePath = storagePath,
            StorageRevision = "matresolve-crash",
            WalStorage = "sqlite",
            WalPath = walPath,
            WalRevision = "matresolve-crash-wal",
            WalSyncWrites = true,
            OnePhaseApplyTimeValidation = true,
            CollectionInterval = TimeSpan.FromMinutes(10),
            DurableMaterializeOnResolve = true,
            WriteBatchExecutorDecorator = decorator
        };

        await using (EmbeddedKahunaNode node = new(Options(inner => blocker = new SettleBlockingExecutor(inner)), loggerFactory))
        {
            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("crash/row-1", ct);

            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(Script("crash/row-", 2), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);

            await WaitUntilAsync(() => blocker!.BlockedSettles > 0, "the settle is refused", ct);

            KahunaManager manager = (KahunaManager)node.Kahuna;
            Assert.Equal(2, manager.DurablePreparedIntentStore.Count);
            Assert.Empty(ScanCommittedShapes(node.Raft).InstalledKeys);
        }

        await using (EmbeddedKahunaNode restarted = new(Options(decorator: null), loggerFactory))
        {
            await restarted.StartAsync(ct);
            await restarted.WaitForLeaderForKeyAsync("crash/row-1", ct);

            KahunaManager manager = (KahunaManager)restarted.Kahuna;
            Assert.Equal(2, manager.DurablePreparedIntentStore.Count);

            long deadline = Environment.TickCount64 + 30_000;
            while (manager.DurablePreparedIntentStore.Count != 0)
            {
                Assert.True(Environment.TickCount64 < deadline, "recovery did not settle the committed intents");
                await manager.KeyValues.RecoverPreparedIntents(ct);
                await Task.Delay(50, ct);
            }

            (_, HashSet<string> installed) = ScanCommittedShapes(restarted.Raft);
            Assert.Contains("crash/row-1", installed);
            Assert.Contains("crash/row-2", installed);

            await AssertReadsBackAsync(restarted.Kahuna, "crash/row-", 2, ct);
        }
    }

    private SettleBlockingExecutor? blocker;

    [Fact]
    public async Task Cluster_EveryReplicaInstallsTheCommittedValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", 4, raftLogger, kahunaLogger, c => c.DurableMaterializeOnResolve = true);

        try
        {
            const int count = 6;

            // A spy on every replica's installer: the leader's actor persists the committed row by itself, so only
            // the spy shows that each replica's own settle apply installed it.
            IKahuna[] replicas = [kahuna1, kahuna2, kahuna3];
            SpyInstaller[] spies = new SpyInstaller[replicas.Length];
            for (int r = 0; r < replicas.Length; r++)
            {
                PreparedIntentStore store = ((KahunaManager)replicas[r]).DurablePreparedIntentStore;
                spies[r] = new SpyInstaller(store.ResolvedIntentInstaller!);
                store.AttachResolvedIntentInstaller(spies[r]);
            }

            KeyValueTransactionResult result = await RetryOnMustRetryAsync(
                () => kahuna1.TryExecuteTransactionScript(Script("replica/row-", count), null, null),
                r => r.Type);
            Assert.Equal(KeyValueResponseType.Set, result.Type);

            await AssertReadsBackAsync(kahuna1, "replica/row-", count, ct);

            // Each replica's own settle apply installs every value, and its own durable state — the unflushed
            // overlay or the backend — then holds it.
            for (int r = 0; r < replicas.Length; r++)
            {
                KahunaManager manager = (KahunaManager)replicas[r];
                SpyInstaller spy = spies[r];
                for (int i = 1; i <= count; i++)
                {
                    string key = $"replica/row-{i}";
                    byte[] expected = Encoding.UTF8.GetBytes($"value-{i}");
                    await WaitUntilAsync(() => spy.Installed(key), $"replica {r} installs {key}", ct);
                    await WaitUntilAsync(
                        () => manager.PersistenceBackend.GetKeyValue(key) is { } entry && entry.Value is not null && entry.Value.AsSpan().SequenceEqual(expected),
                        $"replica {r} holds {key}", ct);
                }
            }

            foreach (IRaft raft in new[] { raft1, raft2, raft3 })
                Assert.Equal(0, ScanCommittedShapes(raft).Materializations);

            await Task.Delay(500, ct);
            Assert.Equal(0, repairs.Count);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }
}
