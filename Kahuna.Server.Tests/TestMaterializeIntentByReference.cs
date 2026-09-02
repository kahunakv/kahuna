using System.Diagnostics.Metrics;
using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
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
/// Covers the by-reference materialization of committed durable transactions: the post-decision record names the
/// prepared intent every replica already holds instead of copying the committed value through the log a second
/// time. The tests assert the record shape, the two consumers that expand it (the replicator and the restorer),
/// the two kinds of miss, the point-in-time-recovery expansion, and end-to-end commits with the flag on.
/// </summary>
public sealed class TestMaterializeIntentByReference : BaseCluster, IDisposable
{
    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private const int PartitionId = 3;

    private readonly ILoggerFactory loggerFactory;

    private readonly string tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_byref_" + Guid.NewGuid().ToString("N"));

    public TestMaterializeIntentByReference(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    public void Dispose()
    {
        if (Directory.Exists(tempRoot))
            Directory.Delete(tempRoot, recursive: true);
    }

    private static PreparedIntent Intent(
        string key, long revision, byte[]? value, KeyValueState state = KeyValueState.Set, long epoch = 1) =>
        new(
            TransactionId: Ts(1_000), Epoch: epoch, Key: key, ManifestHash: 42, RecordAnchorKey: "anchor",
            CommitTimestamp: Ts(1_234),
            State: state, Value: value, Bucket: null, Revision: revision, Expires: Ts(50_000),
            NoRevision: false, BaseRevision: revision - 1, BaseState: KeyValueState.Set,
            RecoveryDeadline: Ts(6_000), Resolution: PreparedIntentResolution.Committed);

    // Copies the produced bytes so no consumer can recognize them as this process's own proposal; the tests
    // assert wire fidelity, which only the decoded path exercises.
    private static RaftLog KvLog(long id, byte[] record) =>
        new() { Id = id, Type = RaftLogType.Committed, LogType = ReplicationTypes.KeyValues, LogData = [.. record] };

    // Sums the increments of a named counter on the "Kahuna" meter emitted while the action runs.
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

    private const string MissCounter = "kahuna.kv.materialization_intent_missing";

    // ── record shape ────────────────────────────────────────────────────────────

    [Fact]
    public void Record_ByReference_CarriesTheIdentityAndNoValue()
    {
        PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);

        byte[] bytes = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);
        KeyValueMessage message = ReplicationSerializer.UnserializeKeyValueMessage(bytes);

        Assert.Equal((int)KeyValueRequestType.MaterializeIntent, message.Type);
        Assert.True(message.Value.IsEmpty);

        // Everything a consumer needs to find the intent, plus what the as-of cut and the collision witness
        // read straight off the record.
        Assert.Equal("acct/1", message.Key);
        Assert.Equal(intent.TransactionId, new HLCTimestamp(
            message.TransactionIdNode, message.TransactionIdPhysical, message.TransactionIdCounter));
        Assert.Equal(1, message.Epoch);
        Assert.Equal(9, message.Revision);
        Assert.Equal(intent.CommitTimestamp, new HLCTimestamp(
            message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter));
        Assert.Equal(intent.Expires, new HLCTimestamp(
            message.ExpireNode, message.ExpirePhysical, message.ExpireCounter));
        Assert.Equal("anchor", message.RecordAnchorKey);
    }

    [Fact]
    public void Record_ByReference_IsHeaderSizedForALargeValue()
    {
        PreparedIntent intent = Intent("acct/1", revision: 9, value: new byte[1024]);

        byte[] byValue = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage());
        byte[] byReference = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);

        Assert.True(byValue.Length > 1024);
        Assert.True(byReference.Length < 200, $"by-reference record was {byReference.Length} bytes");
    }

    [Fact]
    public void Record_ByValue_IsUnchangedWhenTheFlagIsOff()
    {
        PreparedIntent set = Intent("acct/1", revision: 9, value: [4, 5, 6]);
        PreparedIntent delete = Intent("acct/2", revision: 3, value: null, state: KeyValueState.Deleted);

        KeyValueMessage setMessage = ReplicationSerializer.UnserializeKeyValueMessage(
            PreparedIntentMaterializer.ToKeyValueRecord(set, new KeyValueMessage()));
        KeyValueMessage deleteMessage = ReplicationSerializer.UnserializeKeyValueMessage(
            PreparedIntentMaterializer.ToKeyValueRecord(delete, new KeyValueMessage()));

        Assert.Equal((int)KeyValueRequestType.TrySet, setMessage.Type);
        Assert.Equal(new byte[] { 4, 5, 6 }, setMessage.Value.ToByteArray());
        Assert.Equal((int)KeyValueRequestType.TryDelete, deleteMessage.Type);

        // The epoch belongs to the by-reference form alone; the value-carrying record must not start
        // carrying it.
        Assert.Equal(0, setMessage.Epoch);
        Assert.Equal(0, deleteMessage.Epoch);
    }

    [Fact]
    public void Record_ScratchReuse_DoesNotCarryAnEpochIntoAValueCarryingRecord()
    {
        KeyValueMessage scratch = new();

        PreparedIntentMaterializer.ToKeyValueRecord(Intent("acct/1", 9, [4, 5, 6], epoch: 7), scratch, byReference: true);
        byte[] second = PreparedIntentMaterializer.ToKeyValueRecord(Intent("acct/2", 3, [1], epoch: 7), scratch);

        KeyValueMessage message = ReplicationSerializer.UnserializeKeyValueMessage(second);
        Assert.Equal(0, message.Epoch);
        Assert.Equal(new byte[] { 1 }, message.Value.ToByteArray());
    }

    // ── restorer (write-ahead-log replay) ───────────────────────────────────────

    private (KeyValueRestorer Restorer, UnflushedKeyValueWritesIndex Overlay, PreparedIntentStore Intents, IDisposable Lifetime)
        BuildRestorer(out MemoryPersistenceBackend backend)
    {
        IDisposable lifetime = TestActorSystemLifetime.Create(out Nixie.ActorSystem actorSystem);

        backend = new MemoryPersistenceBackend();
        UnflushedKeyValueWritesIndex overlay = new();
        UnflushedOverlayPersistenceBackend decorated = new(backend, overlay, new UnflushedLockWritesIndex());
        PreparedIntentStore intents = new();

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "byref-restore", NodeId = 1, Host = "localhost", Port = 0,
                InitialPartitions = 1, EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new Kommander.Discovery.StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        Kahuna.Server.Configuration.KahunaConfiguration config =
            Kahuna.Server.Configuration.ConfigurationValidator.Validate(new()
            {
                LocksWorkers = 1, KeyValueWorkers = 1, BackgroundWriterWorkers = 1, Storage = "memory",
                CacheEntryTtl = TimeSpan.FromMinutes(5), CacheEntriesToRemove = 1000,
                MaxEntriesPerActor = 50_000, MaxBytesPerActor = 256L * 1024 * 1024, CollectBatchMax = 1000,
                RevisionRetention = 16, DirtyObjectsWriterDelay = 30_000
            });

        Nixie.IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "byref-restore-bg", raft, raft.ReadScheduler, decorated,
                null!, null!, new TransactionRecordStore(), intents,
                config, NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

        KeyValueRestorer restorer = new(
            writer, raft, new CompletionReceiptStore(), NullLogger<IKahuna>.Instance,
            overlay, durabilityTracker: null, preparedIntentStore: intents);

        return (restorer, overlay, intents, lifetime);
    }

    [Fact]
    public async Task Restorer_ByReferenceRecord_AppliesTheValueFromTheReplayedIntent()
    {
        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, PreparedIntentStore intents, IDisposable lifetime) =
            BuildRestorer(out _);

        using (lifetime)
        {
            PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);

            long missed = await MeasureCounter(MissCounter, () =>
            {
                // The prepare delta replays first, exactly as it does on a live replica.
                intents.Apply(new PrepareIntentCommand(intent));

                byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);
                Assert.True(restorer.Restore(PartitionId, KvLog(10, record)));
                return Task.CompletedTask;
            });

            Assert.Equal(0, missed);
            Assert.True(overlay.TryGet("acct/1", out UnflushedKeyValueWrite replayed));
            Assert.Equal(9, replayed.Revision);
            Assert.Equal(new byte[] { 4, 5, 6 }, replayed.Value);
            Assert.Equal(KeyValueState.Set, replayed.State);
            Assert.Equal(Ts(1_234), replayed.LastModified);
        }
    }

    [Fact]
    public async Task Restorer_ByReferenceDeleteRecord_ReplaysTheTombstone()
    {
        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, PreparedIntentStore intents, IDisposable lifetime) =
            BuildRestorer(out _);

        using (lifetime)
        {
            PreparedIntent intent = Intent("acct/2", revision: 4, value: null, state: KeyValueState.Deleted);

            long missed = await MeasureCounter(MissCounter, () =>
            {
                intents.Apply(new PrepareIntentCommand(intent));
                byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);
                Assert.True(restorer.Restore(PartitionId, KvLog(11, record)));
                return Task.CompletedTask;
            });

            Assert.Equal(0, missed);
            Assert.True(overlay.TryGet("acct/2", out UnflushedKeyValueWrite replayed));
            Assert.Equal(KeyValueState.Deleted, replayed.State);
        }
    }

    [Fact]
    public async Task Restorer_DuplicateRecordAfterTheSettle_IsSilent()
    {
        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, PreparedIntentStore intents, IDisposable lifetime) =
            BuildRestorer(out _);

        using (lifetime)
        {
            PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);
            byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);

            long missed = await MeasureCounter(MissCounter, () =>
            {
                intents.Apply(new PrepareIntentCommand(intent));
                Assert.True(restorer.Restore(PartitionId, KvLog(10, record)));

                // The settle removes the intent; a second producer's copy of the same materialization then
                // replays. The overlay already stands at the revision, so it is redundant, not a miss.
                intents.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true));
                intents.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key));
                Assert.True(restorer.Restore(PartitionId, KvLog(12, record)));
                return Task.CompletedTask;
            });

            Assert.Equal(0, missed);
            Assert.Equal(9, overlay.TryGet("acct/1", out UnflushedKeyValueWrite head) ? head.Revision : -1);
        }
    }

    [Fact]
    public async Task Restorer_MissWithNothingDurable_RaisesTheAlarm()
    {
        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, _, IDisposable lifetime) =
            BuildRestorer(out _);

        using (lifetime)
        {
            PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);
            byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);

            long missed = await MeasureCounter(MissCounter, () =>
            {
                // No prepare ever replayed: the value is nowhere on this node.
                Assert.True(restorer.Restore(PartitionId, KvLog(10, record)));
                return Task.CompletedTask;
            });

            Assert.Equal(1, missed);
            Assert.False(overlay.TryGet("acct/1", out _));
        }
    }

    [Fact]
    public async Task Restorer_IntentAtADifferentRevision_RefusesTheApply()
    {
        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, PreparedIntentStore intents, IDisposable lifetime) =
            BuildRestorer(out _);

        using (lifetime)
        {
            PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);
            byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);

            long missed = await MeasureCounter(MissCounter, () =>
            {
                // Same identity, different mutation: applying the local intent would restore the wrong revision.
                intents.Apply(new PrepareIntentCommand(intent with { Revision = 11, Value = [9] }));
                Assert.True(restorer.Restore(PartitionId, KvLog(10, record)));
                return Task.CompletedTask;
            });

            Assert.Equal(1, missed);
            Assert.False(overlay.TryGet("acct/1", out _));
        }
    }

    /// <summary>
    /// The seam a replica seeded by snapshot or state transfer between the prepare and the materialization
    /// depends on: the seeded intent set must be enough to resolve the record that arrives afterwards. The
    /// transfer primitives themselves (SnapshotRange → SerializeIntents → DeserializeIntents → ImportIntents)
    /// are what both PartitionStateTransfer and RangeStateTransferService move intents with, so this drives
    /// them and then applies the record against the seeded store.
    /// </summary>
    [Fact]
    public async Task SeededReplica_ResolvesARecordThatArrivesAfterTheSeed()
    {
        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, PreparedIntentStore seeded, IDisposable lifetime) =
            BuildRestorer(out _);

        using (lifetime)
        {
            PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);

            // The source replica holds the pending intent; the seed carries it to the joining replica.
            PreparedIntentStore source = new();
            source.Apply(new PrepareIntentCommand(intent));

            IReadOnlyList<PreparedIntent> moved = source.SnapshotRange(null, null);
            Assert.Single(moved);
            seeded.ImportIntents(PreparedIntentStore.DeserializeIntents(PreparedIntentStore.SerializeIntents(moved)));

            long missed = await MeasureCounter(MissCounter, () =>
            {
                byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);
                Assert.True(restorer.Restore(PartitionId, KvLog(20, record)));
                return Task.CompletedTask;
            });

            Assert.Equal(0, missed);
            Assert.True(overlay.TryGet("acct/1", out UnflushedKeyValueWrite applied));
            Assert.Equal(9, applied.Revision);
            Assert.Equal(new byte[] { 4, 5, 6 }, applied.Value);
        }
    }

    // ── replicator miss handling ────────────────────────────────────────────────

    [Fact]
    public async Task Replicator_DuplicateRecordProvenByTheCommittedHead_IsSilent()
    {
        // The settle already removed the intent AND the flush already pruned the overlay entry, so the only
        // synchronous proof left is the staged-base fence's committed-head memory.
        KeyValueReplicator replicator = new(
            null!, null!, null!, null!, null!, null!, NullLogger<IKahuna>.Instance,
            committedHeadRevisionProbe: key => key == "acct/1" ? 9 : -1);

        PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);
        byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);

        long missed = await MeasureCounter(MissCounter, () =>
        {
            Assert.True(replicator.Replicate(PartitionId, KvLog(10, record)));
            return Task.CompletedTask;
        });

        Assert.Equal(0, missed);
    }

    [Fact]
    public async Task Replicator_DuplicateRecordProvenByTheOverlay_IsSilent()
    {
        UnflushedKeyValueWritesIndex overlay = new();
        overlay.Record("acct/1", [4, 5, 6], 9, HLCTimestamp.Zero, Ts(1_234), Ts(1_234), KeyValueState.Set, noRevision: false);

        KeyValueReplicator replicator = new(
            null!, null!, null!, null!, null!, null!, NullLogger<IKahuna>.Instance, unflushedWrites: overlay);

        PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);
        byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);

        long missed = await MeasureCounter(MissCounter, () =>
        {
            Assert.True(replicator.Replicate(PartitionId, KvLog(10, record)));
            return Task.CompletedTask;
        });

        Assert.Equal(0, missed);
    }

    [Fact]
    public async Task Replicator_MissVerifiedAgainstTheBackend_ResolvesSilentlyWhenTheRowIsDurable()
    {
        // The overlay entry was pruned by a landed flush and no committed-head memory survives; the off-path
        // verification finds the flushed row at the record's revision, so the record is still redundant.
        KeyValueEntry flushed = new() { Revision = 9, Value = [4, 5, 6], State = KeyValueState.Set, LastModified = Ts(1_234) };

        KeyValueReplicator replicator = new(
            null!, null!, null!, null!, null!, null!, NullLogger<IKahuna>.Instance,
            hydrateFromBackend: (_, _) => Task.FromResult<KeyValueEntry?>(flushed));

        PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);
        byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);

        long missed = await MeasureCounter(MissCounter, async () =>
        {
            Assert.True(replicator.Replicate(PartitionId, KvLog(10, record)));
            await Task.Delay(300, TestContext.Current.CancellationToken);
        });

        Assert.Equal(0, missed);
    }

    [Fact]
    public async Task Replicator_MissVerifiedAgainstTheBackend_AlarmsWhenTheRowIsBelowTheRevision()
    {
        KeyValueEntry stale = new() { Revision = 8, Value = [1], State = KeyValueState.Set, LastModified = Ts(1_000) };

        TaskCompletionSource verified = new(TaskCreationOptions.RunContinuationsAsynchronously);

        KeyValueReplicator replicator = new(
            null!, null!, null!, null!, null!, null!, NullLogger<IKahuna>.Instance,
            hydrateFromBackend: (_, _) =>
            {
                verified.TrySetResult();
                return Task.FromResult<KeyValueEntry?>(stale);
            });

        PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);
        byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);

        long missed = await MeasureCounter(MissCounter, async () =>
        {
            Assert.True(replicator.Replicate(PartitionId, KvLog(10, record)));
            await verified.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            await Task.Delay(300, TestContext.Current.CancellationToken);
        });

        Assert.Equal(1, missed);
    }

    // ── point-in-time recovery ──────────────────────────────────────────────────

    private static RaftLog IntentLog(long id, long timeMs, params PreparedIntentCommand[] commands) =>
        new()
        {
            Id = id,
            Type = RaftLogType.Committed,
            Time = new HLCTimestamp(0, timeMs, 0),
            LogType = ReplicationTypes.PreparedIntent,
            LogData = [.. PreparedIntentStore.SerializeDelta(commands)]
        };

    private static RaftLog ByReferenceLog(long id, long timeMs, PreparedIntent intent) =>
        new()
        {
            Id = id,
            Type = RaftLogType.Committed,
            Time = new HLCTimestamp(0, timeMs, 0),
            LogType = ReplicationTypes.KeyValues,
            LogData = [.. PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true)]
        };

    private static InMemoryWAL BuildWal(int partition, params RaftLog[] logs)
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        wal.Write([(partition, [.. logs])]);
        return wal;
    }

    private static string? ValueOf(MemoryPersistenceBackend backend, string key)
    {
        object? entry = backend.GetKeyValue(key);
        byte[]? bytes = entry?.GetType().GetProperty("Value")?.GetValue(entry) as byte[];
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    [Fact]
    public async Task Restore_ExpandsAByReferenceRecordFromTheReplayedPrepare()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string artifacts = Path.Combine(tempRoot, "artifacts_expand");
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(Path.Combine(tempRoot, "catalog_expand")));

        // The full backup covers an empty partition; everything under test arrives in the incremental.
        InMemoryWAL wal = BuildWal(1, new RaftLog
        {
            Id = 1, Type = RaftLogType.Committed, Time = Ts(100),
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(new KeyValueMessage
            {
                Type = (int)KeyValueRequestType.TrySet, Key = "seed",
                Value = UnsafeByteOperations.UnsafeWrap("seed"u8.ToArray()), Revision = 1, LastModifiedPhysical = 100
            })
        });

        MemoryPersistenceBackend fullBackend = new();
        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, BackupTestStores.Artifacts(artifacts), catalog, ct: ct);

        PreparedIntent intent = Intent("acct/1", revision: 9, value: "committed"u8.ToArray()) with
        {
            CommitTimestamp = Ts(300)
        };

        wal.Write([(1, [
            IntentLog(2, 200, new PrepareIntentCommand(intent)),
            ByReferenceLog(3, 300, intent),
            IntentLog(4, 310,
                new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true),
                new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key))
        ])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(
            wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: ct);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(inc.BackupId, ct);

        await RestoreEngine.RestoreAsync(chain, BackupTestStores.Artifacts(artifacts), Ts(400), restored, ct: ct);

        Assert.Equal("committed", ValueOf(restored, "acct/1"));
    }

    [Fact]
    public async Task Restore_CutBeforeTheCommit_ExcludesTheByReferenceRecord()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string artifacts = Path.Combine(tempRoot, "artifacts_cut");
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(Path.Combine(tempRoot, "catalog_cut")));

        InMemoryWAL wal = BuildWal(1, new RaftLog
        {
            Id = 1, Type = RaftLogType.Committed, Time = Ts(100),
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(new KeyValueMessage
            {
                Type = (int)KeyValueRequestType.TrySet, Key = "seed",
                Value = UnsafeByteOperations.UnsafeWrap("seed"u8.ToArray()), Revision = 1, LastModifiedPhysical = 100
            })
        });

        MemoryPersistenceBackend fullBackend = new();
        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, BackupTestStores.Artifacts(artifacts), catalog, ct: ct);

        PreparedIntent intent = Intent("acct/1", revision: 9, value: "committed"u8.ToArray()) with
        {
            CommitTimestamp = Ts(300)
        };

        wal.Write([(1, [
            IntentLog(2, 200, new PrepareIntentCommand(intent)),
            ByReferenceLog(3, 300, intent)
        ])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(
            wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: ct);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(inc.BackupId, ct);

        // The cut sits before the commit timestamp the record carries, so the transaction is excluded whole.
        await RestoreEngine.RestoreAsync(chain, BackupTestStores.Artifacts(artifacts), Ts(250), restored, ct: ct);

        Assert.Null(ValueOf(restored, "acct/1"));
    }

    [Fact]
    public async Task Restore_ByReferenceRecordWithNoPrepare_FailsTheRestore()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string artifacts = Path.Combine(tempRoot, "artifacts_orphan");
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(Path.Combine(tempRoot, "catalog_orphan")));

        InMemoryWAL wal = BuildWal(1, new RaftLog
        {
            Id = 1, Type = RaftLogType.Committed, Time = Ts(100),
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(new KeyValueMessage
            {
                Type = (int)KeyValueRequestType.TrySet, Key = "seed",
                Value = UnsafeByteOperations.UnsafeWrap("seed"u8.ToArray()), Revision = 1, LastModifiedPhysical = 100
            })
        });

        MemoryPersistenceBackend fullBackend = new();
        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, BackupTestStores.Artifacts(artifacts), catalog, ct: ct);

        PreparedIntent intent = Intent("acct/1", revision: 9, value: "committed"u8.ToArray()) with
        {
            CommitTimestamp = Ts(300)
        };

        // The prepare is deliberately absent: the restore must refuse rather than silently drop the row.
        wal.Write([(1, [ByReferenceLog(2, 300, intent)])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(
            wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: ct);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(inc.BackupId, ct);

        BackupDriverException error = await Assert.ThrowsAsync<BackupDriverException>(() =>
            RestoreEngine.RestoreAsync(chain, BackupTestStores.Artifacts(artifacts), Ts(400), restored, ct: ct));

        Assert.Contains("acct/1", error.Message);
    }

    // ── end to end ──────────────────────────────────────────────────────────────

    /// <summary>The opt-out path an operator uses during a mixed-version rollout must keep working: with the
    /// flag off the finalizer produces value-carrying records and the transaction commits exactly as before.</summary>
    [Fact]
    public async Task EndToEnd_FlagOff_MultiKeyTransactionCommitsAndReadsBack()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableMaterializeByReference = false
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("legacy/row-1", ct);

        const int count = 4;
        StringBuilder script = new("BEGIN ");
        for (int i = 1; i <= count; i++)
            script.Append($"SET `legacy/row-{i}` 'value-{i}' ");
        script.Append("COMMIT END");

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes(script.ToString()), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        for (int i = 1; i <= count; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, $"legacy/row-{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal(Encoding.UTF8.GetBytes($"value-{i}"), entry!.Value);
        }
    }

    [Fact]
    public async Task EndToEnd_FlagOn_MultiKeyTransactionCommitsAndReadsBack()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        long missed = await MeasureCounter(MissCounter, async () =>
        {
            await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
            {
                ReadIOThreads = 1,
                WriteIOThreads = 1,
                PartitionExecutorPoolSize = 1,
                Storage = "memory",
                WalStorage = "memory",
                InitialPartitions = 4,
                DurableMaterializeByReference = true
            }, loggerFactory);

            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("orders/row-1", ct);

            const int count = 8;
            StringBuilder script = new("BEGIN ");
            for (int i = 1; i <= count; i++)
                script.Append($"SET `orders/row-{i}` 'value-{i}' ");
            script.Append("COMMIT END");

            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script.ToString()), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);

            for (int i = 1; i <= count; i++)
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, $"orders/row-{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.NotNull(entry);
                Assert.Equal(Encoding.UTF8.GetBytes($"value-{i}"), entry!.Value);
            }
        });

        Assert.Equal(0, missed);
    }

    [Fact]
    public async Task Cluster_FlagOn_CommittedValuesConvergeOnEveryReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        // The three nodes run in this one process and share the static miss counter, so a follower that failed
        // to resolve a by-reference record shows up here even though the read below is answered by the leader.
        long missed = await MeasureCounter(MissCounter, async () =>
        {
            (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, _, _) = await AssembleThreNodeCluster(
                "memory", 4, raftLogger, kahunaLogger, c => c.DurableMaterializeByReference = true);

            try
            {
                const int count = 6;
                StringBuilder script = new("BEGIN ");
                for (int i = 1; i <= count; i++)
                    script.Append($"SET `byref/row-{i}` 'value-{i}' ");
                script.Append("COMMIT END");

                KeyValueTransactionResult result = await RetryOnMustRetryAsync(
                    () => kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script.ToString()), null, null),
                    r => r.Type);
                Assert.Equal(KeyValueResponseType.Set, result.Type);

                for (int i = 1; i <= count; i++)
                {
                    (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna1.LocateAndTryGetValue(
                        HLCTimestamp.Zero, $"byref/row-{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

                    Assert.Equal(KeyValueResponseType.Get, type);
                    Assert.Equal(Encoding.UTF8.GetBytes($"value-{i}"), entry!.Value);
                }

                // Give the deferred settlement and any redundant re-materialization time to land before the
                // counter is read.
                await Task.Delay(1_000, ct);
            }
            finally
            {
                await LeaveCluster(raft1, raft2, raft3);
            }
        });

        Assert.Equal(0, missed);
    }
}
