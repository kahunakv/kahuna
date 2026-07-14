using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for the partition-scoped durable coordinator decision store: a committed
/// transaction's record is replicated on the data partition that routes its anchor (never the meta
/// partition), records anchored to different data partitions are independent, the participant set is
/// frozen after the first write, removal retires a record, and a record survives a cold restart via its
/// local snapshot.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestCoordinatorDecisionStore
{
    private readonly ILoggerFactory loggerFactory;

    public TestCoordinatorDecisionStore(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaOptions InMemoryOptions(int partitions) => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = partitions
    };

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath) => new()
    {
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "coorddecision",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "coorddecision-wal",
        InitialPartitions = 1,
        DirtyObjectsWriterDelay = 60000
    };

    private static CoordinatorDecisionRecord Decision(
        HLCTimestamp txId, string anchor, CoordinatorDecisionStatus status, params string[] participantKeys)
    {
        List<CoordinatorParticipant> participants = [];
        foreach (string key in participantKeys)
            participants.Add(new CoordinatorParticipant(key, KeyValueDurability.Persistent, HLCTimestamp.Zero, false, false));

        return new CoordinatorDecisionRecord(
            txId, anchor, anchor, txId, status, participants, [], txId, HLCTimestamp.Zero);
    }

    private static CoordinatorDecisionStore StoreOf(EmbeddedKahunaNode node) =>
        ((KahunaManager)node.Kahuna).CoordinatorDecisionStore;

    // ── A committed decision is anchored on a data partition, never partition 0 ────────────────────

    [Fact]
    public async Task UpsertedDecision_IsAnchoredOnADataPartition_NeverPartitionZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(2), loggerFactory);
        await node.StartAsync(ct);

        const string anchor = "acct/alice";
        await node.WaitForLeaderForKeyAsync(anchor, ct);

        // A Durable transaction's record routes to a DATA partition — never the meta partition 0.
        int partitionId = ((KahunaManager)node.Kahuna).GetDataPartitionForKey(anchor);
        Assert.True(partitionId >= RangeMapStore.FirstDataPartitionId);
        Assert.NotEqual(RangeMapStore.MetaPartitionId, partitionId);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        CoordinatorDecisionStore store = StoreOf(node);

        CoordinatorDecisionMutationResult result = await store.UpsertAsync(
            Decision(txId, anchor, CoordinatorDecisionStatus.CommitDecided, anchor), 0, ct);
        Assert.Equal(CoordinatorDecisionMutationResult.Applied, result);

        Assert.True(store.TryGet(txId, out CoordinatorDecisionRecord record));
        Assert.Equal(anchor, record.RecordAnchorKey);
        Assert.Equal(CoordinatorDecisionStatus.CommitDecided, record.Status);
        Assert.Single(record.Participants);
    }

    // ── Records on different data partitions replicate independently ───────────────────────────────

    [Fact]
    public async Task RecordsForDifferentDataPartitions_ReplicateIndependently()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(2), loggerFactory);
        await node.StartAsync(ct);

        // Find two anchors that route to distinct data partitions.
        var manager = (KahunaManager)node.Kahuna;
        string? anchorA = null, anchorB = null;
        int partA = -1, partB = -1;
        for (int i = 0; i < 64 && (anchorA is null || anchorB is null); i++)
        {
            string anchor = $"space{i}/key";
            int partition = manager.GetDataPartitionForKey(anchor);
            if (anchorA is null) { anchorA = anchor; partA = partition; }
            else if (partition != partA) { anchorB = anchor; partB = partition; }
        }
        Assert.NotNull(anchorA);
        Assert.NotNull(anchorB);
        Assert.NotEqual(partA, partB);

        await node.WaitForLeaderForKeyAsync(anchorA!, ct);
        await node.WaitForLeaderForKeyAsync(anchorB!, ct);

        CoordinatorDecisionStore store = StoreOf(node);
        HLCTimestamp txA = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp txB = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        Assert.Equal(CoordinatorDecisionMutationResult.Applied,
            await store.UpsertAsync(Decision(txA, anchorA!, CoordinatorDecisionStatus.CommitDecided, anchorA!), 0, ct));
        Assert.Equal(CoordinatorDecisionMutationResult.Applied,
            await store.UpsertAsync(Decision(txB, anchorB!, CoordinatorDecisionStatus.CommitDecided, anchorB!), 0, ct));

        // Both records are present and each is anchored to its own data partition.
        Assert.True(store.TryGet(txA, out CoordinatorDecisionRecord recordA));
        Assert.True(store.TryGet(txB, out CoordinatorDecisionRecord recordB));
        Assert.Equal(partA, manager.GetDataPartitionForKey(recordA.RecordAnchorKey));
        Assert.Equal(partB, manager.GetDataPartitionForKey(recordB.RecordAnchorKey));
        Assert.NotEqual(RangeMapStore.MetaPartitionId, partA);
        Assert.NotEqual(RangeMapStore.MetaPartitionId, partB);
    }

    // ── The participant set is frozen after the first decision write ───────────────────────────────

    [Fact]
    public async Task ParticipantSet_IsFrozenAfterFirstWrite_ProgressStillUpdates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(1), loggerFactory);
        await node.StartAsync(ct);

        const string anchor = "batch/first";
        await node.WaitForLeaderForKeyAsync(anchor, ct);

        CoordinatorDecisionStore store = StoreOf(node);
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        // First write pins the participant set {anchor, batch/second}.
        Assert.Equal(CoordinatorDecisionMutationResult.Applied,
            await store.UpsertAsync(Decision(txId, anchor, CoordinatorDecisionStatus.CommitDecided, anchor, "batch/second"), 0, ct));

        // A progress update with the SAME participants (status advances) is accepted.
        Assert.Equal(CoordinatorDecisionMutationResult.Applied,
            await store.UpsertAsync(Decision(txId, anchor, CoordinatorDecisionStatus.Completed, anchor, "batch/second"), 0, ct));
        Assert.True(store.TryGet(txId, out CoordinatorDecisionRecord advanced));
        Assert.Equal(CoordinatorDecisionStatus.Completed, advanced.Status);

        // Changing the participant set is rejected, and the stored record is unchanged.
        Assert.Equal(CoordinatorDecisionMutationResult.RejectedParticipantsFrozen,
            await store.UpsertAsync(Decision(txId, anchor, CoordinatorDecisionStatus.CommitDecided, anchor, "batch/other"), 0, ct));
        Assert.True(store.TryGet(txId, out CoordinatorDecisionRecord unchanged));
        Assert.Equal(2, unchanged.Participants.Count);
        Assert.Contains(unchanged.Participants, p => p.Key == "batch/second");
        Assert.Equal(CoordinatorDecisionStatus.Completed, unchanged.Status);
    }

    // ── Removal retires the record ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Remove_RetiresRecord()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(1), loggerFactory);
        await node.StartAsync(ct);

        const string anchor = "retire/me";
        await node.WaitForLeaderForKeyAsync(anchor, ct);

        CoordinatorDecisionStore store = StoreOf(node);
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        Assert.Equal(CoordinatorDecisionMutationResult.Applied,
            await store.UpsertAsync(Decision(txId, anchor, CoordinatorDecisionStatus.Completed, anchor), 0, ct));
        Assert.True(store.TryGet(txId, out _));

        Assert.Equal(CoordinatorDecisionMutationResult.Applied, await store.RemoveAsync(txId, anchor, ct));
        Assert.False(store.TryGet(txId, out _));
    }

    // ── A record survives a cold restart via its local snapshot ────────────────────────────────────

    [Fact]
    public async Task Record_SurvivesColdRestart_ViaLocalSnapshot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-cd-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-cd-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            const string anchor = "durable/record";
            HLCTimestamp txId;

            await using (EmbeddedKahunaNode first = new(PersistentOptions(storagePath, walPath), loggerFactory))
            {
                await first.StartAsync(ct);
                await first.WaitForLeaderForKeyAsync(anchor, ct);

                txId = first.Raft.HybridLogicalClock.TrySendOrLocalEvent(first.Raft.GetLocalNodeId());
                Assert.Equal(CoordinatorDecisionMutationResult.Applied,
                    await StoreOf(first).UpsertAsync(
                        Decision(txId, anchor, CoordinatorDecisionStatus.CommitDecided, anchor), 0, ct));
                Assert.True(StoreOf(first).TryGet(txId, out _));
            }

            // Cold restart over the same storage — the record rebuilds from the durable local snapshot.
            await using EmbeddedKahunaNode second = new(PersistentOptions(storagePath, walPath), loggerFactory);
            await second.StartAsync(ct);

            Assert.True(StoreOf(second).TryGet(txId, out CoordinatorDecisionRecord record));
            Assert.Equal(anchor, record.RecordAnchorKey);
            Assert.Equal(CoordinatorDecisionStatus.CommitDecided, record.Status);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    // ── Concurrent applies across partitions lose no record (partition-safe publish) ───────────────

    [Fact]
    public async Task ConcurrentApplyAcrossPartitions_LosesNoRecord()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(2), loggerFactory);
        await node.StartAsync(ct);

        CoordinatorDecisionStore store = StoreOf(node);
        Assert.Equal(0, store.Count);

        // Build many distinct records and apply them through the follower/restore path concurrently, alternating
        // the source partition so two partition executors publish into the one node-global set at once. Without a
        // serialized publish the copy-on-write set lost-updates and drops records; every record must survive.
        const int total = 400;
        List<(int partition, CoordinatorDecisionRecord record)> work = new(total);
        for (int i = 0; i < total; i++)
        {
            HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
            work.Add((i % 2 + 1, Decision(txId, $"loss/{i}", CoordinatorDecisionStatus.CommitDecided, $"loss/{i}")));
        }

        await Task.WhenAll(work.Select(w => Task.Run(() =>
        {
            Assert.True(store.ApplyUpsertForTest(w.partition, w.record));
        }, ct)));

        Assert.Equal(total, store.Count);
    }

    // ── Racing request-path and recovery updates on different participants do not regress ──────────

    [Fact]
    public async Task RacingProgressOnDifferentParticipants_DoesNotRegress()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(1), loggerFactory);
        await node.StartAsync(ct);

        const string anchor = "mono/anchor";
        const string second = "mono/second";
        await node.WaitForLeaderForKeyAsync(anchor, ct);

        CoordinatorDecisionStore store = StoreOf(node);
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        CoordinatorDecisionRecord View(bool ackAnchor, bool ackSecond) => new(
            txId, "coord", anchor, txId, CoordinatorDecisionStatus.CommitDecided,
            [
                new CoordinatorParticipant(anchor, KeyValueDurability.Persistent, HLCTimestamp.Zero, ackAnchor, false),
                new CoordinatorParticipant(second, KeyValueDurability.Persistent, HLCTimestamp.Zero, ackSecond, false)
            ],
            [], txId, HLCTimestamp.Zero);

        Assert.Equal(CoordinatorDecisionMutationResult.Applied, await store.UpsertAsync(View(false, false), 0, ct));

        // The request path records the anchor ack (its view still shows the secondary unacked); recovery, from a
        // stale view where the anchor is unacked, records the secondary ack. Neither may clobber the other.
        Assert.Equal(CoordinatorDecisionMutationResult.Applied, await store.UpsertAsync(View(true, false), 0, ct));
        Assert.Equal(CoordinatorDecisionMutationResult.Applied, await store.UpsertAsync(View(false, true), 0, ct));

        Assert.True(store.TryGet(txId, out CoordinatorDecisionRecord merged));
        Assert.All(merged.Participants, p => Assert.True(p.Acked));
    }

    // ── An older imported/replayed version never regresses newer local progress ────────────────────

    [Fact]
    public async Task ImportOlderVersion_DoesNotRegressNewerLocalRecord()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(1), loggerFactory);
        await node.StartAsync(ct);

        const string anchor = "import/anchor";
        await node.WaitForLeaderForKeyAsync(anchor, ct);

        CoordinatorDecisionStore store = StoreOf(node);
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        // Local progress reaches Completed.
        Assert.Equal(CoordinatorDecisionMutationResult.Applied,
            await store.UpsertAsync(Decision(txId, anchor, CoordinatorDecisionStatus.Completed, anchor), 0, ct));
        Assert.True(store.TryGet(txId, out CoordinatorDecisionRecord local));
        Assert.Equal(CoordinatorDecisionStatus.Completed, local.Status);

        // A stale handoff carrying the earlier CommitDecided view is imported; it must merge forward, not clobber.
        store.ImportRecords([Decision(txId, anchor, CoordinatorDecisionStatus.CommitDecided, anchor)]);

        Assert.True(store.TryGet(txId, out CoordinatorDecisionRecord afterImport));
        Assert.Equal(CoordinatorDecisionStatus.Completed, afterImport.Status);
    }

    // ── A progress write that changes a participant's prepared ticket is rejected as frozen ────────

    [Fact]
    public async Task ChangedParticipantTicket_IsRejectedAsFrozen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(1), loggerFactory);
        await node.StartAsync(ct);

        const string anchor = "ticket/anchor";
        const string second = "ticket/second";
        await node.WaitForLeaderForKeyAsync(anchor, ct);

        CoordinatorDecisionStore store = StoreOf(node);
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp ticket = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp otherTicket = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        CoordinatorDecisionRecord WithTicket(HLCTimestamp secondTicket, bool ackSecond) => new(
            txId, "coord", anchor, txId, CoordinatorDecisionStatus.CommitDecided,
            [
                new CoordinatorParticipant(anchor, KeyValueDurability.Persistent, HLCTimestamp.Zero, true, false),
                new CoordinatorParticipant(second, KeyValueDurability.Persistent, secondTicket, ackSecond, false)
            ],
            [], txId, HLCTimestamp.Zero);

        Assert.Equal(CoordinatorDecisionMutationResult.Applied, await store.UpsertAsync(WithTicket(ticket, false), 0, ct));

        // Same ticket → accepted progress; changed ticket → rejected as a frozen-participant violation.
        Assert.Equal(CoordinatorDecisionMutationResult.Applied, await store.UpsertAsync(WithTicket(ticket, true), 0, ct));
        Assert.Equal(CoordinatorDecisionMutationResult.RejectedParticipantsFrozen,
            await store.UpsertAsync(WithTicket(otherTicket, true), 0, ct));
    }

    // ── The per-partition checkpoint snapshot holds only that partition's records and reports success ──────

    [Fact]
    public async Task PersistSnapshot_WritesOnlyThePartitionsRecords_AndReportsSuccess()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-cdsnap-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-cdsnap-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
            await node.StartAsync(ct);

            // A standalone store over a private directory, with a deterministic anchor → partition mapping.
            CoordinatorDecisionStore store = new(node.Raft, storagePath, "cdsnap", loggerFactory.CreateLogger<IKahuna>());
            store.AttachAnchorResolver(key => (key.StartsWith("p1", StringComparison.Ordinal) ? 1 : 2, 1L));

            HLCTimestamp tx1 = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
            HLCTimestamp tx2 = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
            store.InstallFromAnchorCommit(Decision(tx1, "p1/a", CoordinatorDecisionStatus.CommitDecided, "p1/a"));
            store.InstallFromAnchorCommit(Decision(tx2, "p2/b", CoordinatorDecisionStatus.CommitDecided, "p2/b"));

            Assert.True(store.PersistSnapshot(1));
            Assert.True(store.PersistSnapshot(2));

            string p1File = Path.Combine(storagePath, "coordinatordecision_cdsnap_p1.snapshot");
            IReadOnlyList<CoordinatorDecisionRecord> p1 = CoordinatorDecisionStore.DeserializeRecords(File.ReadAllBytes(p1File));
            CoordinatorDecisionRecord only = Assert.Single(p1);
            Assert.Equal("p1/a", only.RecordAnchorKey);

            // A fresh store over the same directory reloads both partitions' snapshots.
            CoordinatorDecisionStore reloaded = new(node.Raft, storagePath, "cdsnap", loggerFactory.CreateLogger<IKahuna>());
            Assert.True(reloaded.TryGet(tx1, out _));
            Assert.True(reloaded.TryGet(tx2, out _));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    // ── A snapshot write failure is reported, so the checkpoint gate can hold the WAL floor ─────────────────

    [Fact]
    public async Task PersistSnapshot_ReportsFailure_WhenWriteFaultInjected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-cdsnap-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-cdsnap-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
            await node.StartAsync(ct);

            CoordinatorDecisionStore store = new(node.Raft, storagePath, "cdsnap", loggerFactory.CreateLogger<IKahuna>());
            store.AttachAnchorResolver(key => (1, 1L));

            store.PersistSnapshotFault = _ => true;
            Assert.False(store.PersistSnapshot(1));

            store.PersistSnapshotFault = null;
            Assert.True(store.PersistSnapshot(1));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    // ── A corrupt snapshot fails closed rather than starting empty and losing a committed decision ─────────

    [Fact]
    public async Task LoadFromDisk_FailsClosed_OnCorruptSnapshot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-cdsnap-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-cdsnap-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
            await node.StartAsync(ct);

            File.WriteAllBytes(Path.Combine(storagePath, "coordinatordecision_cdsnap_p1.snapshot"),
                "not a valid decision snapshot"u8.ToArray());

            Assert.ThrowsAny<Exception>(() =>
                new CoordinatorDecisionStore(node.Raft, storagePath, "cdsnap", loggerFactory.CreateLogger<IKahuna>()));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    // ── The checkpoint gate refuses to advance the WAL floor when a snapshot cannot be made durable ────────

    [Fact]
    public async Task CheckpointGate_RefusesToCheckpoint_WhenASnapshotCannotPersist()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-cdgate-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-cdgate-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
            await node.StartAsync(ct);

            var manager = (KahunaManager)node.Kahuna;
            int partition = manager.GetDataPartitionForKey("gate/key");
            BackgroundWriterActor? writer = manager.BackgroundWriterActor;
            Assert.NotNull(writer);

            // With both snapshots durable, the gate lets the checkpoint proceed.
            Assert.True(writer!.TryCaptureCheckpointSnapshots(partition));

            // A decision-snapshot failure blocks the checkpoint — the WAL floor must not advance past a decision
            // whose only durable copy could not be written.
            manager.CoordinatorDecisionStore.PersistSnapshotFault = _ => true;
            Assert.False(writer.TryCaptureCheckpointSnapshots(partition));
            manager.CoordinatorDecisionStore.PersistSnapshotFault = null;

            // A receipt-snapshot failure blocks it just the same.
            manager.CompletionReceiptStore.PersistSnapshotFault = _ => true;
            Assert.False(writer.TryCaptureCheckpointSnapshots(partition));
            manager.CompletionReceiptStore.PersistSnapshotFault = null;

            // Cleared, the gate proceeds again.
            Assert.True(writer.TryCaptureCheckpointSnapshots(partition));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    // ── A stalled anchor partition does not block a mutation on another partition ───────────────────

    /// <summary>
    /// The mutation gate is scoped per anchor partition, so a mutation parked on one partition's Raft replicate
    /// (a slow or leaderless anchor partition) holds only that partition's gate. A mutation whose anchor routes to
    /// a different partition acquires a different gate and completes; a single node-wide gate would serialize it
    /// behind the stalled partition and it would never return.
    /// </summary>
    [Fact]
    public async Task StalledAnchorPartition_DoesNotBlockMutationOnAnotherPartition()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(2), loggerFactory);
        await node.StartAsync(ct);

        var manager = (KahunaManager)node.Kahuna;
        string? anchorA = null, anchorB = null;
        int partA = -1, partB = -1;
        for (int i = 0; i < 64 && (anchorA is null || anchorB is null); i++)
        {
            string anchor = $"stall{i}/key";
            int partition = manager.GetDataPartitionForKey(anchor);
            if (anchorA is null) { anchorA = anchor; partA = partition; }
            else if (partition != partA) { anchorB = anchor; partB = partition; }
        }
        Assert.NotNull(anchorA);
        Assert.NotNull(anchorB);
        Assert.NotEqual(partA, partB);

        await node.WaitForLeaderForKeyAsync(anchorA!, ct);
        await node.WaitForLeaderForKeyAsync(anchorB!, ct);

        CoordinatorDecisionStore store = StoreOf(node);

        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        store.BeforeReplicateHook = pid =>
        {
            if (pid != partA)
                return Task.CompletedTask;
            entered.TrySetResult();
            return release.Task;
        };

        HLCTimestamp txA = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp txB = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        // Partition A's mutation parks in the replicate hook, holding only partition A's gate.
        Task<CoordinatorDecisionMutationResult> aTask =
            store.UpsertAsync(Decision(txA, anchorA!, CoordinatorDecisionStatus.CommitDecided, anchorA!), 0, ct);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);

        // Partition B's mutation must complete despite A being stuck.
        CoordinatorDecisionMutationResult bResult = await store
            .UpsertAsync(Decision(txB, anchorB!, CoordinatorDecisionStatus.CommitDecided, anchorB!), 0, ct)
            .WaitAsync(TimeSpan.FromSeconds(10), ct);
        Assert.Equal(CoordinatorDecisionMutationResult.Applied, bResult);
        Assert.False(aTask.IsCompleted);

        release.SetResult();
        Assert.Equal(CoordinatorDecisionMutationResult.Applied, await aTask.WaitAsync(TimeSpan.FromSeconds(10), ct));
    }

    // ── A redundant delta advances nothing and skips the record-set rebuild ─────────────────────────

    /// <summary>
    /// The leader eager-publishes a mutation and then its own commit echo replays the same delta through the
    /// follower/restore apply path. Only a delta that advances a monotonic field rebuilds and swaps the
    /// O(record-count) copy-on-write set; an identical re-delivery is detected as a no-op and skips the rebuild,
    /// so a steady stream of leader mutations never pays for the same forward merge twice.
    /// </summary>
    [Fact]
    public async Task RedundantDelta_SkipsRecordSetRebuild_OnlyAdvancingDeltaRepublishes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(1), loggerFactory);
        await node.StartAsync(ct);

        CoordinatorDecisionStore store = new(node.Raft, null, null, loggerFactory.CreateLogger<IKahuna>());
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        long baseline = store.PublishGeneration;

        // First apply inserts the record — one rebuild.
        Assert.True(store.ApplyUpsertForTest(1, Decision(txId, "a", CoordinatorDecisionStatus.CommitDecided, "a")));
        Assert.Equal(baseline + 1, store.PublishGeneration);

        // An identical re-delivery advances nothing — no rebuild.
        Assert.True(store.ApplyUpsertForTest(1, Decision(txId, "a", CoordinatorDecisionStatus.CommitDecided, "a")));
        Assert.Equal(baseline + 1, store.PublishGeneration);

        // A forward-advancing delta (status → Completed) rebuilds exactly once.
        Assert.True(store.ApplyUpsertForTest(1, Decision(txId, "a", CoordinatorDecisionStatus.Completed, "a")));
        Assert.Equal(baseline + 2, store.PublishGeneration);

        // And that advance, re-delivered, is again a no-op.
        Assert.True(store.ApplyUpsertForTest(1, Decision(txId, "a", CoordinatorDecisionStatus.Completed, "a")));
        Assert.Equal(baseline + 2, store.PublishGeneration);
    }

    // ── Admission counts only outstanding records; completed retention frees the budget ─────────────

    /// <summary>
    /// Durable admission is gated by the number of <em>outstanding</em> decision records, not the total record
    /// count. A record that reaches <c>Completed</c> is evictable idempotency retention and releases its budget
    /// slot, so a store full of completed records still admits new durable transactions — the defect where retained
    /// outcomes throttled sustained durable throughput.
    /// </summary>
    [Fact]
    public async Task Admission_CountsOutstandingOnly_CompletedRecordsFreeTheBudget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(1), loggerFactory);
        await node.StartAsync(ct);

        CoordinatorDecisionStore store = new(node.Raft, null, null, loggerFactory.CreateLogger<IKahuna>())
        {
            DurableAdmissionCapacity = 2
        };

        HLCTimestamp txA = new(1, 100, 0);
        HLCTimestamp txB = new(1, 200, 0);
        store.ApplyUpsertForTest(1, Decision(txA, "a", CoordinatorDecisionStatus.CommitDecided, "a"));
        store.ApplyUpsertForTest(1, Decision(txB, "b", CoordinatorDecisionStatus.CommitDecided, "b"));

        // Both slots are outstanding — the budget is full, so a new admission is refused and counted.
        Assert.False(store.TryReserveAdmission(out CoordinatorDecisionStore.DurableDecisionReservation? refused));
        Assert.Null(refused);
        Assert.Equal(2, store.GetCapacityStats().Outstanding);
        Assert.True(store.GetCapacityStats().Rejections >= 1);

        // Complete one record: it becomes evictable retention and no longer occupies the outstanding budget.
        store.ApplyUpsertForTest(1, Decision(txA, "a", CoordinatorDecisionStatus.Completed, "a"));
        Assert.Equal(1, store.GetCapacityStats().Outstanding);
        Assert.Equal(1, store.GetCapacityStats().Completed);

        // The freed slot admits a new durable transaction.
        Assert.True(store.TryReserveAdmission(out CoordinatorDecisionStore.DurableDecisionReservation? admitted));
        Assert.NotNull(admitted);
        admitted!.Dispose();
    }

    // ── Concurrent admissions never collectively exceed the budget ──────────────────────────────────

    /// <summary>
    /// Reserving admission is atomic: it counts in-flight reservations under a lock, so a burst of concurrent
    /// admissions grants exactly the budget and no more — closing the read-then-act race where every caller reads
    /// spare capacity and all proceed.
    /// </summary>
    [Fact]
    public async Task Admission_ConcurrentReservations_NeverExceedBudget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(InMemoryOptions(1), loggerFactory);
        await node.StartAsync(ct);

        const int capacity = 50;
        CoordinatorDecisionStore store = new(node.Raft, null, null, loggerFactory.CreateLogger<IKahuna>())
        {
            DurableAdmissionCapacity = capacity
        };

        const int attempts = 400;
        int granted = 0;
        System.Collections.Concurrent.ConcurrentBag<CoordinatorDecisionStore.DurableDecisionReservation> held = [];

        await Task.WhenAll(Enumerable.Range(0, attempts).Select(_ => Task.Run(() =>
        {
            if (store.TryReserveAdmission(out CoordinatorDecisionStore.DurableDecisionReservation? reservation))
            {
                Interlocked.Increment(ref granted);
                held.Add(reservation!);
            }
        }, ct)));

        // Exactly the budget is granted — never more, and all slots are used.
        Assert.Equal(capacity, granted);
        Assert.Equal(capacity, store.GetCapacityStats().Reserved);

        foreach (CoordinatorDecisionStore.DurableDecisionReservation reservation in held)
            reservation.Dispose();

        Assert.Equal(0, store.GetCapacityStats().Reserved);
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }
}
