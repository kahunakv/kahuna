using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
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
