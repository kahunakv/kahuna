using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// What a read of named keys waits for on one node while the delivery of a committed batch is held: a read of a key
/// that the batch can change waits for the delivery, and a read of any other key answers.
/// </summary>
public sealed class TestKeyScopedApplyWait : KeyScopedApplyWaitTestBase
{
    public TestKeyScopedApplyWait(ITestOutputHelper outputHelper) : base(outputHelper)
    {
    }

    /// <summary>The ways to read one named key. Each answers the value, or null when the read was not served.</summary>
    public enum ReadKind
    {
        Get,
        SnapshotGet,
        Exists,
        GetMany,
        ExistsMany,
        Probe
    }

    private static async Task<string?> Read(IKahuna kahuna, ReadKind kind, string key, HLCTimestamp snapshot, CancellationToken ct)
    {
        KeyValueResponseType type;
        ReadOnlyKeyValueEntry? entry;

        switch (kind)
        {
            case ReadKind.Get:
                (type, entry) = await kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                break;

            case ReadKind.SnapshotGet:
                (type, entry) = await kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
                break;

            case ReadKind.Exists:
                (type, entry) = await kahuna.LocateAndTryExistsValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                return type == KeyValueResponseType.Exists ? "revision " + entry!.Revision : null;

            case ReadKind.GetMany:
                (type, _, _, entry) = Assert.Single(await kahuna.LocateAndTryGetManyValues(
                    HLCTimestamp.Zero, HLCTimestamp.Zero, [(key, -1, KeyValueDurability.Persistent)], ct));
                break;

            case ReadKind.ExistsMany:
                (type, _, _, entry) = Assert.Single(await kahuna.LocateAndTryExistsManyValues(
                    HLCTimestamp.Zero, HLCTimestamp.Zero, [(key, -1, KeyValueDurability.Persistent)], ct));
                return type == KeyValueResponseType.Exists ? "revision " + entry!.Revision : null;

            default:
                KeyValueProbeResult probe = Assert.Single(await kahuna.LocateAndTryProbeMany(
                    HLCTimestamp.Zero, HLCTimestamp.Zero, [key], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct));
                (type, entry) = (probe.Type, probe.Entry);
                break;
        }

        return type == KeyValueResponseType.Get ? Encoding.UTF8.GetString(entry!.Value!) : null;
    }

    /// <summary>A read that the hold must keep back: it is not answered while the hold lasts, and never with what the
    /// key held before. A wait that outlives the confirmation timeout answers nothing, which counts as held.</summary>
    private static async Task AssertWaits(Task<string?> read, string before)
    {
        await Task.WhenAny(read, Task.Delay(Pending, TestContext.Current.CancellationToken));

        if (read.IsCompleted)
            Assert.NotEqual(before, await read);

        Assert.False(read.IsCompletedSuccessfully && read.Result is not null, "the read answered while the delivery of its key was held");
    }

    private static TheoryData<ReadKind> AllReads()
    {
        TheoryData<ReadKind> kinds = [];
        foreach (ReadKind kind in Enum.GetValues<ReadKind>())
            kinds.Add(kind);

        return kinds;
    }

    public static TheoryData<ReadKind> Reads => AllReads();

    [Theory]
    [MemberData(nameof(Reads))]
    public async Task ADirectWriteThatIsNotDelivered_HoldsBackAReadOfItsKey_AndNoOtherRead(ReadKind kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        await Set(node.Kahuna, "wait/written", "old", ct);
        await Set(node.Kahuna, "wait/other", "other", ct);
        await WaitUntilAsync(() => !Marked(node, "wait/written") && !Marked(node, "wait/other"));

        string? before = await Read(node.Kahuna, kind, "wait/written", HLCTimestamp.Zero, ct);
        string? other = await Read(node.Kahuna, kind, "wait/other", HLCTimestamp.Zero, ct);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, "wait/written"), ct);

        // Acknowledged at quorum, with its entry not delivered.
        await Set(node.Kahuna, "wait/written", "new", ct);

        HLCTimestamp snapshot = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        await Task.Delay(5, ct);

        Assert.True(Marked(node, "wait/written"));
        Assert.Equal(other ?? "other", await Promptly(() => Read(node.Kahuna, kind, "wait/other", snapshot, ct)));

        Task<string?> read = Read(node.Kahuna, kind, "wait/written", snapshot, ct);
        await AssertWaits(read, before ?? "old");

        await held.Resume();

        Assert.False(Marked(node, "wait/written"));
        await WaitUntilAsync(async () => await Read(node.Kahuna, kind, "wait/written", snapshot, ct) is { } after && after != before);
    }

    [Fact]
    public async Task ABatchRead_WaitsWhenOneOfItsKeysIsNotDelivered()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        string[] keys = [.. Enumerable.Range(0, 8).Select(i => "wait/k" + i)];
        foreach (string key in keys)
            await Set(node.Kahuna, key, "old", ct);
        await WaitUntilAsync(() => !keys.Any(key => Marked(node, key)));

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, keys[0]), ct);
        await Set(node.Kahuna, keys[7], "new", ct);

        List<(string, long, KeyValueDurability)> Batch(IEnumerable<string> batch) => [.. batch.Select(key => (key, -1L, KeyValueDurability.Persistent))];

        // Without the written key the batch answers.
        Assert.All(
            await Promptly(() => node.Kahuna.LocateAndTryGetManyValues(HLCTimestamp.Zero, HLCTimestamp.Zero, Batch(keys[..7]), ct)),
            static answer => Assert.Equal(KeyValueResponseType.Get, answer.Item1));
        Assert.All(
            await Promptly(() => node.Kahuna.LocateAndTryProbeMany(HLCTimestamp.Zero, HLCTimestamp.Zero, keys[..7], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct)),
            static answer => Assert.Equal(KeyValueResponseType.Get, answer.Type));

        var batch = node.Kahuna.LocateAndTryGetManyValues(HLCTimestamp.Zero, HLCTimestamp.Zero, Batch(keys), ct);
        Task<KeyValueProbeResult[]> probe = node.Kahuna.LocateAndTryProbeMany(
            HLCTimestamp.Zero, HLCTimestamp.Zero, keys, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct);

        await Task.Delay(Pending, ct);
        Assert.False(batch.IsCompleted);
        Assert.False(probe.IsCompleted);

        await held.Resume();

        Assert.Equal("new", Encoding.UTF8.GetString((await batch)[7].Item4!.Value!));
        Assert.Equal("new", Encoding.UTF8.GetString((await probe)[7].Entry!.Value!));
    }

    [Fact]
    public async Task ABatchReadOfManyKeys_WaitsForTheWholeFrontier()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        string[] keys = [.. Enumerable.Range(0, 513).Select(i => "wait/k" + i)];
        await Set(node.Kahuna, "wait/written", "old", ct);
        await WaitUntilAsync(() => !Marked(node, "wait/written"));

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, keys[0]), ct);
        await Set(node.Kahuna, "wait/written", "new", ct);

        List<(string, long, KeyValueDurability)> Batch(IEnumerable<string> batch) => [.. batch.Select(key => (key, -1L, KeyValueDurability.Persistent))];

        Assert.Equal(512, (await Promptly(() => node.Kahuna.LocateAndTryGetManyValues(HLCTimestamp.Zero, HLCTimestamp.Zero, Batch(keys[..512]), ct))).Count);

        var batch = node.Kahuna.LocateAndTryGetManyValues(HLCTimestamp.Zero, HLCTimestamp.Zero, Batch(keys), ct);
        await Task.Delay(Pending, ct);
        Assert.False(batch.IsCompleted);

        await held.Resume();
        Assert.Equal(513, (await batch).Count);
    }

    [Fact]
    public async Task ThePeriodicMaintenance_GivesUpTheMarksOfIdlePartitions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct, options => options.CollectionInterval = TimeSpan.FromMilliseconds(100));

        await Set(node.Kahuna, "wait/k", "value", ct);
        await WaitUntilAsync(() => ManagerOf(node).KeyValues.UndeliveredKeys.PartitionCount == 0);

        await Set(node.Kahuna, "wait/k", "later", ct);
        Assert.Equal("later", await Read(node.Kahuna, ReadKind.Get, "wait/k", HLCTimestamp.Zero, ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ACommitThatIsNotDelivered_HoldsBackReadsOfItsKeys_AndNoOtherRead(bool onePhase)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct, options =>
        {
            options.OnePhaseApplyTimeValidation = onePhase;
            options.DurableMaterializeOnResolve = onePhase;
        });

        foreach (string key in (string[])["wait/a", "wait/b", "wait/other"])
            await Set(node.Kahuna, key, "old", ct);

        TransactionHandle tx = await Start(node.Kahuna, "wait/a", ct);
        await Stage(node.Kahuna, tx, "wait/a", "new", ct);
        await Stage(node.Kahuna, tx, "wait/b", "new", ct);

        // A transaction that reads while the delivery is held, begun before it: the session itself needs no read.
        TransactionHandle reader = await Start(node.Kahuna, "wait/other", ct);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, "wait/a"), ct);

        long before = node.Raft.GetCommitIndex(PartitionOf(node, "wait/a"));
        Task<KeyValueResponseType> commit = Task.Run(async () => (await node.Kahuna.LocateAndCommitTransaction(tx, ct)).Type, ct);

        // Marked from the dispatch on. The wait of a read starts with the commit of the batch.
        await WaitUntilAsync(() => Marked(node, "wait/a") && Marked(node, "wait/b"));
        await WaitUntilAsync(() => node.Raft.GetCommitIndex(PartitionOf(node, "wait/a")) > before);

        Assert.Equal("old", await Promptly(() => Read(node.Kahuna, ReadKind.Get, "wait/other", HLCTimestamp.Zero, ct)));

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await Promptly(() => node.Kahuna.LocateAndTryGetValue(
            reader.TransactionId, "wait/other", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: reader.CoordinatorKey, operationId: TransactionOperationId.NewRandom()).AsTask());
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("old", Encoding.UTF8.GetString(entry!.Value!));

        Assert.False(commit.IsCompleted);
        await AssertWaits(Read(node.Kahuna, ReadKind.Get, "wait/b", HLCTimestamp.Zero, ct), "old");

        await held.Resume();

        Assert.Equal(KeyValueResponseType.Committed, await commit);
        Assert.Equal("new", await Read(node.Kahuna, ReadKind.Get, "wait/a", HLCTimestamp.Zero, ct));
        Assert.Equal("new", await Read(node.Kahuna, ReadKind.Get, "wait/b", HLCTimestamp.Zero, ct));

        await node.Kahuna.LocateAndRollbackTransaction(reader, ct);
        await WaitUntilAsync(() => !Marked(node, "wait/a") && !Marked(node, "wait/b"));
    }

    [Fact]
    public async Task ACopiedPageOfARangeMove_HoldsBackEveryReadOfThePartition()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        await Set(node.Kahuna, "wait/other", "other", ct);
        int partition = PartitionOf(node, "wait/other");
        await WaitUntilAsync(() => !Marked(node, "wait/other"));

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, partition, ct);

        HLCTimestamp now = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        using MemoryStream page = new();
        KvStateMachineTransfer.WritePage(
            page, [("wait/moved", new ReadOnlyKeyValueEntry("moved"u8.ToArray(), 7, HLCTimestamp.Zero, now, now, KeyValueState.Set))], hasMore: false);

        // The mover is told that the page is durable, and cuts the range over next.
        Assert.True(await node.Kahuna.ReplicateKeyValueRangePageLocal(partition, page.ToArray(), ct));

        // A read that finds nothing here would take an acknowledged key for an absent one.
        Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> read = node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wait/moved", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).AsTask();
        await Task.Delay(Pending, ct);
        Assert.False(read.IsCompleted);

        await held.Resume();

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await read;
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("moved", Encoding.UTF8.GetString(entry!.Value!));
        Assert.Equal(7, entry.Revision);

        await WaitUntilAsync(() => !Marked(node, "wait/other"));
    }

    [Fact]
    public async Task AnEphemeralRead_DoesNotWait()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "wait/key", "ephemeral"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Ephemeral, ct);
        Assert.Equal(KeyValueResponseType.Set, set);

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, "wait/key"), ct);

        // The persistent key of the same name is the one the log can change.
        await Set(node.Kahuna, "wait/key", "persistent", ct);

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await Promptly(() => node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wait/key", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct).AsTask());
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("ephemeral", Encoding.UTF8.GetString(entry!.Value!));
    }

    [Theory]
    [MemberData(nameof(Reads))]
    public async Task WithTheSwitchOff_EveryReadWaitsForTheDelivery(ReadKind kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct, static options => options.KeyScopedApplyWait = false);

        await Set(node.Kahuna, "wait/written", "old", ct);
        await Set(node.Kahuna, "wait/other", "other", ct);
        await WaitUntilAsync(() => !Marked(node, "wait/written") && !Marked(node, "wait/other"));

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, "wait/written"), ct);
        await Set(node.Kahuna, "wait/written", "new", ct);

        HLCTimestamp snapshot = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        Task<string?> read = Read(node.Kahuna, kind, "wait/other", snapshot, ct);
        await Task.Delay(Pending, ct);
        Assert.False(read.IsCompleted);

        await held.Resume();
        Assert.NotNull(await read);
    }

    /// <summary>Lets a batch reach Raft and then loses the answer of the proposal, as a timeout does.</summary>
    private sealed class AnswerLoss(IPartitionBatchExecutor inner) : IPartitionBatchExecutor
    {
        public bool Armed;

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            RaftBatchReplicationResult result = await inner.ReplicateAsync(partitionId, entries, cancellationToken);

            if (Volatile.Read(ref Armed))
            {
                Volatile.Write(ref Armed, false);
                throw new TimeoutException("the answer of the proposal was lost");
            }

            return result;
        }
    }

    [Fact]
    public async Task AProposalWhoseAnswerIsLost_HoldsBackEveryRead_UntilALaterBatchIsDelivered()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
        AnswerLoss[] losses = new AnswerLoss[3];

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", 1, raftLogger, kahunaLogger, decorateWriteBatchExecutor: id => inner => losses[id - 1] = new AnswerLoss(inner));

        IRaft[] rafts = [raft1, raft2, raft3];
        IKahuna[] kahunas = [kahuna1, kahuna2, kahuna3];

        try
        {
            int leader = -1;
            await WaitUntilAsync(async () =>
            {
                for (int i = 0; i < rafts.Length && leader < 0; i++)
                    leader = await rafts[i].AmILeaderIfHosted(1, ct) ? i : -1;

                return leader >= 0;
            }, timeoutMs: 30_000);

            KahunaManager manager = (KahunaManager)kahunas[leader];
            UndeliveredKeys marks = manager.KeyValues.UndeliveredKeys;

            await Set(kahunas[leader], "wait/written", "old", ct);
            await Set(kahunas[leader], "wait/other", "other", ct);
            await WaitUntilAsync(() => !marks.MayChange(1, "wait/written") && !marks.MayChange(1, "wait/other"));

            await using HeldDelivery held = await HeldDelivery.Start(rafts[leader], 1, ct);

            // The entry is committed on the quorum, and its producer is told that it can retry.
            Volatile.Write(ref losses[leader].Armed, true);
            (KeyValueResponseType type, _, _) = await kahunas[leader].LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "wait/written", "new"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, type);

            Assert.True(marks.MayChange(1, "wait/written"));
            Assert.True(marks.MayChange(1, "wait/other"));

            Task<string?> read = Read(kahunas[leader], ReadKind.Get, "wait/written", HLCTimestamp.Zero, ct);
            await AssertWaits(read, "old");

            await held.Resume();
            await WaitUntilAsync(async () => await Read(kahunas[leader], ReadKind.Get, "wait/written", HLCTimestamp.Zero, ct) == "new");

            // The lost entry was delivered, and nothing says so. The delivery of the next write does.
            Assert.True(marks.MayChange(1, "wait/other"));
            await Set(kahunas[leader], "wait/next", "next", ct);
            await WaitUntilAsync(() => !marks.MayChange(1, "wait/other"));
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    [Fact]
    public async Task TheReadsThatKeepTheWait_AreCounted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        await Set(node.Kahuna, "wait/written", "old", ct);
        await Set(node.Kahuna, "wait/other", "other", ct);
        await WaitUntilAsync(() => !Marked(node, "wait/written") && !Marked(node, "wait/other"));

        await using HeldDelivery held = await HeldDelivery.Start(node.Raft, PartitionOf(node, "wait/written"), ct);
        await Set(node.Kahuna, "wait/written", "new", ct);

        // Other nodes of this process record into the same counter, so it can only be asked to have grown.
        using MetricCapture counted = new("unused", "kahuna.kv.read.undelivered_key");
        Task<string?> read = Read(node.Kahuna, ReadKind.Get, "wait/written", HLCTimestamp.Zero, ct);
        Assert.True(counted.Total("kahuna.kv.read.undelivered_key") >= 1);

        await held.Resume();
        Assert.Equal("new", await read);
    }
}
