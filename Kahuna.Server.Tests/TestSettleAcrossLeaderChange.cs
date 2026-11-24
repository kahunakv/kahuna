using System.Diagnostics;
using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transaction commits on the leader of its partition, and the leadership moves before the settlement of its
/// write lands. The decision is durable, so the intent that still holds the key is settlement lag. The key must be
/// writable again without a wait for the decision deadline of the first writer, and a commit that another node
/// finalizes must not abort against it.
/// </summary>
public sealed class TestSettleAcrossLeaderChange : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSettleAcrossLeaderChange(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// The write executor of one node, which can hold or refuse each proposal that carries a settle. A held
    /// proposal that is released without a status goes to the real executor, so Kommander gives the answer.
    /// </summary>
    private sealed class SettleTap(IPartitionBatchExecutor inner) : IPartitionBatchExecutor
    {
        private readonly TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int proposals;

        private RaftOperationStatus? lastStatus;

        /// <summary>Called for each settle proposal, with its ordinal from 1; a status refuses the proposal.</summary>
        public Func<int, Task<RaftOperationStatus?>>? OnSettle { get; set; }

        /// <summary>The ordinal of a settle proposal that is carried out and then answered as failed.</summary>
        public int LostAnswerOrdinal { get; set; }

        public Task Reached => reached.Task;

        public int Proposals => Volatile.Read(ref proposals);

        public RaftOperationStatus? LastStatus => lastStatus;

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            if (!entries.Any(IsSettle))
                return await inner.ReplicateAsync(partitionId, entries, cancellationToken);

            int ordinal = Interlocked.Increment(ref proposals);
            reached.TrySetResult();

            if (OnSettle is { } onSettle && await onSettle(ordinal) is { } refused)
            {
                lastStatus = refused;
                return Failed(entries, refused);
            }

            RaftBatchReplicationResult result = await inner.ReplicateAsync(partitionId, entries, cancellationToken);

            if (ordinal == LostAnswerOrdinal)
                return Failed(entries, RaftOperationStatus.NodeIsNotLeader);

            lastStatus = result.Status;
            return result;
        }

        private static RaftBatchReplicationResult Failed(IReadOnlyList<RaftProposalEntry> entries, RaftOperationStatus status) =>
            new(false, status, HLCTimestamp.Zero, [.. entries.Select(_ => new RaftEntryResult(status, -1, HLCTimestamp.Zero))]);

        private static bool IsSettle(RaftProposalEntry entry) =>
            entry.Type == ReplicationTypes.PreparedIntent && PreparedIntentStore.DecodeDelta(entry.Data).Any(c => c is ResolveIntentCommand);
    }

    private sealed class Cluster : IAsyncDisposable
    {
        public required IRaft[] Rafts { get; init; }

        public required KahunaManager[] Managers { get; init; }

        public required SettleTap[] Taps { get; init; }

        public required MemoryInterNodeCommmunication InterNode { get; init; }

        public static string EndpointOf(int index) => $"localhost:{8001 + index}";

        public async Task<int> LeaderOf(int partition, CancellationToken ct)
        {
            int index = -1;
            await WaitUntilAsync(async () =>
            {
                for (int i = 0; i < Rafts.Length; i++)
                {
                    if (!await Rafts[i].AmILeaderIfHosted(partition, ct))
                        continue;

                    index = i;
                    return true;
                }

                return false;
            }, timeoutMs: 30_000);

            return index;
        }

        /// <summary>Moves the leadership and waits until every node names the new leader.</summary>
        public async Task MoveLeadership(int partition, int from, int to, CancellationToken ct)
        {
            RaftOperationStatus status = await Rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
            Assert.Equal(RaftOperationStatus.Success, status);

            await WaitUntilAsync(async () => await Rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
            await WaitUntilAsync(async () => !await Rafts[from].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);

            foreach (IRaft raft in Rafts)
                await WaitUntilAsync(async () => await raft.WaitForLeader(partition, ct) == EndpointOf(to), timeoutMs: 30_000);
        }

        public bool IntentLiveEverywhere(string key) => Managers.All(m => m.DurablePreparedIntentStore.Get(key) is not null);

        public bool IntentGoneEverywhere(string key) => Managers.All(m => m.DurablePreparedIntentStore.Get(key) is null);

        public async ValueTask DisposeAsync() => await LeaveCluster(Rafts[0], Rafts[1], Rafts[2]);
    }

    /// <param name="settlesDecided">Whether the leaders settle decided intents on their own. Off, only the settlement
    /// of the transaction itself and the helping pass of a blocked writer can settle an intent.</param>
    private async Task<Cluster> Assemble(int partitions, bool settlesDecided, Action<KahunaConfiguration>? configure = null)
    {
        SettleTap[] taps = new SettleTap[3];
        MemoryInterNodeCommmunication interNode = new();

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", partitions, raftLogger, kahunaLogger,
            configure: config =>
            {
                // Neither the sweep of intents past their decision deadline nor the periodic pass over old decided
                // intents may be what clears the key, unless a test asks for the second.
                config.DurableDecisionDeadlineFloorMs = 60_000;
                config.DurableMaintenanceInterval = TimeSpan.FromMinutes(1);
                config.DefaultTransactionTimeout = 60_000;
                configure?.Invoke(config);
            },
            decorateWriteBatchExecutor: node => executor => taps[node - 1] = new SettleTap(executor),
            interNode: interNode);

        foreach (IKahuna kahuna in (IKahuna[])[kahuna1, kahuna2, kahuna3])
            ((KahunaManager)kahuna).KeyValues.DurableMaintenance.SettlesDecidedIntents = settlesDecided;

        return new Cluster
        {
            Rafts = [raft1, raft2, raft3],
            Managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3],
            Taps = taps,
            InterNode = interNode
        };
    }

    private static string KeyOn(KahunaManager probe, int partition, string tag)
    {
        string random = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; i < 4_096; i++)
        {
            string candidate = $"{tag}{i}/{random}";
            if (probe.KeyValues.LocateDurablePartition(candidate).PartitionId == partition)
                return candidate;
        }

        throw new InvalidOperationException($"no key found for partition {partition}");
    }

    private static async Task<TransactionHandle> Begin(KahunaManager session, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static async Task Set(KahunaManager session, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        KeyValueResponseType type = await RetryOnMustRetryAsync(async () =>
        {
            (KeyValueResponseType answer, _, _) = await session.LocateAndTrySetKeyValue(
                handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0,
                KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            return answer;
        }, t => t);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task Delete(KahunaManager session, TransactionHandle handle, string key, CancellationToken ct)
    {
        KeyValueResponseType type = await RetryOnMustRetryAsync(async () =>
        {
            (KeyValueResponseType answer, _, _) = await session.LocateAndTryDeleteKeyValue(
                handle.TransactionId, key, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            return answer;
        }, t => t);

        Assert.Equal(KeyValueResponseType.Deleted, type);
    }

    private static async Task<HLCTimestamp> CommitWrite(KahunaManager session, string key, string value, CancellationToken ct)
    {
        TransactionHandle writer = await Begin(session, key, ct);
        await Set(session, writer, key, value, ct);

        TransactionCommitAnswer answer = await session.LocateAndCommitTransaction(writer, ct);
        Assert.Equal(KeyValueResponseType.Committed, answer.Type);

        return writer.TransactionId;
    }

    private static async Task<string?> Read(KahunaManager reader, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).AsTask(),
            r => r.Item1);

        return type == KeyValueResponseType.Get ? Encoding.UTF8.GetString(entry!.Value!) : null;
    }

    private static void Log(string line) => TestContext.Current.TestOutputHelper?.WriteLine(line);

    private static async Task Seed(Cluster cluster, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await cluster.Managers[0].LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>One cluster with a decided write whose settlement is held on the old leader, after the leadership of
    /// its partition moved to the successor.</summary>
    private sealed record Blocked(int OldLeader, int Successor, int Third, string Key, HLCTimestamp Writer, TransactionHandle EarlySession, string FreshKey)
    {
        /// <summary>Lets the held settlement run.</summary>
        public required Action Release { get; init; }
    }

    /// <summary>
    /// Commits a write on the leader of partition 1 with its deferred settlement held before it starts, and moves
    /// the leadership. The early session begins before the move, so the old leader owns it.
    /// </summary>
    private static async Task<Blocked> BlockKey(Cluster cluster, CancellationToken ct)
    {
        int leader = await cluster.LeaderOf(1, ct);
        int successor = (leader + 1) % 3;
        int third = (leader + 2) % 3;

        string key = KeyOn(cluster.Managers[0], 1, "held");
        string fresh = KeyOn(cluster.Managers[0], 1, "fresh");
        await Seed(cluster, key, ct);

        TransactionHandle early = await Begin(cluster.Managers[third], fresh, ct);

        TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);
        DurableTransactionFinalizer finalizer = cluster.Managers[leader].TransactionCoordinator.DurableFinalizerForTests;
        finalizer.TestBeforeDeferredResolutionHook = token => open.Task.WaitAsync(token);

        HLCTimestamp writer = await CommitWrite(cluster.Managers[third], key, "w", ct);
        finalizer.TestBeforeDeferredResolutionHook = null;

        await WaitUntilAsync(() => Task.FromResult(cluster.IntentLiveEverywhere(key)), timeoutMs: 10_000);
        await cluster.MoveLeadership(1, leader, successor, ct);

        return new Blocked(leader, successor, third, key, writer, early, fresh) { Release = () => open.TrySetResult() };
    }

    /// <summary>
    /// The session began on the old leader, which finalizes it and does not lead the partition any more. The helping
    /// pass must run on the new leader: the holder is decided, so the commit waits for nothing and succeeds whichever
    /// node the client sends it to. The write it commits over is the one the decided writer left.
    /// </summary>
    [Theory]
    [InlineData("third")]
    [InlineData("successor")]
    [InlineData("old leader")]
    public async Task CommitFinalizedByTheOldLeader_SettlesTheDecidedHolderOnTheNewLeader(string sender)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: false);
        Blocked blocked = await BlockKey(cluster, ct);

        KahunaManager client = cluster.Managers[sender switch
        {
            "third" => blocked.Third,
            "successor" => blocked.Successor,
            _ => blocked.OldLeader
        }];

        using MetricCapture metrics = new("result", "kahuna.durable_tx.helping_forwards");

        await Delete(client, blocked.EarlySession, blocked.Key, ct);
        await Set(client, blocked.EarlySession, blocked.FreshKey, "t", ct);

        Assert.True(cluster.IntentLiveEverywhere(blocked.Key), "the decided intent must still hold the key when the commit runs");

        TransactionCommitAnswer answer = await client.LocateAndCommitTransaction(blocked.EarlySession, ct);
        Log($"commit: {answer.Type} {answer.Refusal} {answer.Reason}");

        Assert.Equal(KeyValueResponseType.Committed, answer.Type);
        Assert.True(metrics.Total("kahuna.durable_tx.helping_forwards", "inspected") >= 1);

        Assert.Null(await Read(cluster.Managers[blocked.Third], blocked.Key, ct));
        Assert.Equal("t", await Read(cluster.Managers[blocked.Third], blocked.FreshKey, ct));

        // The held settlement arrives after another transaction wrote the key: it must change nothing.
        blocked.Release();
        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(blocked.Key)), timeoutMs: 10_000);
        await Task.Delay(300, ct);

        foreach (KahunaManager reader in cluster.Managers)
            Assert.Null(await Read(reader, blocked.Key, ct));
    }

    /// <summary>A session that begins after the move is finalized by the new leader, whose helping pass needs no
    /// forward.</summary>
    [Fact]
    public async Task CommitFinalizedByTheNewLeader_SettlesTheDecidedHolder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: false);
        Blocked blocked = await BlockKey(cluster, ct);

        await CommitWrite(cluster.Managers[blocked.Third], blocked.Key, "late", ct);

        Assert.Equal("late", await Read(cluster.Managers[blocked.OldLeader], blocked.Key, ct));
        blocked.Release();
    }

    /// <summary>
    /// The failure needs no old session: a session that begins after the move, on a coordinator partition the old
    /// leader still leads, is finalized by a node that does not lead the written partition.
    /// </summary>
    [Fact]
    public async Task CommitFinalizedByALeaderOfAnotherPartition_SettlesTheDecidedHolder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(2, settlesDecided: false);

        int leader = await cluster.LeaderOf(1, ct);
        int other = await cluster.LeaderOf(2, ct);
        if (other != leader)
            await cluster.MoveLeadership(2, other, leader, ct);

        Blocked blocked = await BlockKey(cluster, ct);

        string coordinatorKey = KeyOn(cluster.Managers[0], 2, "coord");
        TransactionHandle session = await Begin(cluster.Managers[blocked.Third], coordinatorKey, ct);
        Assert.Equal(blocked.OldLeader + 1, session.TransactionId.N);

        await Set(cluster.Managers[blocked.Third], session, blocked.Key, "other", ct);
        Assert.True(cluster.IntentLiveEverywhere(blocked.Key));

        TransactionCommitAnswer answer = await cluster.Managers[blocked.Third].LocateAndCommitTransaction(session, ct);
        Log($"commit: {answer.Type} {answer.Refusal} {answer.Reason}");
        Assert.Equal(KeyValueResponseType.Committed, answer.Type);

        Assert.Equal("other", await Read(cluster.Managers[blocked.Third], blocked.Key, ct));
        blocked.Release();
    }

    /// <summary>
    /// The leadership moves again while the new leader settles the holder for the forwarded pass. The pass reports a
    /// decided holder and nothing settled, so the commit waits under its lease, asks the third leader and commits.
    /// </summary>
    [Fact]
    public async Task LeaderChangeDuringTheForwardedPass_TheNextPassSettlesTheHolder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: false);
        Blocked blocked = await BlockKey(cluster, ct);

        cluster.Taps[blocked.Successor].OnSettle = async ordinal =>
        {
            if (ordinal == 1)
                await cluster.MoveLeadership(1, blocked.Successor, blocked.Third, ct);

            return null;
        };

        await Set(cluster.Managers[blocked.Third], blocked.EarlySession, blocked.Key, "t", ct);

        TransactionCommitAnswer answer = await cluster.Managers[blocked.Third].LocateAndCommitTransaction(blocked.EarlySession, ct);
        Log($"commit: {answer.Type} {answer.Refusal} {answer.Reason}");

        Assert.Equal(1, cluster.Taps[blocked.Successor].Proposals);
        // The writer staged its value on the leader that was deposed, so the commit may be refused for its lost
        // staging. It must not be refused for holders nobody looked at.
        Assert.NotEqual(CommitRefusalClass.PrepareExhausted, answer.Refusal);

        await CommitWrite(cluster.Managers[blocked.Third], blocked.Key, "after", ct);
        Assert.Equal("after", await Read(cluster.Managers[blocked.OldLeader], blocked.Key, ct));
        blocked.Release();
    }

    /// <summary>
    /// An undecided holder on a partition led elsewhere is counted and left alone: the commit keeps the short round
    /// budget of a live conflict, and the holder's record and intent stay as they are.
    /// </summary>
    [Fact]
    public async Task UndecidedHolderOnARemoteLeader_KeepsTheRoundBudgetAndIsNotTouched()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: true);

        int leader = await cluster.LeaderOf(1, ct);
        int successor = (leader + 1) % 3;
        int third = (leader + 2) % 3;
        string key = KeyOn(cluster.Managers[0], 1, "undecided");

        TransactionHandle session = await Begin(cluster.Managers[third], key, ct);

        await cluster.MoveLeadership(1, leader, successor, ct);
        await Set(cluster.Managers[third], session, key, "t", ct);

        HLCTimestamp holder = await InstallUndecidedHolder(cluster.Managers[successor], cluster.Rafts[successor], key, withRecord: true, ct);

        long start = Stopwatch.GetTimestamp();
        TransactionCommitAnswer answer = await cluster.Managers[leader].LocateAndCommitTransaction(session, ct);
        TimeSpan elapsed = Stopwatch.GetElapsedTime(start);
        Log($"commit: {answer.Type} {answer.Refusal} {answer.Reason} in {elapsed.TotalMilliseconds:F0} ms");

        Assert.Equal(KeyValueResponseType.Aborted, answer.Type);
        Assert.Equal(CommitRefusalClass.PrepareExhausted, answer.Refusal);
        Assert.Contains("1 undecided holders", answer.Reason);
        Assert.True(elapsed < TimeSpan.FromSeconds(5), "a live conflict must break inside the round budget");

        Assert.Equal(holder, cluster.Managers[successor].DurablePreparedIntentStore.Get(key)!.TransactionId);
        Assert.Equal(TransactionDecision.Undecided, cluster.Managers[successor].DurableTransactionRecordStore.Get(holder, 1)!.Decision);
    }

    private static async Task<HLCTimestamp> InstallUndecidedHolder(KahunaManager leader, IRaft raft, string key, bool withRecord, CancellationToken ct)
    {
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp deadline = new(now.N, now.L + 60_000, now.C);

        PreparedIntent intent = new(now, 1, key, ManifestHash: 0, RecordAnchorKey: key, CommitTimestamp: now,
            State: KeyValueState.Set, Value: "raw"u8.ToArray(), Bucket: null, Revision: 0, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: -1, BaseState: KeyValueState.Undefined, RecoveryDeadline: deadline,
            Resolution: PreparedIntentResolution.Pending);

        List<(string LogType, byte[] Payload)> bundle = [];
        if (withRecord)
            bundle.Add((ReplicationTypes.TransactionRecord, TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(
                now, 1, key, key, now, deadline, 0, [new TransactionParticipantRef(key, KeyValueDurability.Persistent)], now, now)])));
        bundle.Add((ReplicationTypes.PreparedIntent, PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(intent)])));

        int partition = leader.KeyValues.LocateDurablePartition(key).PartitionId;
        DurableBundleWireReply? reply = await leader.DurableBundleLocal(partition, bundle, terminal: false, stage: 0, [], ct);
        Assert.True(reply!.Value.PrepareAcknowledged);

        return now;
    }

    /// <summary>
    /// A sender that does not host the partition can name a replica that does not lead it: the receiver sends the
    /// request to the leader it knows, once the request may still be forwarded, and answers that it could not look
    /// otherwise. A node that is shutting down, and one that cannot be reached, answer the same.
    /// </summary>
    [Fact]
    public async Task ForwardedPass_IsRedirectedToTheLeaderOnceAndNeverPastTheForwardBudget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: false);
        Blocked blocked = await BlockKey(cluster, ct);
        HLCTimestamp asker = new(9, 1, 0);

        using (ForwardedRequestScope.EnterTerminal())
        {
            Assert.Equal(KeyValueResponseType.MustRetry,
                (await cluster.Managers[blocked.Third].SettleDecidedBlockers(1, [blocked.Key], asker, 1, ct)).Type);
        }

        Assert.True(cluster.IntentLiveEverywhere(blocked.Key));

        // The identity that asks never settles its own intent.
        Assert.Equal((KeyValueResponseType.Get, 0, 0, 0),
            await cluster.Managers[blocked.Successor].SettleDecidedBlockers(1, [blocked.Key], blocked.Writer, 1, ct));

        (KeyValueResponseType type, int settled, int decided, int undecided) =
            await cluster.Managers[blocked.Third].SettleDecidedBlockers(1, [blocked.Key], asker, 1, ct);
        Assert.Equal((KeyValueResponseType.Get, 1, 1, 0), (type, settled, decided, undecided));

        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(blocked.Key)), timeoutMs: 10_000);

        cluster.Managers[blocked.Successor].KeyValues.SignalShutdown();
        Assert.Equal(KeyValueResponseType.MustRetry,
            (await cluster.Managers[blocked.Successor].SettleDecidedBlockers(1, [blocked.Key], asker, 1, ct)).Type);

        PreparedIntent probe = new(asker, 1, blocked.Key, 0, blocked.Key, asker, KeyValueState.Set, [], null, 0, HLCTimestamp.Zero,
            false, -1, KeyValueState.Undefined, asker, PreparedIntentResolution.Pending);

        // The transport knows no node at the leader's address: the pass reports that it could not look.
        cluster.InterNode.SetNodes(new Dictionary<string, IKahuna> { [Cluster.EndpointOf(blocked.Third)] = cluster.Managers[blocked.Third] });
        Assert.Equal(BlockerHelpResult.NotInspected,
            await cluster.Managers[blocked.Third].KeyValues.TryResolveDecidedDurableBlockersAsync(1, [probe], asker, 1, ct));

        blocked.Release();
    }

    /// <summary>
    /// The new leader settles the decided intent it inherits on its own, with no other writer on the key and the
    /// decision deadline a minute away. The old leader never sends the settlement. The value is the committed one.
    /// </summary>
    [Fact]
    public async Task NewLeader_SettlesTheDecidedIntentsItInherits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using MetricCapture metrics = new("source", "kahuna.durable_tx.settled_intents");

        await using Cluster cluster = await Assemble(1, settlesDecided: true);
        Blocked blocked = await BlockKey(cluster, ct);

        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(blocked.Key)), timeoutMs: 5_000);
        Assert.True(metrics.Total("kahuna.durable_tx.settled_intents", "decided_sweep") >= 1);

        foreach (KahunaManager reader in cluster.Managers)
            Assert.Equal("w", await Read(reader, blocked.Key, ct));

        // A later commit on the key, and then the held settlement of the first one: the later value stays.
        await CommitWrite(cluster.Managers[blocked.Third], blocked.Key, "later", ct);
        blocked.Release();
        await Task.Delay(500, ct);

        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(blocked.Key)), timeoutMs: 10_000);
        foreach (KahunaManager reader in cluster.Managers)
            Assert.Equal("later", await Read(reader, blocked.Key, ct));
    }

    /// <summary>
    /// An inherited intent whose record is undecided, and one with no record, stay exactly as they are before their
    /// deadline: at the leader change and in a maintenance pass.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task NewLeader_LeavesAnUndecidedInheritedIntentAlone(bool withRecord)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: true);

        int leader = await cluster.LeaderOf(1, ct);
        int successor = (leader + 1) % 3;
        string key = KeyOn(cluster.Managers[0], 1, "undecided");

        HLCTimestamp holder = await InstallUndecidedHolder(cluster.Managers[leader], cluster.Rafts[leader], key, withRecord, ct);
        await WaitUntilAsync(() => Task.FromResult(cluster.IntentLiveEverywhere(key)), timeoutMs: 10_000);

        await cluster.MoveLeadership(1, leader, successor, ct);
        await Task.Delay(500, ct);
        await cluster.Managers[successor].KeyValues.SettleInheritedIntents(ct);
        await cluster.Managers[successor].KeyValues.RecoverPreparedIntents(ct);

        foreach (KahunaManager manager in cluster.Managers)
        {
            Assert.Equal(holder, manager.DurablePreparedIntentStore.Get(key)?.TransactionId);
            Assert.Equal(withRecord ? TransactionDecision.Undecided : null, manager.DurableTransactionRecordStore.Get(holder, 1)?.Decision);
        }
    }

    /// <summary>The first settlement the new leader proposes fails. The pass over the inherited intents repeats after
    /// a short wait; the next maintenance tick is a minute away.</summary>
    [Fact]
    public async Task NewLeader_WhoseFirstSettlementFails_SettlesTheInheritedIntentSoonAfter()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: true);

        foreach (SettleTap tap in cluster.Taps)
            tap.OnSettle = ordinal => Task.FromResult<RaftOperationStatus?>(ordinal == 1 ? RaftOperationStatus.Errored : null);

        Blocked blocked = await BlockKey(cluster, ct);

        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(blocked.Key)), timeoutMs: 5_000);
        Assert.True(cluster.Taps[blocked.Successor].Proposals >= 2);
        Assert.Equal("w", await Read(cluster.Managers[blocked.Third], blocked.Key, ct));

        blocked.Release();
    }

    /// <summary>A write that commits after the leadership arrived is not inherited, also while the pass over an older
    /// inherited intent still repeats: its own settlement is in flight.</summary>
    [Fact]
    public async Task NewLeader_LeavesAYoungWriteOfItsOwnToItsSettlement()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: true);

        int leader = await cluster.LeaderOf(1, ct);
        int successor = (leader + 1) % 3;
        string undecided = KeyOn(cluster.Managers[0], 1, "undecided");
        string young = KeyOn(cluster.Managers[0], 1, "young");

        await InstallUndecidedHolder(cluster.Managers[leader], cluster.Rafts[leader], undecided, withRecord: true, ct);
        await WaitUntilAsync(() => Task.FromResult(cluster.IntentLiveEverywhere(undecided)), timeoutMs: 10_000);
        await cluster.MoveLeadership(1, leader, successor, ct);

        TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);
        DurableTransactionFinalizer finalizer = cluster.Managers[successor].TransactionCoordinator.DurableFinalizerForTests;
        finalizer.TestBeforeDeferredResolutionHook = token => open.Task.WaitAsync(token);

        await CommitWrite(cluster.Managers[successor], young, "w", ct);
        finalizer.TestBeforeDeferredResolutionHook = null;

        Assert.True(await cluster.Managers[successor].KeyValues.SettleInheritedIntents(ct), "the undecided intent keeps the pass repeating");
        await Task.Delay(500, ct);
        Assert.NotNull(cluster.Managers[successor].DurablePreparedIntentStore.Get(young));

        open.TrySetResult();
        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(young)), timeoutMs: 10_000);
    }

    /// <summary>
    /// The settlement of a write never arrives, with no leader change. The intent is too young for the periodic pass
    /// at first, and the pass settles it once it outlived a maintenance tick.
    /// </summary>
    [Fact]
    public async Task PeriodicPass_SettlesADecidedIntentThatOutlivedATick_AndLeavesAYoungerOne()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: true, config => config.DurableMaintenanceInterval = TimeSpan.FromSeconds(4));

        int leader = await cluster.LeaderOf(1, ct);
        string key = KeyOn(cluster.Managers[0], 1, "aged");
        await Seed(cluster, key, ct);

        TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);
        DurableTransactionFinalizer finalizer = cluster.Managers[leader].TransactionCoordinator.DurableFinalizerForTests;
        finalizer.TestBeforeDeferredResolutionHook = token => open.Task.WaitAsync(token);

        long start = Stopwatch.GetTimestamp();
        await CommitWrite(cluster.Managers[leader], key, "w", ct);
        finalizer.TestBeforeDeferredResolutionHook = null;

        await cluster.Managers[leader].KeyValues.RecoverPreparedIntents(ct);
        Assert.NotNull(cluster.Managers[leader].DurablePreparedIntentStore.Get(key));

        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(key)), timeoutMs: 20_000);
        Assert.True(Stopwatch.GetElapsedTime(start) >= TimeSpan.FromSeconds(3.5), "a settlement in flight must not be duplicated");

        Assert.Equal("w", await Read(cluster.Managers[(leader + 1) % 3], key, ct));
        open.TrySetResult();
    }

    /// <summary>
    /// The settlement is in its proposal when the leadership moves, and the proposal fails: with the answer Kommander
    /// gives, or with one of the two it can give. The settlement is sent again and reaches the new leader, which has
    /// no pass of its own here.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(RaftOperationStatus.NodeIsNotLeader)]
    [InlineData(RaftOperationStatus.TermMismatch)]
    public async Task SettleThatFailsAtTheStepDown_IsSentAgainToTheNewLeader(RaftOperationStatus? injected)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using MetricCapture metrics = new("result", "kahuna.durable_tx.settle_resends");

        await using Cluster cluster = await Assemble(1, settlesDecided: false);

        int leader = await cluster.LeaderOf(1, ct);
        int successor = (leader + 1) % 3;
        string key = KeyOn(cluster.Managers[0], 1, "held");
        await Seed(cluster, key, ct);

        TaskCompletionSource<RaftOperationStatus?> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Taps[leader].OnSettle = ordinal => ordinal == 1 ? gate.Task : Task.FromResult<RaftOperationStatus?>(null);

        await CommitWrite(cluster.Managers[leader], key, "w", ct);
        await cluster.Taps[leader].Reached;
        await cluster.MoveLeadership(1, leader, successor, ct);

        Assert.True(cluster.IntentLiveEverywhere(key));
        gate.SetResult(injected);

        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(key)), timeoutMs: 10_000);

        RaftOperationStatus? status = cluster.Taps[leader].LastStatus;
        Log($"the first settle ended with {status}");
        Assert.True(status is RaftOperationStatus.NodeIsNotLeader or RaftOperationStatus.TermMismatch);

        Assert.Equal(1, cluster.Taps[leader].Proposals);
        Assert.True(cluster.Taps[successor].Proposals >= 1);
        Assert.True(metrics.Total("kahuna.durable_tx.settle_resends", "settled") >= 1);

        foreach (KahunaManager reader in cluster.Managers)
            Assert.Equal("w", await Read(reader, key, ct));

        await CommitWrite(cluster.Managers[successor], key, "next", ct);
        Assert.Equal("next", await Read(cluster.Managers[leader], key, ct));
    }

    /// <summary>A settlement that keeps failing is sent a bounded number of times, and the intent stays for the
    /// passes of the partition leader.</summary>
    [Fact]
    public async Task SettleThatKeepsFailing_IsSentABoundedNumberOfTimes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: false);

        int leader = await cluster.LeaderOf(1, ct);
        string key = KeyOn(cluster.Managers[0], 1, "held");
        await Seed(cluster, key, ct);

        cluster.Taps[leader].OnSettle = _ => Task.FromResult<RaftOperationStatus?>(RaftOperationStatus.Errored);
        await CommitWrite(cluster.Managers[leader], key, "w", ct);

        await WaitUntilAsync(() => Task.FromResult(cluster.Taps[leader].Proposals >= 6), timeoutMs: 10_000);
        await Task.Delay(2_000, ct);

        Assert.Equal(6, cluster.Taps[leader].Proposals);
        Assert.True(cluster.IntentLiveEverywhere(key));
        Assert.Equal("w", await Read(cluster.Managers[leader], key, ct));
    }

    /// <summary>A node that is shutting down does not send a failed settlement again; the new leader of the partition
    /// settles the intent it inherits.</summary>
    [Fact]
    public async Task SettleThatFailsOnANodeThatStops_IsLeftToTheNewLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: false);

        int leader = await cluster.LeaderOf(1, ct);
        int successor = (leader + 1) % 3;
        string key = KeyOn(cluster.Managers[0], 1, "held");
        await Seed(cluster, key, ct);

        TaskCompletionSource<RaftOperationStatus?> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Taps[leader].OnSettle = _ => gate.Task;

        await CommitWrite(cluster.Managers[leader], key, "w", ct);
        await cluster.Taps[leader].Reached;

        cluster.Managers[leader].KeyValues.SignalShutdown();
        gate.SetResult(RaftOperationStatus.NodeIsNotLeader);
        await Task.Delay(1_000, ct);

        Assert.Equal(1, cluster.Taps[leader].Proposals);
        Assert.True(cluster.IntentLiveEverywhere(key));

        cluster.Managers[successor].KeyValues.DurableMaintenance.SettlesDecidedIntents = true;
        await cluster.MoveLeadership(1, leader, successor, ct);

        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(key)), timeoutMs: 5_000);
        Assert.Equal("w", await Read(cluster.Managers[successor], key, ct));
    }

    /// <summary>
    /// The settlement lands and its answer is lost, so it is sent again after another transaction may have written
    /// the key. The second application changes nothing.
    /// </summary>
    [Fact]
    public async Task SettleWhoseAnswerIsLost_AppliesOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble(1, settlesDecided: false);

        int leader = await cluster.LeaderOf(1, ct);
        string key = KeyOn(cluster.Managers[0], 1, "held");
        await Seed(cluster, key, ct);

        TaskCompletionSource<RaftOperationStatus?> second = new(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Taps[leader].LostAnswerOrdinal = 1;
        cluster.Taps[leader].OnSettle = ordinal => ordinal == 2 ? second.Task : Task.FromResult<RaftOperationStatus?>(null);

        await CommitWrite(cluster.Managers[leader], key, "w", ct);
        await WaitUntilAsync(() => Task.FromResult(cluster.Taps[leader].Proposals >= 2), timeoutMs: 10_000);
        Assert.Null(cluster.Managers[leader].DurablePreparedIntentStore.Get(key));

        Task<HLCTimestamp> later = CommitWrite(cluster.Managers[leader], key, "later", ct);
        await Task.Delay(200, ct);
        second.SetResult(null);
        await later;

        await WaitUntilAsync(() => Task.FromResult(cluster.IntentGoneEverywhere(key)), timeoutMs: 10_000);

        foreach (KahunaManager reader in cluster.Managers)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
                () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).AsTask(),
                r => r.Item1);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal("later", Encoding.UTF8.GetString(entry!.Value!));
            Assert.Equal(2, entry.Revision);
        }
    }
}
