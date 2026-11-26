using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transaction that writes the keys of a committed and unsettled transaction, on three nodes. The bundle that
/// carries the settlement of the first one is proposed by the leader of the keys only, and only when that leader
/// also leads the record of the first one. Every replica installs the value of the first transaction from the
/// bundle. A leader change before the propose refuses the bundle.
/// </summary>
public sealed class TestOnePhaseOverCommittedPredecessorCluster : BaseCluster
{
    private const int Partitions = 2;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestOnePhaseOverCommittedPredecessorCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private sealed class Cluster : IAsyncDisposable
    {
        public required IRaft[] Rafts { get; init; }

        public required KahunaManager[] Managers { get; init; }

        public required BundleEntryTap[] Taps { get; init; }

        // The nodes count their commit calls by path only while something listens to their histogram.
        private readonly MetricCapture commitCalls = new("path", "kahuna.transactions.commit_call_ms");

        private readonly List<TaskCompletionSource> held = [];

        private static string EndpointOf(int index) => $"localhost:{8001 + index}";

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
        public async Task MoveLeadership(int partition, int to, CancellationToken ct)
        {
            int from = await LeaderOf(partition, ct);
            if (from == to)
                return;

            RaftOperationStatus status = await Rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
            Assert.Equal(RaftOperationStatus.Success, status);

            await WaitUntilAsync(async () => await Rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
            await WaitUntilAsync(async () => !await Rafts[from].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);

            foreach (IRaft raft in Rafts)
                await WaitUntilAsync(async () => await raft.WaitForLeader(partition, ct) == EndpointOf(to), timeoutMs: 30_000);
        }

        public int OwnerOf(TransactionHandle handle)
        {
            int owner = Array.FindIndex(Managers, manager => manager.TransactionCoordinator.HasSession(handle.TransactionId));
            Assert.True(owner >= 0, "some node must hold the session");
            return owner;
        }

        public bool HeldEverywhereBy(HLCTimestamp transactionId, string key) =>
            Managers.All(manager => manager.DurablePreparedIntentStore.Get(key)?.TransactionId == transactionId);

        public bool FreeEverywhere(string key) => Managers.All(manager => manager.DurablePreparedIntentStore.Get(key) is null);

        public long Calls(int node, CommitCallPath path) => Managers[node].TransactionCoordinator.CommitCalls.Calls(path);

        public string BundleOf(HLCTimestamp transactionId) =>
            Taps.Single(tap => tap.Prepared(transactionId)).BundleOf(transactionId);

        /// <summary>Holds the deferred resolution of each transaction that commits while <paramref name="body"/> runs,
        /// on every node, and returns the action that lets them run.</summary>
        public async Task<Action> HoldResolutions(Func<Task> body)
        {
            TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);
            held.Add(open);

            foreach (KahunaManager manager in Managers)
                manager.TransactionCoordinator.DurableFinalizerForTests.TestBeforeDeferredResolutionHook = token => open.Task.WaitAsync(token);

            try
            {
                await body();
            }
            finally
            {
                foreach (KahunaManager manager in Managers)
                    manager.TransactionCoordinator.DurableFinalizerForTests.TestBeforeDeferredResolutionHook = null;
            }

            return () => open.TrySetResult();
        }

        public async ValueTask DisposeAsync()
        {
            foreach (TaskCompletionSource open in held)
                open.TrySetResult();

            await LeaveCluster(Rafts[0], Rafts[1], Rafts[2]);
            commitCalls.Dispose();
        }
    }

    private async Task<Cluster> Assemble()
    {
        BundleEntryTap[] taps = new BundleEntryTap[3];

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", Partitions, raftLogger, kahunaLogger,
            configure: config =>
            {
                config.OnePhaseApplyTimeValidation = true;
                config.DurableMaterializeOnResolve = true;

                // Neither the sweep of intents past their decision deadline nor the pass over decided intents may
                // settle what a test holds.
                config.DurableDecisionDeadlineFloorMs = 60_000;
                config.DurableMaintenanceInterval = TimeSpan.FromMinutes(1);
                config.DefaultTransactionTimeout = 60_000;
            },
            decorateWriteBatchExecutor: node => executor => taps[node - 1] = new BundleEntryTap(executor));

        KahunaManager[] managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];
        foreach (KahunaManager manager in managers)
            manager.KeyValues.DurableMaintenance.SettlesDecidedIntents = false;

        return new Cluster { Rafts = [raft1, raft2, raft3], Managers = managers, Taps = taps };
    }

    private static string KeyOn(KahunaManager probe, int partition, string tag)
    {
        string random = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; i < 4_096; i++)
        {
            string candidate = $"{tag}{i}-{random}/k";
            if (probe.KeyValues.LocateDurablePartition(candidate).PartitionId == partition)
                return candidate;
        }

        throw new InvalidOperationException($"no key found for partition {partition}");
    }

    private static async Task<TransactionHandle> Begin(KahunaManager session, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await RetryOnMustRetryAsync(
            () => session.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey = coordinatorKey,
                    Locking = KeyValueTransactionLocking.Optimistic,
                    ReadValidation = ReadValidation.TrackAndValidate,
                    Timeout = 60_000
                }, ct), r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static async Task ReadAndWrite(KahunaManager session, TransactionHandle tx, string key, string value, CancellationToken ct)
    {
        await RetryOnMustRetryAsync(
            () => session.LocateAndTryGetValue(
                tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()).AsTask(),
            r => r.Item1);

        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTrySetKeyValue(
                tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task<TransactionHandle> Stage(KahunaManager session, string coordinatorKey, string value, CancellationToken ct, params string[] keys)
    {
        TransactionHandle tx = await Begin(session, coordinatorKey, ct);

        foreach (string key in keys)
            await ReadAndWrite(session, tx, key, value, ct);

        return tx;
    }

    private static async Task Commit(KahunaManager session, TransactionHandle tx, CancellationToken ct)
    {
        TransactionCommitAnswer answer = await RetryOnMustRetryAsync(() => session.LocateAndCommitTransaction(tx, ct), r => r.Type);
        Assert.True(answer.Type == KeyValueResponseType.Committed, $"{answer.Type} {answer.Refusal} {answer.Reason}");
    }

    private static async Task<(string? Value, long Revision)> ReadLatest(KahunaManager reader, string key, CancellationToken ct, long revision = -1)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, revision, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).AsTask(),
            r => r.Item1);

        return type == KeyValueResponseType.Get ? (Encoding.UTF8.GetString(entry!.Value!), entry.Revision) : (null, -1);
    }

    /// <summary>Records every live install on one replica and forwards it to the replica's real installer.</summary>
    private sealed class InstallSpy(IResolvedIntentInstaller inner) : IResolvedIntentInstaller
    {
        private readonly Lock gate = new();

        private readonly List<(HLCTimestamp TransactionId, string Key)> installed = [];

        public int Count(HLCTimestamp transactionId, string key)
        {
            lock (gate)
                return installed.Count(install => install == (transactionId, key));
        }

        public void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay)
        {
            lock (gate)
                installed.Add((intent.TransactionId, intent.Key));

            inner.Install(partitionId, logIndex, intent, replay);
        }

        public void CompleteEntry(int partitionId, long logIndex, bool replay) => inner.CompleteEntry(partitionId, logIndex, replay);

        public void NoteUnresolvedOnReplay(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp) =>
            inner.NoteUnresolvedOnReplay(partitionId, logIndex, transactionId, epoch, key, commitTimestamp);

        public void NoteUnresolvedOnApply(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp) =>
            inner.NoteUnresolvedOnApply(partitionId, logIndex, transactionId, epoch, key, commitTimestamp);
    }

    /// <summary>
    /// The leader of the keys commits over an unsettled transaction. Each replica installs the first transaction's
    /// values once, from the bundle, and holds the second one's intents. The leadership then moves, and the new
    /// leader serves both revisions.
    /// </summary>
    [Fact]
    public async Task CommitOnTheLeader_SettlesThePredecessorOnEveryReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble();

        string[] keys = [KeyOn(cluster.Managers[0], 1, "a"), KeyOn(cluster.Managers[0], 1, "b")];
        int leader = await cluster.LeaderOf(1, ct);
        KahunaManager node = cluster.Managers[leader];

        InstallSpy[] spies = new InstallSpy[3];
        for (int i = 0; i < 3; i++)
        {
            PreparedIntentStore store = cluster.Managers[i].DurablePreparedIntentStore;
            store.AttachResolvedIntentInstaller(spies[i] = new InstallSpy(store.ResolvedIntentInstaller!));
        }

        TransactionHandle first = default, second = default;
        Action release = await cluster.HoldResolutions(async () =>
        {
            first = await Stage(node, keys[0], "1", ct, keys);
            await Commit(node, first, ct);
            await WaitUntilAsync(() => keys.All(key => cluster.HeldEverywhereBy(first.TransactionId, key)));

            second = await Stage(node, keys[0], "2", ct, keys);
            Assert.Equal(leader, cluster.OwnerOf(second));
            long onePhase = cluster.Calls(leader, CommitCallPath.OnePhase);

            await Commit(node, second, ct);
            Assert.Equal(1, cluster.Calls(leader, CommitCallPath.OnePhase) - onePhase);
        });

        Assert.Equal("settle k,k | init | prepare k,k | commit", cluster.BundleOf(second.TransactionId));
        await WaitUntilAsync(() => keys.All(key => cluster.HeldEverywhereBy(second.TransactionId, key)));

        foreach (InstallSpy spy in spies)
            Assert.All(keys, key => Assert.Equal(1, spy.Count(first.TransactionId, key)));

        await cluster.MoveLeadership(1, (leader + 1) % 3, ct);

        foreach (string key in keys)
        {
            Assert.Equal(("2", 1L), await ReadLatest(node, key, ct));
            Assert.Equal(("1", 0L), await ReadLatest(node, key, ct, revision: 0));
        }

        release();
        await WaitUntilAsync(() => keys.All(cluster.FreeEverywhere), timeoutMs: 30_000);

        foreach (InstallSpy spy in spies)
        {
            foreach (string key in keys)
            {
                Assert.Equal(1, spy.Count(first.TransactionId, key));
                Assert.Equal(1, spy.Count(second.TransactionId, key));
            }
        }

        foreach (string key in keys)
            Assert.Equal(("2", 1L), await ReadLatest(cluster.Managers[(leader + 2) % 3], key, ct));
    }

    /// <summary>
    /// The leadership of the keys moves after the validation of the second transaction and before its propose. The
    /// bundle leaves this node by no path: it is not proposed and not forwarded, the attempt answers a retry, and
    /// the second transaction cannot commit on the new leader, which never held its staged writes. The first
    /// transaction's value stands, and a new transaction on the new leader commits over it.
    /// </summary>
    [Fact]
    public async Task LeaderChangeBeforeThePropose_RefusesTheBundle()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble();

        string key = KeyOn(cluster.Managers[0], 1, "moved");
        int leader = await cluster.LeaderOf(1, ct);
        int successor = (leader + 1) % 3;
        KahunaManager node = cluster.Managers[leader];

        TransactionHandle first = default;
        Action release = await cluster.HoldResolutions(async () =>
        {
            first = await Stage(node, key, "1", ct, key);
            await Commit(node, first, ct);
            await WaitUntilAsync(() => cluster.HeldEverywhereBy(first.TransactionId, key));
        });

        TransactionHandle second = await Stage(node, key, "2", ct, key);
        long forwards = node.KeyValues.DurableTransportCounts.OnePhaseForwards;

        DurableTransactionFinalizer finalizer = node.TransactionCoordinator.DurableFinalizerForTests;
        finalizer.TestAfterReadSetValidationHook = async _ =>
        {
            finalizer.TestAfterReadSetValidationHook = null;
            await cluster.MoveLeadership(1, successor, ct);
        };

        TransactionCommitAnswer answer = await node.LocateAndCommitTransaction(second, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, answer.Type);
        Assert.Equal(0, node.KeyValues.DurableTransportCounts.OnePhaseForwards - forwards);
        Assert.DoesNotContain(cluster.Taps, tap => tap.Prepared(second.TransactionId));

        TransactionCommitAnswer retried = await RetryOnMustRetryAsync(() => node.LocateAndCommitTransaction(second, ct), r => r.Type);
        Assert.NotEqual(KeyValueResponseType.Committed, retried.Type);
        Assert.Equal(("1", 0L), await ReadLatest(cluster.Managers[successor], key, ct));

        release();
        await WaitUntilAsync(() => cluster.FreeEverywhere(key), timeoutMs: 30_000);

        TransactionHandle third = await Stage(cluster.Managers[successor], key, "3", ct, key);
        await Commit(cluster.Managers[successor], third, ct);
        Assert.Equal(("3", 1L), await ReadLatest(node, key, ct));
    }

    /// <summary>
    /// The session of the second transaction lives on a node that does not lead the keys. That node sees the first
    /// transaction's intent on its own replica, which says nothing about the leader's state, so it takes the
    /// two-phase flow: the helping pass on the leader settles the holder, and the commit succeeds.
    /// </summary>
    [Fact]
    public async Task CommitOnANodeThatDoesNotLeadTheKeys_TakesTheTwoPhaseFlow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble();

        string key = KeyOn(cluster.Managers[0], 1, "remote");
        string coordinatorKey = KeyOn(cluster.Managers[0], 2, "coord");
        int leader = await cluster.LeaderOf(1, ct);
        int other = (leader + 1) % 3;
        await cluster.MoveLeadership(2, other, ct);

        TransactionHandle first = default;
        Action release = await cluster.HoldResolutions(async () =>
        {
            first = await Stage(cluster.Managers[leader], key, "1", ct, key);
            await Commit(cluster.Managers[leader], first, ct);
            await WaitUntilAsync(() => cluster.HeldEverywhereBy(first.TransactionId, key));
        });

        TransactionHandle second = await Stage(cluster.Managers[other], coordinatorKey, "2", ct, key);
        Assert.Equal(other, cluster.OwnerOf(second));
        Assert.True(cluster.HeldEverywhereBy(first.TransactionId, key));

        long twoPhase = cluster.Calls(other, CommitCallPath.TwoPhase);
        long forwards = cluster.Managers[other].KeyValues.DurableTransportCounts.OnePhaseForwards;

        await Commit(cluster.Managers[other], second, ct);

        Assert.Equal(1, cluster.Calls(other, CommitCallPath.TwoPhase) - twoPhase);
        Assert.Equal(0, cluster.Managers[other].KeyValues.DurableTransportCounts.OnePhaseForwards - forwards);

        release();
        await WaitUntilAsync(() => cluster.FreeEverywhere(key), timeoutMs: 30_000);
        Assert.Equal(("2", 1L), await ReadLatest(cluster.Managers[leader], key, ct));
    }

    /// <summary>
    /// The first transaction wrote keys on two partitions, and its record is not on the partition of the key that
    /// the second one writes. The leader of that key settles the holder in its bundle only when it leads the record
    /// too; otherwise its record store is a replica's, and the two-phase flow runs.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PredecessorWithItsRecordOnAnotherPartition_IsCarriedOnlyWhenThisNodeLeadsTheRecord(bool leadsTheRecord)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Cluster cluster = await Assemble();

        string anchorKey = KeyOn(cluster.Managers[0], 2, "anchor");
        string key = KeyOn(cluster.Managers[0], 1, "other");
        int leader = await cluster.LeaderOf(1, ct);
        await cluster.MoveLeadership(2, leadsTheRecord ? leader : (leader + 1) % 3, ct);
        KahunaManager node = cluster.Managers[leader];

        TransactionHandle first = default;
        Action release = await cluster.HoldResolutions(async () =>
        {
            first = await Stage(node, key, "1", ct, anchorKey, key);
            await Commit(node, first, ct);

            // The record is on every replica too, so a replica's record store answers a commit for it.
            await WaitUntilAsync(() => cluster.HeldEverywhereBy(first.TransactionId, key) && cluster.HeldEverywhereBy(first.TransactionId, anchorKey));
            await WaitUntilAsync(() => node.DurableTransactionRecordStore.Get(first.TransactionId, 1) is { Decision: TransactionDecision.Commit });
        });

        Assert.Equal(2, node.KeyValues.LocateDurablePartition(node.DurablePreparedIntentStore.Get(key)!.RecordAnchorKey).PartitionId);

        TransactionHandle second = await Stage(node, key, "2", ct, key);
        Assert.Equal(leader, cluster.OwnerOf(second));
        long onePhase = cluster.Calls(leader, CommitCallPath.OnePhase);
        long twoPhase = cluster.Calls(leader, CommitCallPath.TwoPhase);

        await Commit(node, second, ct);

        Assert.Equal(leadsTheRecord ? 1 : 0, cluster.Calls(leader, CommitCallPath.OnePhase) - onePhase);
        Assert.Equal(leadsTheRecord ? 0 : 1, cluster.Calls(leader, CommitCallPath.TwoPhase) - twoPhase);

        release();
        await WaitUntilAsync(() => cluster.FreeEverywhere(key) && cluster.FreeEverywhere(anchorKey), timeoutMs: 30_000);
        Assert.Equal(("2", 1L), await ReadLatest(cluster.Managers[(leader + 1) % 3], key, ct));
        Assert.Equal(("1", 0L), await ReadLatest(cluster.Managers[(leader + 1) % 3], anchorKey, ct));
    }
}
