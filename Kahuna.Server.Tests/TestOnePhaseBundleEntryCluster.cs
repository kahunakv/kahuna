using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// One-phase commits that go to the log as one bundle entry, on real nodes: one node, a restart of it, every replica of
/// a three-node group, the peer guard that keeps a leader at three entries while a replica did not confirm the type,
/// and a group whose nodes have different settings. Each test reads the shapes from the log itself.
/// </summary>
public sealed class TestOnePhaseBundleEntryCluster : BaseCluster, IDisposable
{
    private readonly ILoggerFactory loggerFactory;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly string tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_bundleentry_" + Guid.NewGuid().ToString("N"));

    public TestOnePhaseBundleEntryCluster(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(tempRoot))
                Directory.Delete(tempRoot, recursive: true);
        }
        catch (IOException)
        {
            // A file that a node still holds open; the system cleans its temp directory.
        }
    }

    private static byte[] Script(string prefix, int transaction) =>
        Encoding.UTF8.GetBytes($"BEGIN SET `{prefix}/{transaction}` 'value-{transaction}' COMMIT END");

    /// <summary>The shapes of the one-phase commits in the logs of <paramref name="raft"/>, for the keys under
    /// <paramref name="prefix"/>: commits that are one bundle entry, and commits whose prepare is an entry of its own.</summary>
    private static (int Bundles, int SeparatePrepares) ScanShapes(IRaft raft, string prefix)
    {
        int bundles = 0, separate = 0;

        foreach (RaftPartitionRange partition in raft.GetPartitionMap())
        {
            foreach (RaftLog log in raft.WalAdapter.ReadLogsRange(partition.PartitionId, 1))
            {
                if (log.LogData is not { Length: > 0 })
                    continue;

                bool ours(PreparedIntentCommand[] commands) =>
                    commands.OfType<PrepareIntentCommand>().Any(p => p.Intent.Key.StartsWith(prefix, StringComparison.Ordinal));

                if (log.LogType == ReplicationTypes.OnePhaseBundle && ours(OnePhaseBundleEntry.PrepareCommandsOf(log.LogData)))
                    bundles++;
                else if (log.LogType == ReplicationTypes.PreparedIntent && ours(PreparedIntentStore.DecodeDelta(log.LogData)))
                    separate++;
            }
        }

        return (bundles, separate);
    }

    private static async Task CommitAsync(IKahuna kahuna, string prefix, int from, int count)
    {
        for (int i = from; i < from + count; i++)
        {
            KeyValueTransactionResult result = await RetryOnMustRetryAsync(() => kahuna.TryExecuteTransactionScript(Script(prefix, i), null, null), r => r.Type);
            Assert.Equal(KeyValueResponseType.Set, result.Type);
        }
    }

    private static async Task AssertReadsBackAsync(IKahuna kahuna, string prefix, int count, CancellationToken ct)
    {
        for (int i = 0; i < count; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, $"{prefix}/{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal($"value-{i}", Encoding.UTF8.GetString(entry!.Value!));
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string what, CancellationToken ct, int timeoutMs = 20_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (!condition())
        {
            if (Environment.TickCount64 > deadline)
                Assert.Fail("Timed out waiting until " + what);

            await Task.Delay(20, ct);
        }
    }

    private static async Task AssertEveryReplicaHoldsAsync(KahunaManager[] managers, string prefix, int count, CancellationToken ct)
    {
        for (int r = 0; r < managers.Length; r++)
        {
            KahunaManager manager = managers[r];
            await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Count == 0, $"replica {r} settles every intent", ct);

            for (int i = 0; i < count; i++)
            {
                string key = $"{prefix}/{i}";
                byte[] expected = Encoding.UTF8.GetBytes($"value-{i}");
                await WaitUntilAsync(
                    () => manager.PersistenceBackend.GetKeyValue(key) is { } entry && entry.Value is not null && entry.Value.AsSpan().SequenceEqual(expected),
                    $"replica {r} holds {key}", ct);
            }
        }
    }

    private static OnePhaseBundlePeerGuard GuardOf(IKahuna kahuna) => ((KahunaManager)kahuna).KeyValues.BundlePeerGuard!;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OneNode_EachOnePhaseCommitIsOneEntry_AndItsValueServes(bool materializeOnResolve)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int commits = 12;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            DurableMaterializeOnResolve = materializeOnResolve,
            DurableOnePhaseBundleEntry = true
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("one/0", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        KeyValueApplyTotals totals = manager.KeyValues.ReplicationDispatcher.ApplyTotals;
        long bundlesBefore = totals.Entries(KeyValueApplyEntryType.Bundle);

        await CommitAsync(node.Kahuna, "one", 0, commits);
        await AssertReadsBackAsync(node.Kahuna, "one", commits, ct);
        await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Count == 0, "every intent settled", ct);

        Assert.Equal((commits, 0), ScanShapes(node.Raft, "one/"));
        Assert.Equal(commits, totals.Entries(KeyValueApplyEntryType.Bundle) - bundlesBefore);

        // A node with no other replica asks nobody.
        Assert.Equal(0, GuardOf(node.Kahuna).Asks);
        Assert.Equal(0, GuardOf(node.Kahuna).Fallbacks);

        await AssertReadsBackAsync(node.Kahuna, "one", commits, ct);
    }

    private static byte[] BundleFor(HLCTimestamp txId, string key)
    {
        List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(txId, 1, key, new HLCTimestamp(0, txId.L + 1, 0), manifest);
        HLCTimestamp opId = new(0, txId.L + 2, 0);

        PreparedIntent intent = new(txId, 1, key, hash, key, new HLCTimestamp(0, txId.L + 1, 0), KeyValueState.Set, Encoding.UTF8.GetBytes("direct"),
            null, 1, HLCTimestamp.Zero, false, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, new HLCTimestamp(0, txId.L + 900_000, 0),
            PreparedIntentResolution.Pending);

        return OnePhaseBundleEntry.Serialize(
            TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(txId, 1, "coord", key, new HLCTimestamp(0, txId.L + 1, 0), new HLCTimestamp(0, txId.L + 900_000, 0), hash, manifest, opId, txId)]),
            PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(intent)]),
            TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(txId, 1, hash, opId, opId, BundledPrepareKeys: [key])]));
    }

    /// <summary>
    /// The dispatcher of a node, given bundle entries directly: a live apply records the prepare result of the entry in
    /// the ledger that the leader's completion waits on, and a restore applies the entry as a replay, which memos no
    /// refusal of its prepare.
    /// </summary>
    [Fact]
    public async Task TheDispatcher_AppliesALiveBundleWithItsResult_AndARestoredBundleAsAReplay()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("direct/a", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        int partition = manager.KeyValues.LocateDurablePartition("direct/a").PartitionId;
        KeyValueReplicationDispatcher dispatcher = manager.KeyValues.ReplicationDispatcher;
        HLCTimestamp first = new(9, 5_000_000, 0), second = new(9, 6_000_000, 0);

        RaftLog live = new() { Id = 1_000_000, Term = node.Raft.GetPartitionTerm(partition), Type = RaftLogType.Committed, Time = new HLCTimestamp(9, 5_000_010, 0), LogType = ReplicationTypes.OnePhaseBundle, LogData = [.. BundleFor(first, "direct/a")] };
        Assert.True(await dispatcher.OnReplicationReceived(partition, live));

        Assert.Equal(TransactionDecision.Commit, manager.DurableTransactionRecordStore.Get(first, 1)!.Decision);
        Assert.NotNull(manager.DurablePreparedIntentStore.GetByIdentity(first, 1, "direct/a"));

        DurableApplyWaitOutcome outcome = await manager.KeyValues.DurableApplyResults.WaitAppliedAsync(partition, live.Id, TimeSpan.FromSeconds(1), ct);
        Assert.Equal(DurableApplyWaitStatus.Recorded, outcome.Status);
        Assert.True(outcome.Result);

        // The first transaction holds the key; a restored bundle of a second one is refused as a replay refuses.
        RaftLog restored = new() { Id = 1_000_001, Term = live.Term, Type = RaftLogType.Committed, Time = new HLCTimestamp(9, 6_000_010, 0), LogType = ReplicationTypes.OnePhaseBundle, LogData = [.. BundleFor(second, "direct/a")] };
        Assert.True(await dispatcher.OnLogRestored(partition, restored));

        Assert.Equal(TransactionDecision.Undecided, manager.DurableTransactionRecordStore.Get(second, 1)!.Decision);
        Assert.False(manager.DurablePreparedIntentStore.TryTakePrepareRejection(second, 1, "direct/a", out _));
    }

    [Fact]
    public async Task OneNode_BundleEntries_ReplayAfterARestart()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int commits = 10;

        string storagePath = Path.Combine(tempRoot, "store");
        string walPath = Path.Combine(tempRoot, "wal");
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        EmbeddedKahunaOptions Options() => new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            InitialPartitions = 1,
            Storage = "sqlite",
            StoragePath = storagePath,
            StorageRevision = "bundle",
            WalStorage = "sqlite",
            WalPath = walPath,
            WalRevision = "bundle-wal",
            DurableMaterializeOnResolve = true,
            DurableOnePhaseBundleEntry = true
        };

        List<HLCTimestamp> transactions = [];

        await using (EmbeddedKahunaNode node = new(Options(), loggerFactory))
        {
            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("restart/0", ct);

            await CommitAsync(node.Kahuna, "restart", 0, commits);

            KahunaManager manager = (KahunaManager)node.Kahuna;
            await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Count == 0, "every intent settled", ct);
            Assert.Equal((commits, 0), ScanShapes(node.Raft, "restart/"));

            foreach (RaftPartitionRange partition in node.Raft.GetPartitionMap())
                foreach (RaftLog log in node.Raft.WalAdapter.ReadLogsRange(partition.PartitionId, 1))
                    if (log.LogType == ReplicationTypes.OnePhaseBundle)
                        transactions.Add(((PrepareIntentCommand)OnePhaseBundleEntry.PrepareCommandsOf(log.LogData!)[0]).Intent.TransactionId);
        }

        await using (EmbeddedKahunaNode restarted = new(Options(), loggerFactory))
        {
            await restarted.StartAsync(ct);
            await restarted.WaitForLeaderForKeyAsync("restart/0", ct);

            await AssertReadsBackAsync(restarted.Kahuna, "restart", commits, ct);

            // The records of the replayed entries are decided as they were before the restart.
            KahunaManager manager = (KahunaManager)restarted.Kahuna;
            foreach (HLCTimestamp transaction in transactions)
            {
                TransactionRecord? record = manager.DurableTransactionRecordStore.Get(transaction, 1);
                if (record is not null)
                    Assert.Equal(TransactionDecision.Commit, record.Decision);
            }
        }
    }

    [Fact]
    public async Task Cluster_EveryReplicaAppliesTheBundleEntries()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int commits = 16;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", 1, raftLogger, kahunaLogger, c =>
            {
                c.DurableMaterializeOnResolve = true;
                c.DurableOnePhaseBundleEntry = true;

                // A script's write carries its base, which only the apply-time check keeps on the one-phase path.
                c.OnePhaseApplyTimeValidation = true;
            });

        try
        {
            IRaft[] rafts = [raft1, raft2, raft3];
            KahunaManager[] managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];

            // The leader asks its replicas when it takes the leadership; the commits wait for the confirmation.
            int leader = await LeaderOfAsync(rafts, ct);
            await WaitUntilAsync(() => GuardOf(managers[leader]).ShouldProposeBundle(1, rafts[leader].GetPartitionTerm(1)),
                "the leader of the partition confirms its replicas", ct);

            await CommitAsync(kahuna1, "cluster", 0, commits);
            await AssertReadsBackAsync(kahuna2, "cluster", commits, ct);
            await AssertEveryReplicaHoldsAsync(managers, "cluster", commits, ct);

            for (int r = 0; r < rafts.Length; r++)
            {
                (int bundles, int separate) = ScanShapes(rafts[r], "cluster/");
                Assert.True(bundles == commits && separate == 0, $"replica {r}: {bundles} bundle entries and {separate} separate prepares");
                Assert.True(managers[r].KeyValues.ReplicationDispatcher.ApplyTotals.Entries(KeyValueApplyEntryType.Bundle) >= commits);
            }
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A replica that does not confirm the type keeps its leader at three entries, and the guard counts each such commit.
    /// Once the replica confirms, the leader proposes bundle entries, and while the confirmation holds a commit makes no
    /// call for it. Then a leader change: the new leader proposes three entries until its own replicas confirmed.
    /// </summary>
    [Fact]
    public async Task PeerGuard_ThreeEntriesUntilEveryReplicaConfirms_AndAgainAfterALeaderChange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", 1, raftLogger, kahunaLogger, c =>
            {
                c.DurableMaterializeOnResolve = true;
                c.DurableOnePhaseBundleEntry = true;

                // A script's write carries its base, which only the apply-time check keeps on the one-phase path.
                c.OnePhaseApplyTimeValidation = true;
            });

        try
        {
            IRaft[] rafts = [raft1, raft2, raft3];
            KahunaManager[] managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];

            int leader = await LeaderOfAsync(rafts, ct);
            int refusing = (leader + 1) % 3;
            int third = (leader + 2) % 3;

            // One replica answers like a build that does not know the bundle type.
            managers[refusing].KeyValues.AppliedLogTypesOverrideForTesting = () => null;

            // Its next ask finds the refusal: the old confirmation, if any, holds only until the term or the replicas
            // change, so a leader change makes the guard ask again.
            Assert.True(await rafts[leader].TransferLeadershipAsync(1, raftEndpoint(third), ct) is RaftOperationStatus.Success or RaftOperationStatus.Pending);
            await WaitForLeadershipAsync(rafts[third], "the third node leads", ct);
            leader = third;
            refusing = Array.FindIndex(managers, m => m.KeyValues.AppliedLogTypesOverrideForTesting is not null);

            OnePhaseBundlePeerGuard guard = GuardOf(managers[leader]);
            long fallbacksBefore = guard.Fallbacks;

            await CommitAsync(managers[leader], "guard-off", 0, 6);
            (int bundles, int separate) = ScanShapes(rafts[leader], "guard-off/");
            Assert.Equal((0, 6), (bundles, separate));
            Assert.True(guard.Fallbacks - fallbacksBefore >= 6, "each commit of a refused partition counts as a fallback");

            // The replica confirms. The leader asks again within the retry interval, and the commits become one entry each.
            managers[refusing].KeyValues.AppliedLogTypesOverrideForTesting = null;
            await WaitUntilAsync(() => guard.ShouldProposeBundle(1, rafts[leader].GetPartitionTerm(1)), "the leader confirms its replicas", ct);

            long asksBefore = guard.Asks;
            await CommitAsync(managers[leader], "guard-on", 0, 6);
            Assert.Equal((6, 0), ScanShapes(rafts[leader], "guard-on/"));
            Assert.Equal(asksBefore, guard.Asks);

            // A leader change with a replica that refuses again: the new leader starts at three entries.
            managers[refusing].KeyValues.AppliedLogTypesOverrideForTesting = () => [ReplicationTypes.KeyValues];
            int next = Enumerable.Range(0, 3).First(i => i != leader && i != refusing);
            Assert.True(await rafts[leader].TransferLeadershipAsync(1, raftEndpoint(next), ct) is RaftOperationStatus.Success or RaftOperationStatus.Pending);
            await WaitForLeadershipAsync(rafts[next], "the next node leads", ct);

            await CommitAsync(managers[next], "guard-again", 0, 4);
            Assert.Equal((0, 4), ScanShapes(rafts[next], "guard-again/"));

            await AssertEveryReplicaHoldsAsync(managers, "guard-off", 6, ct);
            await AssertEveryReplicaHoldsAsync(managers, "guard-on", 6, ct);
            await AssertEveryReplicaHoldsAsync(managers, "guard-again", 4, ct);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// The nodes of a group with different settings: what a leader proposes follows its own setting, and every node
    /// applies both shapes whatever its setting. The leadership then moves to a node with the other setting.
    /// </summary>
    [Fact]
    public async Task MixedFlags_TheShapeFollowsTheLeader_AndEveryNodeAppliesBoth()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int configured = 0;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", 1, raftLogger, kahunaLogger, c =>
            {
                c.DurableMaterializeOnResolve = true;
                c.OnePhaseApplyTimeValidation = true;

                // Only the first node proposes the bundle entry.
                c.DurableOnePhaseBundleEntry = Interlocked.Increment(ref configured) == 1;
            });

        try
        {
            IRaft[] rafts = [raft1, raft2, raft3];
            KahunaManager[] managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];

            // The node with the setting on leads first.
            int leader = await LeaderOfAsync(rafts, ct);
            if (leader != 0)
            {
                Assert.True(await rafts[leader].TransferLeadershipAsync(1, raftEndpoint(0), ct) is RaftOperationStatus.Success or RaftOperationStatus.Pending);
                await WaitForLeadershipAsync(rafts[0], "the first node leads", ct);
            }

            await WaitUntilAsync(() => GuardOf(managers[0]).ShouldProposeBundle(1, rafts[0].GetPartitionTerm(1)), "the first node confirms its replicas", ct);
            await CommitAsync(managers[0], "mixed-on", 0, 6);
            Assert.Equal((6, 0), ScanShapes(rafts[0], "mixed-on/"));

            // A node with the setting off takes the leadership and proposes three entries.
            Assert.True(await rafts[0].TransferLeadershipAsync(1, raftEndpoint(1), ct) is RaftOperationStatus.Success or RaftOperationStatus.Pending);
            await WaitForLeadershipAsync(rafts[1], "the second node leads", ct);

            await CommitAsync(managers[1], "mixed-off", 0, 6);
            Assert.Equal((0, 6), ScanShapes(rafts[1], "mixed-off/"));

            await AssertEveryReplicaHoldsAsync(managers, "mixed-on", 6, ct);
            await AssertEveryReplicaHoldsAsync(managers, "mixed-off", 6, ct);

            // Every node applied the bundle entries, also the two with the setting off.
            foreach (KahunaManager manager in managers)
                Assert.True(manager.KeyValues.ReplicationDispatcher.ApplyTotals.Entries(KeyValueApplyEntryType.Bundle) >= 6);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    private static string raftEndpoint(int index) => $"localhost:{8001 + index}";

    private static async Task WaitForLeadershipAsync(IRaft raft, string what, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 20_000;
        while (!await raft.AmILeaderIfHosted(1, ct))
        {
            if (Environment.TickCount64 > deadline)
                Assert.Fail("Timed out waiting until " + what);

            await Task.Delay(20, ct);
        }
    }

    private static async Task<int> LeaderOfAsync(IRaft[] rafts, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 20_000;
        while (Environment.TickCount64 < deadline)
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (await rafts[i].AmILeaderIfHosted(1, ct))
                    return i;
            }

            await Task.Delay(20, ct);
        }

        Assert.Fail("No leader of partition 1");
        return -1;
    }
}
