using System.Text;
using Kahuna;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers the grouped commit-time write-intent probe: the concurrent-writer check an optimistic commit runs over
/// its read set, issued as one call per node owning part of the set instead of one call per key.
///
/// Two properties are tested separately because they fail in different ways. The semantics — which keys are
/// flagged as having a concurrent writer, and that every requested key comes back — are exercised on a single
/// node where the answer is deterministic. The routing property, that the number of inter-node calls follows the
/// number of owning nodes rather than the number of keys, is exercised on a real three-node cluster against the
/// transport's own call counters.
/// </summary>
public sealed class TestManyWriteIntentProbe : BaseCluster
{
    private readonly ITestOutputHelper output;
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestManyWriteIntentProbe(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>The transaction that owns every injected intent below.</summary>
    private static readonly HLCTimestamp IntentOwner = new(0, 100, 0);

    private static PreparedIntent UndecidedIntent(string key) => new(
        TransactionId: IntentOwner, Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(0, 200, 0),
        State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes("P"), Bucket: null, Revision: 2,
        Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: 1, BaseState: KeyValueState.Set,
        // Far-future recovery deadline so the periodic sweep never resolves it mid-test.
        RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending);

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("probe/seed", ct);

        return node;
    }

    /// <summary>
    /// The probe answers every key it was asked about, flags exactly the key carrying a live foreign writer, and
    /// does not flag the intent's own transaction. Answering every key is part of the contract: a caller that
    /// silently lost a key would drop that key's write-skew guard.
    /// </summary>
    [Fact]
    public async Task Probe_AnswersEveryKey_AndFlagsOnlyForeignWriters()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        store.Apply(new PrepareIntentCommand(UndecidedIntent("probe/many/conflicted")));

        List<(string key, KeyValueDurability durability)> keys =
        [
            ("probe/many/clean-a", KeyValueDurability.Persistent),
            ("probe/many/conflicted", KeyValueDurability.Persistent),
            ("probe/many/clean-b", KeyValueDurability.Persistent),
            ("probe/many/clean-c", KeyValueDurability.Persistent)
        ];

        HLCTimestamp otherTransaction = new(0, 500, 0);

        Dictionary<string, KeyValueResponseType> byKey = await ProbeByKey(node.Kahuna, otherTransaction, keys, ct);

        Assert.Equal(keys.Count, byKey.Count);
        Assert.Equal(KeyValueResponseType.Aborted, byKey["probe/many/conflicted"]);
        Assert.Equal(KeyValueResponseType.DoesNotExist, byKey["probe/many/clean-a"]);
        Assert.Equal(KeyValueResponseType.DoesNotExist, byKey["probe/many/clean-b"]);
        Assert.Equal(KeyValueResponseType.DoesNotExist, byKey["probe/many/clean-c"]);

        // The intent's own transaction is not a foreign writer, so the same key is clean for it.
        Dictionary<string, KeyValueResponseType> byKeyForOwner = await ProbeByKey(node.Kahuna, IntentOwner, keys, ct);

        Assert.Equal(keys.Count, byKeyForOwner.Count);
        Assert.All(byKeyForOwner.Values, type => Assert.Equal(KeyValueResponseType.DoesNotExist, type));
    }

    /// <summary>
    /// A malformed key is reported against itself and does not cancel the probe for the rest of the set. The
    /// opposite behaviour — bailing out on the whole request — would silently drop the write-skew guard for
    /// every other read dependency in the transaction.
    /// </summary>
    [Fact]
    public async Task Probe_MalformedKey_DoesNotSuppressTheRestOfTheSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        store.Apply(new PrepareIntentCommand(UndecidedIntent("probe/malformed/conflicted")));

        List<(string key, KeyValueDurability durability)> keys =
        [
            ("", KeyValueDurability.Persistent),
            ("probe/malformed/conflicted", KeyValueDurability.Persistent),
            ("probe/malformed/clean", KeyValueDurability.Persistent)
        ];

        Dictionary<string, KeyValueResponseType> byKey =
            await ProbeByKey(node.Kahuna, new HLCTimestamp(0, 500, 0), keys, ct);

        Assert.Equal(3, byKey.Count);
        Assert.Equal(KeyValueResponseType.InvalidInput, byKey[""]);
        Assert.Equal(KeyValueResponseType.Aborted, byKey["probe/malformed/conflicted"]);
        Assert.Equal(KeyValueResponseType.DoesNotExist, byKey["probe/malformed/clean"]);
    }

    /// <summary>An empty read set costs nothing and asks nobody.</summary>
    [Fact]
    public async Task Probe_EmptyKeySet_ReturnsNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        Assert.Empty(await node.Kahuna.LocateAndTryCheckManyWriteIntents(new HLCTimestamp(0, 500, 0), [], ct));
    }

    /// <summary>
    /// On a cluster the grouped probe issues at most one call per remote node, however many keys that node owns,
    /// while the per-key probe issues one per remotely owned key. Both cover the same key set. This is the
    /// property the grouping exists for, and it holds regardless of machine speed.
    /// </summary>
    [Fact]
    public async Task Probe_AcrossCluster_CostsOneCallPerRemoteNode()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3,
         IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3,
         MemoryInterNodeCommmunication transport) =
            await AssembleThreNodeClusterWithTransport("memory", 8, raftLogger, kahunaLogger);

        try
        {
            const int keyCount = 60;
            string prefix = "probe/cluster/" + Guid.NewGuid().ToString("N")[..8];
            List<(string key, KeyValueDurability durability)> keys = new(keyCount);

            for (int i = 0; i < keyCount; i++)
                keys.Add(($"{prefix}/{i}", KeyValueDurability.Persistent));

            HLCTimestamp probeTransaction = new(0, 500, 0);

            // Which node wins each partition's election varies between runs, and in these clusters one node
            // tends to win them all. Probing from a node that leads the keys would measure the local path and
            // report zero remote calls for both shapes — a vacuous pass. Pick a caller that does not lead the
            // partitions holding this key set, so the comparison is about routing rather than about luck.
            IKahuna caller = await SelectLeastOwningNode(
                [(raft1, kahuna1), (raft2, kahuna2), (raft3, kahuna3)], partitionCount: 8, ct);

            // Warm the routing tables so leader resolution is not part of what the counters below observe.
            await caller.LocateAndTryCheckManyWriteIntents(probeTransaction, keys, ct);

            int perKeyBefore = transport.CheckWriteIntentCallCount;
            foreach ((string key, KeyValueDurability durability) in keys)
                await caller.LocateAndTryCheckWriteIntent(probeTransaction, key, durability, ct);
            int perKeyRemoteCalls = transport.CheckWriteIntentCallCount - perKeyBefore;

            int groupedBefore = transport.CheckManyWriteIntentsCallCount;
            List<(KeyValueResponseType type, string key, KeyValueDurability durability)> grouped =
                await caller.LocateAndTryCheckManyWriteIntents(probeTransaction, keys, ct);
            int groupedRemoteCalls = transport.CheckManyWriteIntentsCallCount - groupedBefore;

            // Same coverage: one answer per requested key, no duplicates, no omissions.
            Assert.Equal(keyCount, grouped.Count);
            Assert.Equal(keyCount, grouped.Select(item => item.key).Distinct(StringComparer.Ordinal).Count());
            Assert.All(grouped, item => Assert.Equal(KeyValueResponseType.DoesNotExist, item.type));

            // Two remote nodes exist, so the grouped probe can never exceed two calls no matter how many keys it
            // covers, while the per-key probe pays one per remotely owned key.
            output.WriteLine($"per-key remote calls={perKeyRemoteCalls}, grouped remote calls={groupedRemoteCalls}, keys={keyCount}");

            // The comparison below is only meaningful if the key set really is spread beyond the local node.
            // Assert that rather than let a run where one node happened to lead every partition pass vacuously.
            Assert.True(perKeyRemoteCalls > 2, $"expected remotely owned keys, per-key probe issued {perKeyRemoteCalls} remote calls");

            Assert.True(groupedRemoteCalls <= 2, $"grouped probe issued {groupedRemoteCalls} remote calls");
            Assert.True(groupedRemoteCalls > 0, "grouped probe reached no remote node for a spread key set");
            Assert.True(
                groupedRemoteCalls < perKeyRemoteCalls,
                $"grouped probe issued {groupedRemoteCalls} remote calls against {perKeyRemoteCalls} per-key");
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A concurrent writer on a key owned by another node is still detected — the grouped probe carries the
    /// conflict back across the transport, not just the clean answers.
    /// </summary>
    [Fact]
    public async Task Probe_AcrossCluster_DetectsConcurrentWriterOnRemotelyOwnedKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3,
         IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3,
         MemoryInterNodeCommmunication _) =
            await AssembleThreNodeClusterWithTransport("memory", 8, raftLogger, kahunaLogger);

        try
        {
            string prefix = "probe/remote-conflict/" + Guid.NewGuid().ToString("N")[..8];
            string conflicted = $"{prefix}/conflicted";

            // The key's owner is whichever node leads its partition. Injecting the intent on every node makes
            // the test independent of that placement: only the owner is ever asked about this key.
            foreach (IKahuna node in new[] { kahuna1, kahuna2, kahuna3 })
                ((KahunaManager)node).DurablePreparedIntentStore.Apply(new PrepareIntentCommand(UndecidedIntent(conflicted)));

            List<(string key, KeyValueDurability durability)> keys =
            [
                ($"{prefix}/clean-a", KeyValueDurability.Persistent),
                (conflicted, KeyValueDurability.Persistent),
                ($"{prefix}/clean-b", KeyValueDurability.Persistent)
            ];

            Dictionary<string, KeyValueResponseType> byKey =
                await ProbeByKey(kahuna1, new HLCTimestamp(0, 500, 0), keys, ct);

            Assert.Equal(3, byKey.Count);
            Assert.Equal(KeyValueResponseType.Aborted, byKey[conflicted]);
            Assert.Equal(KeyValueResponseType.DoesNotExist, byKey[$"{prefix}/clean-a"]);
            Assert.Equal(KeyValueResponseType.DoesNotExist, byKey[$"{prefix}/clean-b"]);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A real optimistic commit whose read set is large finds the one key in it that acquired a concurrent
    /// writer after the read, and aborts. This is the coordinator's own use of the grouped probe: it exercises
    /// correlating one flagged answer back to its key inside a set where every other answer is clean — the part
    /// a per-key fan-out got for free and a grouped probe has to get right.
    /// </summary>
    [Fact]
    public async Task OptimisticCommit_AbortsOnConcurrentWriterInsideALargeReadSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string prefix = "probe/commit-abort/" + Guid.NewGuid().ToString("N")[..8];
        (TransactionHandle handle, string conflictKey) = await ReadLargeSetAndWrite(node, prefix, ct);

        // The concurrent writer appears after the reads, exactly as write skew does: a foreign transaction
        // prepares a durable intent on one key this transaction read but did not modify.
        ((KahunaManager)node.Kahuna).DurablePreparedIntentStore.Apply(new PrepareIntentCommand(UndecidedIntent(conflictKey)));

        (KeyValueResponseType commitType, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.Aborted, commitType);
    }

    /// <summary>
    /// The same large read set with no concurrent writer commits. Without this, an abort-only test would pass
    /// just as well against a probe that flagged everything.
    /// </summary>
    [Fact]
    public async Task OptimisticCommit_CommitsWhenALargeReadSetIsClean()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string prefix = "probe/commit-clean/" + Guid.NewGuid().ToString("N")[..8];
        (TransactionHandle handle, string _) = await ReadLargeSetAndWrite(node, prefix, ct);

        (KeyValueResponseType commitType, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.Committed, commitType);
    }

    /// <summary>
    /// Seeds forty keys, opens an optimistic transaction that folds all of them as read dependencies through one
    /// registered batch read, and stages a write on a key outside that set (a modified key is validated as a
    /// write, not as a read dependency). Returns the open handle and the read key a caller may put a concurrent
    /// writer on.
    /// </summary>
    private static async Task<(TransactionHandle Handle, string ConflictKey)> ReadLargeSetAndWrite(
        EmbeddedKahunaNode node,
        string prefix,
        CancellationToken cancellationToken)
    {
        const int readSetSize = 40;
        List<(string key, long revision, KeyValueDurability durability)> readKeys = new(readSetSize);
        List<KahunaSetKeyValueRequestItem> seed = [];

        for (int i = 0; i < readSetSize; i++)
        {
            string key = $"{prefix}/read/{i}";
            readKeys.Add((key, -1, KeyValueDurability.Persistent));
            seed.Add(new()
            {
                TransactionId = HLCTimestamp.Zero,
                Key = key,
                Value = "seed"u8.ToArray(),
                ExpiresMs = 0,
                Flags = KeyValueFlags.None,
                Durability = KeyValueDurability.Persistent
            });
        }

        await SeedWithRetry(node.Kahuna, seed, cancellationToken);

        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = $"{prefix}/tx",
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 60_000
            }, cancellationToken);

        Assert.Equal(KeyValueResponseType.Set, startType);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> reads =
            await node.Kahuna.LocateAndTryGetManyValues(
                handle.TransactionId, HLCTimestamp.Zero, readKeys, cancellationToken,
                handle.CoordinatorKey, TransactionOperationId.NewRandom());

        Assert.All(reads, read => Assert.Equal(KeyValueResponseType.Get, read.type));

        await SeedWithRetry(node.Kahuna,
        [
            new()
            {
                TransactionId = handle.TransactionId,
                Key = $"{prefix}/written",
                Value = "v"u8.ToArray(),
                ExpiresMs = 0,
                Flags = KeyValueFlags.None,
                Durability = KeyValueDurability.Persistent
            }
        ], cancellationToken, handle.CoordinatorKey);

        // A key in the middle of the set, so a conflict is neither the first nor the last answer correlated.
        return (handle, $"{prefix}/read/{readSetSize / 2}");
    }

    /// <summary>
    /// Returns the node leading the fewest partitions, so a probe issued from it crosses the network for as much
    /// of the key set as this cluster allows.
    /// </summary>
    private static async Task<IKahuna> SelectLeastOwningNode(
        (IRaft Raft, IKahuna Kahuna)[] nodes,
        int partitionCount,
        CancellationToken cancellationToken)
    {
        Dictionary<string, int> ledPartitions = new(StringComparer.Ordinal);

        for (int partition = 0; partition < partitionCount; partition++)
        {
            string leader = await nodes[0].Raft.WaitForLeader(partition, cancellationToken);
            ledPartitions[leader] = ledPartitions.GetValueOrDefault(leader) + 1;
        }

        return nodes
            .OrderBy(node => ledPartitions.GetValueOrDefault(node.Raft.GetLocalEndpoint()))
            .First()
            .Kahuna;
    }

    private static async Task<Dictionary<string, KeyValueResponseType>> ProbeByKey(
        IKahuna kahuna,
        HLCTimestamp transactionId,
        List<(string key, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken)
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results =
            await kahuna.LocateAndTryCheckManyWriteIntents(transactionId, keys, cancellationToken);

        Dictionary<string, KeyValueResponseType> byKey = new(results.Count, StringComparer.Ordinal);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability _) in results)
            byKey[key] = type;

        return byKey;
    }

    /// <summary>
    /// Writes the batch, retrying the retryable MustRetry that a freshly assembled cluster can return while
    /// partition leadership settles. A MustRetry attempt had no durable effect, so re-issuing it is what a real
    /// client does.
    /// </summary>
    private static async Task SeedWithRetry(
        IKahuna kahuna,
        List<KahunaSetKeyValueRequestItem> items,
        CancellationToken cancellationToken,
        string coordinatorKey = "")
    {
        List<KahunaSetKeyValueResponseItem> written = await Write();

        for (int attempt = 0; attempt < 20 && written.Any(item => item.Type != KeyValueResponseType.Set); attempt++)
        {
            await Task.Delay(25, cancellationToken);
            written = await Write();
        }

        Assert.All(written, item => Assert.Equal(KeyValueResponseType.Set, item.Type));

        Task<List<KahunaSetKeyValueResponseItem>> Write() => string.IsNullOrEmpty(coordinatorKey)
            ? kahuna.LocateAndTrySetManyKeyValue(items, cancellationToken)
            : kahuna.LocateAndTrySetManyKeyValue(items, cancellationToken, coordinatorKey, TransactionOperationId.NewRandom());
    }
}
