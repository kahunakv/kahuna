using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A durable transaction whose keys share one partition but span several key spaces — a key-range space beside a
/// hash-routed one, or two key-range spaces at different generations — must commit. Its prepare is re-fenced at
/// dispatch against every space it touches, each key at its own generation; fencing the first key against the
/// last key's generation read as a moved range on every round and refused every such commit. The key order the
/// freeze meets comes from a hash set, so each test commits several key pairs: one order cannot hide the defect.
/// </summary>
public sealed class TestMixedKeySpacePrepareFence
{
    private const string RangeSpace = "mixfence:r";

    private const string SecondRangeSpace = "mixfence:i";

    // Hash routing places a whole key space on one partition, so the hash space beside the range space is chosen
    // per test: the first candidate whose keys route to the range space's partition.
    private const string HashSpacePrefix = "mixfence-meta";

    private const int Partitions = 4;

    private const int Pairs = 8;

    private readonly ILoggerFactory loggerFactory;

    public TestMixedKeySpacePrepareFence(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = Partitions,
            DurableDeferredSettlement = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync($"{HashSpacePrefix}/probe", ct);
        return node;
    }

    private static KeyValuesManager KeyValues(EmbeddedKahunaNode node) => ((KahunaManager)node.Kahuna).KeyValues;

    /// <summary><paramref name="count"/> keys of <paramref name="space"/>; every one must route to <paramref name="partition"/>.</summary>
    private static List<string> KeysOn(EmbeddedKahunaNode node, string space, int partition, int count)
    {
        List<string> keys = new(count);
        for (int i = 0; i < count; i++)
        {
            string key = $"{space}/k{i:D4}";
            Assert.Equal(partition, KeyValues(node).LocateDurablePartition(key).PartitionId);
            keys.Add(key);
        }

        return keys;
    }

    /// <summary>A hash-routed key space whose keys route to <paramref name="partition"/> (generation zero).</summary>
    private static string HashSpaceOn(EmbeddedKahunaNode node, int partition)
    {
        for (int candidate = 0; candidate < 1_000; candidate++)
        {
            string space = $"{HashSpacePrefix}{candidate}";
            DurableKeyRoute route = KeyValues(node).LocateDurablePartition($"{space}/k0000");
            if (route.PartitionId == partition)
            {
                Assert.Equal(0L, route.Generation);
                return space;
            }
        }

        throw new InvalidOperationException($"no hash space routes to partition {partition}");
    }

    private static async Task<TransactionHandle> StartSession(EmbeddedKahunaNode node, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 10_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static async Task WriteInSession(EmbeddedKahunaNode node, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task AssertCommitted(EmbeddedKahunaNode node, string key, string expected, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.NotNull(entry);
        Assert.Equal(expected, Encoding.UTF8.GetString(entry!.Value!));
    }

    /// <summary>Commits <paramref name="pairs"/> transactions, each writing one key of every list at index i, and
    /// asserts every write is visible afterwards and no prepare was released as a stale fence.</summary>
    private static async Task CommitPairs(EmbeddedKahunaNode node, string coordinatorSpace, IReadOnlyList<IReadOnlyList<string>> keyLists, int pairs, CancellationToken ct)
    {
        using MetricCapture rejections = new("reason", "kahuna.kv.write.rejections");

        for (int i = 0; i < pairs; i++)
        {
            TransactionHandle tx = await StartSession(node, $"{coordinatorSpace}/tx-{i}", ct);

            foreach (IReadOnlyList<string> keys in keyLists)
                await WriteInSession(node, tx, keys[i], $"v{i}", ct);

            (KeyValueResponseType committed, _) = await node.Kahuna.LocateAndCommitTransaction(tx, ct);
            Assert.Equal(KeyValueResponseType.Committed, committed);
        }

        for (int i = 0; i < pairs; i++)
            foreach (IReadOnlyList<string> keys in keyLists)
                await AssertCommitted(node, keys[i], $"v{i}", ct);

        // The fence is only ever a release of a prepare whose descriptor moved; nothing moved here.
        Assert.Equal(0, rejections.Total("kahuna.kv.write.rejections", "fence_stale"));
    }

    /// <summary>
    /// A key-range key (generation 1) and a hash-routed key (generation 0) on the same partition: the shape of a
    /// row write beside a schema-log write. Every transaction commits whatever order the freeze meets the keys in.
    /// </summary>
    [Fact]
    public async Task KeyRangeAndHashKeyOnOnePartition_Commit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        Assert.True(await node.Kahuna.RegisterKeyRangeAsync(RangeSpace, ct));
        await node.WaitForLeaderForKeyAsync($"{RangeSpace}/probe", ct);
        int partition = KeyValues(node).LocateDurablePartition($"{RangeSpace}/probe").PartitionId;
        Assert.Equal(1L, KeyValues(node).LocateDurablePartition($"{RangeSpace}/probe").Generation);

        List<string> rangeKeys = KeysOn(node, RangeSpace, partition, Pairs);
        List<string> hashKeys = KeysOn(node, HashSpaceOn(node, partition), partition, Pairs);

        await CommitPairs(node, "mixfence-tx", [rangeKeys, hashKeys], Pairs, ct);
    }

    /// <summary>
    /// The same mix spread over two partitions: the mixed partition is prepared through the two-phase prepare
    /// path (a non-anchor group, or the anchor bundle) rather than the one-phase bundle, and commits too.
    /// </summary>
    [Fact]
    public async Task KeyRangeAndHashKeyOnOnePartition_WithAThirdKeyElsewhere_Commit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        Assert.True(await node.Kahuna.RegisterKeyRangeAsync(RangeSpace, ct));
        await node.WaitForLeaderForKeyAsync($"{RangeSpace}/probe", ct);
        int partition = KeyValues(node).LocateDurablePartition($"{RangeSpace}/probe").PartitionId;
        int other = partition == 1 ? 2 : 1;
        string otherSpace = HashSpaceOn(node, other);
        await node.WaitForLeaderForKeyAsync($"{otherSpace}/probe", ct);

        List<string> rangeKeys = KeysOn(node, RangeSpace, partition, Pairs);
        List<string> hashKeys = KeysOn(node, HashSpaceOn(node, partition), partition, Pairs);
        List<string> elsewhere = KeysOn(node, otherSpace, other, Pairs);

        await CommitPairs(node, "mixfence-tx2", [rangeKeys, hashKeys, elsewhere], Pairs, ct);
    }

    /// <summary>
    /// Two key-range spaces on one partition at generations 1 and 2 — only one of them split so far. The group
    /// fences both spaces, each at its own generation, and commits.
    /// </summary>
    [Fact]
    public async Task TwoKeyRangeSpacesAtDifferentGenerations_Commit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        Assert.True(await node.Kahuna.RegisterKeyRangeAsync(RangeSpace, ct));
        await node.WaitForLeaderForKeyAsync($"{RangeSpace}/probe", ct);
        int partition = KeyValues(node).LocateDurablePartition($"{RangeSpace}/probe").PartitionId;

        // The second space's whole-space descriptor lands on the same partition at generation 2, as if its single
        // range had already split once and merged back.
        Assert.True(await manager.RangeMapStore.MutateAsync(current =>
        {
            RangeDescriptor[] next = new RangeDescriptor[current.Count + 1];
            for (int i = 0; i < current.Count; i++)
                next[i] = current[i];
            next[current.Count] = new RangeDescriptor { KeySpace = SecondRangeSpace, StartKey = null, EndKey = null, PartitionId = partition, Generation = 2 };
            return next;
        }, ct));
        manager.RegisterKeyRange(SecondRangeSpace);

        Assert.Equal((partition, 1L), (KeyValues(node).LocateDurablePartition($"{RangeSpace}/probe").PartitionId, KeyValues(node).LocateDurablePartition($"{RangeSpace}/probe").Generation));
        Assert.Equal((partition, 2L), (KeyValues(node).LocateDurablePartition($"{SecondRangeSpace}/probe").PartitionId, KeyValues(node).LocateDurablePartition($"{SecondRangeSpace}/probe").Generation));

        List<string> first = KeysOn(node, RangeSpace, partition, Pairs);
        List<string> second = KeysOn(node, SecondRangeSpace, partition, Pairs);

        await CommitPairs(node, "mixfence-tx3", [first, second], Pairs, ct);
    }

    /// <summary>
    /// A key space registered while writers route into it: a writer meets the space either hash-routed (before the
    /// seed is visible) or key-range routed with its descriptor present (after), never key-range routed with no
    /// descriptor. Every write answers Set or a retryable refusal, and no exception escapes to the caller.
    /// </summary>
    [Fact]
    public async Task RegisterKeyRange_WhileWritersRouteIntoTheSpace_NeverEscapesAnException()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        const string space = "mixfence:live";
        const int writers = 4;

        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        List<Exception> escaped = [];
        HashSet<KeyValueResponseType> answers = [];
        object gate = new();

        Task[] loops = new Task[writers];
        for (int w = 0; w < writers; w++)
        {
            int writer = w;
            loops[w] = Task.Run(async () =>
            {
                for (int i = 0; !stop.IsCancellationRequested; i++)
                {
                    try
                    {
                        (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                            HLCTimestamp.Zero, $"{space}/w{writer}-{i}", [1], null, -1,
                            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                        lock (gate)
                            answers.Add(type);
                    }
                    catch (OperationCanceledException) when (stop.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        lock (gate)
                            escaped.Add(ex);
                    }
                }
            }, ct);
        }

        // Let the writers run for a moment on the hash route, flip the space under them, then let them run on.
        await Task.Delay(150, ct);
        Assert.True(await node.Kahuna.RegisterKeyRangeAsync(space, ct));
        await Task.Delay(150, ct);
        stop.Cancel();
        await Task.WhenAll(loops);

        Assert.Empty(escaped);
        Assert.Contains(KeyValueResponseType.Set, answers);
        Assert.All(answers, type => Assert.True(type is KeyValueResponseType.Set or KeyValueResponseType.MustRetry, $"unexpected answer {type}"));
    }
}
