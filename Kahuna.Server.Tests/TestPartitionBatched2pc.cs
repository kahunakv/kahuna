using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for partition-batched two-phase commit: a multi-key transaction whose participants share a
/// Raft partition issues a single proposal/commit ticket for the whole group instead of one per key,
/// collapsing N fsyncs into one. This first slice covers the routing seam — grouping participants by
/// their resolved partition (and carrying the shared descriptor-fence generation) — which every later
/// batched step builds on.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestPartitionBatched2pc
{
    private readonly ILoggerFactory loggerFactory;

    public TestPartitionBatched2pc(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>
    /// Grouping resolves each key through the same <see cref="RangeRouting.Locate"/> the per-key path
    /// uses, so a set of keys drawn from several key spaces forms one group per resolved partition — each
    /// key landing in the group whose partition it actually routes to — with no drift from the routing the
    /// participants themselves observe.
    /// </summary>
    [Fact]
    public async Task GroupByPartition_BucketsKeysByResolvedPartition()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KeySpaceRegistry registry = new();
        DataPartitionRouter router = new(node.Raft);
        RangeMap rangeMap = new RangeMapStore(node.Raft, null, null, logger).Current;

        // Hash routing assigns a whole key space (the prefix before the last '/') to one partition, so
        // distinct key spaces are what spread work across the four partitions.
        List<string> keys = Enumerable.Range(0, 64).Select(i => $"space-{i}/row").ToList();

        Dictionary<int, (long Generation, List<string> Items)> groups =
            KeyValuesManager.GroupByPartition(registry, rangeMap, router, keys, k => k);

        // Every key is present exactly once, and each key sits in the group for the partition it routes to.
        Assert.Equal(keys.Count, groups.Values.Sum(g => g.Items.Count));
        Assert.True(groups.Count > 1, "64 keys across distinct key spaces should span more than one partition");

        foreach ((int partitionId, (long _, List<string> items)) in groups)
            foreach (string key in items)
                Assert.Equal(partitionId, RangeRouting.Locate(registry, rangeMap, router, key).PartitionId);
    }

    /// <summary>
    /// The batching payoff case: many keys inside one hash key space all resolve to the same partition, so
    /// they collapse into a single group — one proposal ticket for the whole write set instead of one per
    /// key. Grouping preserves the shared descriptor-fence generation for that partition.
    /// </summary>
    [Fact]
    public async Task GroupByPartition_KeysInOneKeySpace_CollapseToOneGroup()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KeySpaceRegistry registry = new();
        DataPartitionRouter router = new(node.Raft);
        RangeMap rangeMap = new RangeMapStore(node.Raft, null, null, logger).Current;

        List<string> keys = Enumerable.Range(0, 32).Select(i => $"orders/row-{i}").ToList();

        Dictionary<int, (long Generation, List<string> Items)> groups =
            KeyValuesManager.GroupByPartition(registry, rangeMap, router, keys, k => k);

        (long generation, List<string> items) = Assert.Single(groups.Values);
        Assert.Equal(keys.Count, items.Count);
        Assert.Equal(RangeRouting.Locate(registry, rangeMap, router, keys[0]).Generation, generation);
    }

    /// <summary>
    /// The batched prepare stage runs the same validation as a per-key prepare and pins the write intent,
    /// but instead of proposing it hands back the serialized committed proposal (and the resolving
    /// partition) for the manager to batch — settling no Raft ticket of its own. Proven by staging a
    /// persistent mutation against a joined node and asserting: Prepared with proposal bytes, no ticket, the
    /// intent pinned to the transaction, and the bytes decoding to a TrySet for the key.
    /// </summary>
    [Fact]
    public async Task StageExecute_PersistentPrepare_ReturnsProposalBytesAndPinsIntent_WithoutRaft()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.Raft.WaitForLeader(1, ct);

        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KeyValueContext context = DirectContext(node.Raft, logger);

        const string key = "orders/row-1";
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp commitId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        KeyValueEntry entry = new()
        {
            Value = "old"u8.ToArray(),
            Revision = 4,
            State = KeyValueState.Set,
            LastUsed = txId,
            LastModified = txId,
            MvccEntries = new() { [txId] = new KeyValueMvccEntry { Value = "new"u8.ToArray(), Revision = 5, State = KeyValueState.Set, LastModified = commitId } }
        };
        context.Store.Insert(key, entry);

        KeyValueResponse response = await new TryPrepareMutationsHandler(context)
            .StageExecute(StagePrepareRequest(txId, commitId, key, KeyValueDurability.Persistent));

        Assert.Equal(KeyValueResponseType.Prepared, response.Type);
        Assert.NotNull(response.StagedProposal);
        Assert.Equal(HLCTimestamp.Zero, response.Ticket);      // staging settles no Raft ticket
        Assert.Equal(1, response.StagedPartitionId);           // single-partition node
        Assert.NotNull(entry.WriteIntent);                     // intent pins the entry across the batch window
        Assert.Equal(txId, entry.WriteIntent!.TransactionId);

        KeyValueMessage kvm = ReplicationSerializer.UnserializeKeyValueMessage(response.StagedProposal!);
        Assert.Equal(key, kvm.Key);
        Assert.Equal((int)KeyValueRequestType.TrySet, kvm.Type);
    }

    /// <summary>
    /// A staged prepare that hits a conflicting write intent held by another transaction rejects with
    /// Errored and hands back no proposal — so the manager aborts the batch rather than replicating a
    /// conflicting write.
    /// </summary>
    [Fact]
    public async Task StageExecute_ConflictingIntent_RejectsWithNoProposal()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.Raft.WaitForLeader(1, ct);

        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KeyValueContext context = DirectContext(node.Raft, logger);

        const string key = "orders/row-2";
        HLCTimestamp otherTx = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp commitId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        context.Store.Insert(key, new KeyValueEntry
        {
            Value = "v"u8.ToArray(),
            Revision = 4,
            State = KeyValueState.Set,
            WriteIntent = new() { TransactionId = otherTx, Expires = otherTx + 15000 },
            MvccEntries = new() { [txId] = new KeyValueMvccEntry { Value = "new"u8.ToArray(), Revision = 5, State = KeyValueState.Set } }
        });

        KeyValueResponse response = await new TryPrepareMutationsHandler(context)
            .StageExecute(StagePrepareRequest(txId, commitId, key, KeyValueDurability.Persistent));

        Assert.Equal(KeyValueResponseType.Errored, response.Type);
        Assert.Null(response.StagedProposal);
    }

    /// <summary>
    /// An ephemeral prepare succeeds but has nothing to replicate: the stage returns Prepared with a null
    /// proposal so the manager counts the key as prepared while contributing nothing to any partition batch.
    /// </summary>
    [Fact]
    public async Task StageExecute_EphemeralPrepare_PreparedWithNoProposal()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.Raft.WaitForLeader(1, ct);

        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KeyValueContext context = DirectContext(node.Raft, logger);

        const string key = "cache/row-1";
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp commitId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        context.Store.Insert(key, new KeyValueEntry
        {
            Value = "v"u8.ToArray(),
            Revision = 4,
            State = KeyValueState.Set,
            LastUsed = txId,
            LastModified = txId,
            MvccEntries = new() { [txId] = new KeyValueMvccEntry { Value = "new"u8.ToArray(), Revision = 5, State = KeyValueState.Set } }
        });

        KeyValueResponse response = await new TryPrepareMutationsHandler(context)
            .StageExecute(StagePrepareRequest(txId, commitId, key, KeyValueDurability.Ephemeral));

        Assert.Equal(KeyValueResponseType.Prepared, response.Type);
        Assert.Null(response.StagedProposal);
    }

    /// <summary>
    /// End-to-end acceptance: a transaction writing many keys of one hash key space — all resolving to one
    /// partition — commits through the batched propose path and every key is durably readable afterward. This
    /// is the motivating bulk-write shape (a table's rows), where the batch collapses N proposes into one.
    /// </summary>
    [Fact]
    public async Task MultiKeyOneKeySpace_TransactionCommitsAndReadsBack()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("orders/row-1", ct);

        const int count = 16;
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
    }

    /// <summary>
    /// The partial-staging unwind mechanism: clearing a staged write intent that never proposed removes it
    /// with no Raft round trip, is idempotent on already-cleared state, and never touches an intent owned by
    /// another transaction — so a failed batch cannot leak a pinned intent or clobber a concurrent owner.
    /// </summary>
    [Fact]
    public async Task ApplyRolledBackMutations_ClearsStagedIntent_IdempotentAndOwnershipSafe()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.Raft.WaitForLeader(1, ct);

        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KeyValueContext context = DirectContext(node.Raft, logger);

        const string key = "orders/row-1";
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp commitId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        KeyValueEntry entry = new()
        {
            Value = "old"u8.ToArray(),
            Revision = 4,
            State = KeyValueState.Set,
            LastUsed = txId,
            LastModified = txId,
            MvccEntries = new() { [txId] = new KeyValueMvccEntry { Value = "new"u8.ToArray(), Revision = 5, State = KeyValueState.Set, LastModified = commitId } }
        };
        context.Store.Insert(key, entry);

        // Stage installs the intent.
        await new TryPrepareMutationsHandler(context).StageExecute(StagePrepareRequest(txId, commitId, key, KeyValueDurability.Persistent));
        Assert.NotNull(entry.WriteIntent);

        // Unwind clears it with no Raft.
        KeyValueResponse cleared = await new ApplyRolledBackMutationsHandler(context).Execute(ClearRequest(txId, key));
        Assert.Equal(KeyValueResponseType.RolledBack, cleared.Type);
        Assert.Null(entry.WriteIntent);

        // Idempotent on already-cleared state.
        KeyValueResponse again = await new ApplyRolledBackMutationsHandler(context).Execute(ClearRequest(txId, key));
        Assert.Equal(KeyValueResponseType.RolledBack, again.Type);

        // A foreign transaction's intent is left untouched.
        HLCTimestamp otherTx = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        entry.WriteIntent = new() { TransactionId = otherTx, Expires = otherTx + 15000 };
        KeyValueResponse foreign = await new ApplyRolledBackMutationsHandler(context).Execute(ClearRequest(txId, key));
        Assert.Equal(KeyValueResponseType.RolledBack, foreign.Type);
        Assert.NotNull(entry.WriteIntent);
        Assert.Equal(otherTx, entry.WriteIntent!.TransactionId);
    }

    private static KeyValueRequest ClearRequest(HLCTimestamp txId, string key) =>
        new(KeyValueRequestType.ApplyRolledBackMutations, txId, HLCTimestamp.Zero, key,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, null);

    private static KahunaConfiguration Config() => ConfigurationValidator.Validate(new()
    {
        LocksWorkers = 1,
        KeyValueWorkers = 1,
        BackgroundWriterWorkers = 1,
        Storage = "memory",
        CacheEntryTtl = TimeSpan.FromMinutes(5),
        CacheEntriesToRemove = 1000,
        MaxEntriesPerActor = 50_000,
        MaxBytesPerActor = 256L * 1024 * 1024,
        CollectBatchMax = 1000,
        RevisionRetention = 16,
        Phase2CommitTimeout = 5000
    });

    private static KeyValueRequest StagePrepareRequest(HLCTimestamp txId, HLCTimestamp commitId, string key, KeyValueDurability durability) =>
        new(KeyValueRequestType.StagePrepareMutations, txId, commitId, key,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, durability, 0, 0, null);

    private KeyValueContext DirectContext(IRaft raft, ILogger<IKahuna> logger) =>
        new(null!, new BTree<string, KeyValueEntry>(32), new(), new(), new(),
            null!, null!, new MemoryPersistenceBackend(), raft,
            new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), Config(), logger);
}
