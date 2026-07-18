using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
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
/// collapsing N fsyncs into one. Covers the routing/grouping seam, the stage-and-propose and apply steps,
/// the partial-staging unwind, and the atomicity guard that a committed shared ticket can never surface an
/// abort. Multi-node failover, split-between-prepare-and-commit, and operation-count instrumentation are
/// tracked separately in the remediation plan.
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

        Dictionary<(int PartitionId, long Generation), List<string>> groups =
            KeyValuesManager.GroupByPartition(registry, rangeMap, router, keys, k => k);

        // Every key is present exactly once, and each key sits in the group for the partition it routes to.
        Assert.Equal(keys.Count, groups.Values.Sum(g => g.Count));
        Assert.True(groups.Count > 1, "64 keys across distinct key spaces should span more than one partition");

        foreach (((int partitionId, long _), List<string> items) in groups)
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

        Dictionary<(int PartitionId, long Generation), List<string>> groups =
            KeyValuesManager.GroupByPartition(registry, rangeMap, router, keys, k => k);

        List<string> items = Assert.Single(groups.Values);
        Assert.Equal(keys.Count, items.Count);
        (int _, long generation) = Assert.Single(groups.Keys);
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

    /// <summary>
    /// Cross-partition atomicity guard for B1: a multi-key transaction where one partition commits for real
    /// and another is forced to fail its commit must report <b>MustRetry</b> (in-doubt), never a false
    /// <c>Aborted</c> — because the committed partition is durable and an abort would contradict it. Uses the
    /// commit-failure injection seam to fail exactly one partition group's <c>CommitLogs</c> while the other
    /// commits. Asserts the transaction returns MustRetry and the committed partition's value is durable while
    /// the failed partition keeps its pre-transaction value.
    /// </summary>
    [Fact]
    public async Task CrossPartitionPartialCommit_ReturnsMustRetryNotAborted_AndCommittedPartitionIsDurable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 8
        }, loggerFactory);
        await node.StartAsync(ct);

        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        (string keyA, string keyB) = FindKeysOnDifferentPartitions(node, logger);
        await node.WaitForLeaderForKeyAsync(keyA, ct);
        await node.WaitForLeaderForKeyAsync(keyB, ct);

        byte[] initA = "init-a"u8.ToArray();
        byte[] initB = "init-b"u8.ToArray();
        byte[] newA = "new-a"u8.ToArray();
        byte[] newB = "new-b"u8.ToArray();

        // Both keys start with a committed value.
        (KeyValueResponseType sa, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyA, initA, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sa);
        (KeyValueResponseType sb, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyB, initB, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sb);

        // Force partition B's group to hard-fail its commit (its CommitLogs never takes effect).
        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;
        kv.ForceCommitOutcomeForKey = k => string.Equals(k, keyB, StringComparison.Ordinal) ? KeyValueResponseType.Errored : null;

        try
        {
            (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey = "xpart-b1",
                    Locking = KeyValueTransactionLocking.Pessimistic,
                    Timeout = 10_000,
                },
                ct);
            Assert.Equal(KeyValueResponseType.Set, startType);

            (KeyValueResponseType wa, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                handle.TransactionId, keyA, newA, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, wa);
            (KeyValueResponseType wb, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                handle.TransactionId, keyB, newB, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, wb);

            (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

            // The fix: partition A committed durably while B failed → in-doubt, NOT a false Aborted.
            Assert.Equal(KeyValueResponseType.MustRetry, commitResult);
        }
        finally
        {
            kv.ForceCommitOutcomeForKey = null;
        }

        // Partition A's mutation is durably committed.
        (KeyValueResponseType ra, ReadOnlyKeyValueEntry? ea) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, keyA, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, ra);
        Assert.Equal(newA, ea!.Value);

        // Partition B's mutation never committed — it retains its pre-transaction value.
        (KeyValueResponseType rb, ReadOnlyKeyValueEntry? eb) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, keyB, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rb);
        Assert.Equal(initB, eb!.Value);
    }

    /// <summary>
    /// B5 split-cohesion guard: if a range split/move divides a prepared ticket's keys across partitions
    /// between prepare and commit, the batched commit must NOT commit the shared ticket (committing on the
    /// origin partition would strand the moved keys' logs where reads no longer route). It returns in-doubt
    /// MustRetry so the transaction re-drives, and neither key is committed — no stranding, no partial commit.
    /// Uses the commit-partition override seam to simulate the split without running a real one.
    /// </summary>
    [Fact]
    public async Task PreparedTicketSplitAcrossPartitions_RefusesCommit_ReturnsMustRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);

        // Two keys in ONE key space share a partition, so the batched prepare groups them under one ticket.
        const string keyA = "split/a";
        const string keyB = "split/b";
        await node.WaitForLeaderForKeyAsync(keyA, ct);

        byte[] initA = "init-a"u8.ToArray();
        byte[] initB = "init-b"u8.ToArray();
        byte[] newA = "new-a"u8.ToArray();
        byte[] newB = "new-b"u8.ToArray();

        (KeyValueResponseType sa, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyA, initA, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sa);
        (KeyValueResponseType sb, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyB, initB, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sb);

        // Simulate a split moving keyB to a different partition at commit time (999 is outside the pool).
        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;
        kv.PartitionOverrideForCommit = k => string.Equals(k, keyB, StringComparison.Ordinal) ? 999 : null;

        try
        {
            (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey = "split-b5",
                    Locking = KeyValueTransactionLocking.Pessimistic,
                    Timeout = 10_000,
                },
                ct);
            Assert.Equal(KeyValueResponseType.Set, startType);

            await node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, keyA, newA, null, -1,
                KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            await node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, keyB, newB, null, -1,
                KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

            // The ticket group split across partitions ⇒ in-doubt MustRetry, never a stranding commit.
            Assert.Equal(KeyValueResponseType.MustRetry, commitResult);
        }
        finally
        {
            kv.PartitionOverrideForCommit = null;
        }

        // Neither key committed — both retain their pre-transaction value.
        (KeyValueResponseType ra, ReadOnlyKeyValueEntry? ea) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, keyA, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, ra);
        Assert.Equal(initA, ea!.Value);
        (KeyValueResponseType rb, ReadOnlyKeyValueEntry? eb) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, keyB, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rb);
        Assert.Equal(initB, eb!.Value);
    }

    /// <summary>
    /// S1 batched rollback: when a transaction prepares one partition (getting a shared ticket) but another
    /// partition fails prepare, the coordinator aborts and rolls the prepared partition back through the
    /// batched path — one <c>RollbackLogs</c> for the ticket, then a per-key intent clear. Asserts the
    /// transaction aborts, the prepared key is not committed, and — crucially — its write intent is cleared
    /// (a fresh direct write to it succeeds), proving the batched rollback settled the ticket and cleaned up.
    /// </summary>
    [Fact]
    public async Task PreparedPartitionRolledBackViaBatch_ClearsTicketAndIntents_KeyWritableAgain()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 8
        }, loggerFactory);
        await node.StartAsync(ct);

        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        (string keyA, string keyB) = FindKeysOnDifferentPartitions(node, logger);
        await node.WaitForLeaderForKeyAsync(keyA, ct);
        await node.WaitForLeaderForKeyAsync(keyB, ct);

        byte[] initA = "init-a"u8.ToArray();
        byte[] newA = "new-a"u8.ToArray();
        byte[] finalA = "final-a"u8.ToArray();

        (KeyValueResponseType sa, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyA, initA, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sa);

        // Force partition B's prepare to fail so the transaction aborts after partition A has prepared a ticket.
        KeyValuesManager kv = ((KahunaManager)node.Kahuna).KeyValues;
        kv.ForcePrepareOutcomeForKey = k => string.Equals(k, keyB, StringComparison.Ordinal) ? KeyValueResponseType.Errored : null;

        try
        {
            (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey = "rollback-s1",
                    Locking = KeyValueTransactionLocking.Pessimistic,
                    Timeout = 10_000,
                },
                ct);
            Assert.Equal(KeyValueResponseType.Set, startType);

            await node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, keyA, newA, null, -1,
                KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            await node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, keyB, "new-b"u8.ToArray(), null, -1,
                KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Aborted, commitResult);
        }
        finally
        {
            kv.ForcePrepareOutcomeForKey = null;
        }

        // Partition A was rolled back — it keeps its pre-transaction value.
        (KeyValueResponseType ra, ReadOnlyKeyValueEntry? ea) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, keyA, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, ra);
        Assert.Equal(initA, ea!.Value);

        // The batched rollback cleared A's write intent — a fresh direct write succeeds instead of conflicting.
        (KeyValueResponseType fresh, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyA, finalA, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, fresh);

        (KeyValueResponseType rf, ReadOnlyKeyValueEntry? ef) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, keyA, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rf);
        Assert.Equal(finalA, ef!.Value);
    }

    private static (string keyA, string keyB) FindKeysOnDifferentPartitions(EmbeddedKahunaNode node, ILogger<IKahuna> logger)
    {
        KeySpaceRegistry registry = new();
        DataPartitionRouter router = new(node.Raft);
        RangeMap rangeMap = new RangeMapStore(node.Raft, null, null, logger).Current;

        const string first = "xpb-0/k";
        int firstPartition = RangeRouting.Locate(registry, rangeMap, router, first).PartitionId;

        for (int i = 1; i < 256; i++)
        {
            string candidate = $"xpb-{i}/k";
            if (RangeRouting.Locate(registry, rangeMap, router, candidate).PartitionId != firstPartition)
                return (first, candidate);
        }

        throw new InvalidOperationException("Could not find two keys routing to different partitions");
    }

    private static KeyValueRequest ClearRequest(HLCTimestamp txId, string key) =>
        new(KeyValueRequestType.ApplyRolledBackMutations, txId, HLCTimestamp.Zero, key,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, null);

    private static KeyValueRequest ApplyCommittedRequest(HLCTimestamp txId, string key) =>
        new(KeyValueRequestType.ApplyCommittedMutations, txId, HLCTimestamp.Zero, key,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, null);

    /// <summary>
    /// Atomicity guard: the batched apply runs only after the group's shared <c>CommitLogs</c> already made the
    /// mutation durable, so it must never report a definite failure. When our write intent has been replaced by
    /// a <b>later</b> transaction (the lease expired and another owner took the entry) but a durable proof of
    /// our commit exists, the apply reports <c>Committed</c> — not <c>Errored</c> — leaving the foreign owner's
    /// intent untouched. A false <c>Errored</c> here is what lets a committed transaction be reported aborted.
    /// </summary>
    [Fact]
    public async Task ApplyExecute_ForeignIntentWithDurableProof_ReturnsCommittedNotErrored()
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
        HLCTimestamp foreignTx = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        // A later transaction now holds the intent, and our commit is durably proven (recorded here).
        context.Store.Insert(key, new KeyValueEntry
        {
            Value = "v"u8.ToArray(),
            Revision = 4,
            State = KeyValueState.Set,
            WriteIntent = new() { TransactionId = foreignTx, Expires = foreignTx + 15000 }
        });
        context.RecordCommitted(txId);

        KeyValueResponse response = await new TryCommitMutationsHandler(context).ApplyExecute(ApplyCommittedRequest(txId, key));

        Assert.Equal(KeyValueResponseType.Committed, response.Type);
        Assert.NotNull(context.Store.Get(key)!.WriteIntent);
        Assert.Equal(foreignTx, context.Store.Get(key)!.WriteIntent!.TransactionId);
    }

    /// <summary>
    /// Atomicity guard: with no live own-intent and no durable proof yet (the prepare state is gone on this
    /// node), the batched apply is in-doubt, not failed — it returns <c>MustRetry</c> for re-drive/recovery,
    /// never <c>Errored</c>. Nothing may surface a definite failure after the shared ticket has committed.
    /// </summary>
    [Fact]
    public async Task ApplyExecute_NoIntentNoProof_ReturnsMustRetryNotErrored()
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
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        // No write intent, no MVCC for the transaction, no recorded decision — prepare state is simply gone.
        context.Store.Insert(key, new KeyValueEntry { Value = "v"u8.ToArray(), Revision = 4, State = KeyValueState.Set });

        KeyValueResponse response = await new TryCommitMutationsHandler(context).ApplyExecute(ApplyCommittedRequest(txId, key));

        Assert.Equal(KeyValueResponseType.MustRetry, response.Type);
    }

    /// <summary>
    /// Staging-leak guard (partial staging): a persistent participant whose key-range generation fence rejects
    /// must leave <b>no</b> write intent. The batched staging path only unwinds participants that stage
    /// Prepared, so a fence rejection that had installed an intent would leak it — the coordinator never sees
    /// the key as prepared and never rolls it back. All fallible checks now run before the intent install, so
    /// the rejection returns MustRetry with the entry unpinned.
    /// </summary>
    [Fact]
    public async Task StageExecute_KeyRangeFenceFails_LeavesNoIntent()
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

        // A key-range key with no descriptor in the (empty) range map fails the fence unconditionally.
        context.KeySpaceRegistry.RegisterKeyRange("ranged");
        const string key = "ranged/row-1";
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

        Assert.Equal(KeyValueResponseType.MustRetry, response.Type);
        Assert.Null(response.StagedProposal);
        Assert.Null(entry.WriteIntent);
    }

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
