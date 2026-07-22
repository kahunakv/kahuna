using System.Collections.Concurrent;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
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
/// The registered (interactive) operation path — the one CamusDB drives via
/// <c>LocateAndTrySetManyKeyValue</c>/<c>LocateAndCommitTransaction</c> — now stages the actor-confirmed committed
/// value on completion, so an all-persistent registered transaction finalizes through the durable-intent path
/// (canonical record + prepared intents) rather than the manual ticket path. The durable canonical record is the
/// distinguishing evidence: the ticket path leaves none.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRegisteredDurableStaging
{
    private readonly ILoggerFactory loggerFactory;

    private sealed class CountingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;

        public CountingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public long Calls;
        public long Entries;
        public readonly ConcurrentQueue<string[]> Batches = new();

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            Interlocked.Add(ref Entries, entries.Count);
            Batches.Enqueue(entries.Select(entry => entry.Type).ToArray());
            return inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    public TestRegisteredDurableStaging(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<(KahunaManager, TransactionHandle)> StartTransaction(KahunaManager kahuna, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                AsyncRelease = true,
                Timeout = 10_000,
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return (kahuna, handle);
    }

    [Fact]
    public async Task RegisteredSetMany_OnePartition_CoalescesMaterializationRecords()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CountingExecutor? counter = null;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            WriteBatchExecutorDecorator = inner => counter = new CountingExecutor(inner)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("coalesce/0", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        (_, TransactionHandle handle) = await StartTransaction(kahuna, "coalesce-registered-setmany", ct);
        List<KahunaSetKeyValueRequestItem> items = new(10);
        for (int i = 0; i < 10; i++)
        {
            items.Add(new()
            {
                TransactionId = handle.TransactionId,
                Key = $"coalesce/{i}",
                Value = "v"u8.ToArray(),
                ExpiresMs = 0,
                Flags = KeyValueFlags.None,
                Durability = KeyValueDurability.Persistent
            });
        }

        List<KahunaSetKeyValueResponseItem> writes = await kahuna.LocateAndTrySetManyKeyValue(
            items, ct, handle.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.All(writes, write => Assert.Equal(KeyValueResponseType.Set, write.Type));

        long callsBefore = counter!.Calls;
        long entriesBefore = counter.Entries;
        int batchesBefore = counter.Batches.Count;
        (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.Committed, commitType);
        string[][] commitBatches = counter.Batches.Skip(batchesBefore).ToArray();
        int materializationEntries = commitBatches.Sum(batch =>
            batch.Count(type => type == ReplicationTypes.KeyValues));
        Assert.Equal(items.Count, materializationEntries);
        Assert.Contains(commitBatches, batch =>
            batch.Count(type => type == ReplicationTypes.KeyValues) > 1);
        Assert.True(counter.Calls - callsBefore < counter.Entries - entriesBefore);
    }

    /// <summary>
    /// Drives the public registered API used by database consumers: zero-duration exclusive locks,
    /// a row-plus-index initial commit, a live snapshot hold, and enough later row transactions to
    /// move the fork revision beyond ordinary retention. Exact-revision and point/range as-of reads
    /// must all retain the fork value.
    /// </summary>
    [Fact]
    public async Task ZeroDurationRegisteredWrites_PreserveHeldForkRevisionPastRetention()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            RevisionRetention = 2,
        }, loggerFactory);
        await node.StartAsync(ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        string suffix = Guid.NewGuid().ToString("N")[..8];
        string rowPrefix = $"fork/rows/{suffix}";
        string rowKey = $"{rowPrefix}/row";
        string indexKey = $"fork/index/{suffix}/value";
        await node.WaitForLeaderForKeyAsync(rowKey, ct);
        await node.WaitForLeaderForKeyAsync(indexKey, ct);

        (_, TransactionHandle initial) = await StartTransaction(kahuna, $"fork/init/{suffix}", ct);
        foreach (string key in new[] { rowKey, indexKey })
        {
            (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                initial.TransactionId, key, 0, KeyValueDurability.Persistent, ct,
                initial.CoordinatorKey, TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, lockType);
        }

        List<KahunaSetKeyValueResponseItem> initialWrites = await kahuna.LocateAndTrySetManyKeyValue(
        [
            new()
            {
                TransactionId = initial.TransactionId, Key = rowKey, Value = "fork-value"u8.ToArray(),
                ExpiresMs = 0, Flags = KeyValueFlags.None, Durability = KeyValueDurability.Persistent
            },
            new()
            {
                TransactionId = initial.TransactionId, Key = indexKey, Value = Encoding.UTF8.GetBytes(rowKey),
                ExpiresMs = 0, Flags = KeyValueFlags.None, Durability = KeyValueDurability.Persistent
            }
        ], ct, initial.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.All(initialWrites, write => Assert.Equal(KeyValueResponseType.Set, write.Type));

        (KeyValueResponseType initialCommit, _) = await kahuna.LocateAndCommitTransaction(initial, ct);
        Assert.Equal(KeyValueResponseType.Committed, initialCommit);

        (KeyValueResponseType initialReadType, ReadOnlyKeyValueEntry? initialRow) =
            await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, rowKey, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, initialReadType);
        Assert.NotNull(initialRow);
        long forkRevision = initialRow!.Revision;
        HLCTimestamp forkTimestamp = initialRow.LastModified;

        (KeyValueResponseType holdType, string holdId, _) =
            await kahuna.LocateAndAcquireSnapshotHold($"fork-holder/{suffix}", forkTimestamp, 60_000, ct);
        Assert.Equal(KeyValueResponseType.Set, holdType);

        try
        {
            for (int i = 0; i < 8; i++)
            {
                (_, TransactionHandle update) = await StartTransaction(kahuna, $"fork/update/{suffix}/{i}", ct);
                (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                    update.TransactionId, rowKey, 0, KeyValueDurability.Persistent, ct,
                    update.CoordinatorKey, TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Locked, lockType);

                (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                    update.TransactionId, rowKey, Encoding.UTF8.GetBytes($"later-{i}"), null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: update.CoordinatorKey,
                    operationId: TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Set, setType);

                (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(update, ct);
                Assert.Equal(KeyValueResponseType.Committed, commitType);
            }

            (KeyValueResponseType exactType, ReadOnlyKeyValueEntry? exact) =
                await kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, rowKey, forkRevision, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, exactType);
            Assert.Equal("fork-value"u8.ToArray(), exact!.Value);

            (KeyValueResponseType pointType, ReadOnlyKeyValueEntry? point) =
                await kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, rowKey, -1, forkTimestamp,
                    KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, pointType);
            Assert.Equal("fork-value"u8.ToArray(), point!.Value);

            KeyValueGetByRangeResult range = await kahuna.LocateAndGetByRange(
                HLCTimestamp.Zero, rowPrefix, rowKey, true, rowKey, true, 10,
                forkTimestamp, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, range.Type);
            Assert.Single(range.Items);
            Assert.Equal(rowKey, range.Items[0].Item1);
            Assert.Equal("fork-value"u8.ToArray(), range.Items[0].Item2.Value);
        }
        finally
        {
            await kahuna.LocateAndReleaseSnapshotHold(holdId, ct);
        }
    }

    [Fact]
    public async Task RegisteredSetMany_AllPersistent_FinalizesThroughDurableIntentPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("accounts/alice", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        (_, TransactionHandle handle) = await StartTransaction(kahuna, "reg-tx-setmany", ct);
        HLCTimestamp txId = handle.TransactionId;

        // Register a persistent set-many through the same API CamusDB uses — no script staging. The batch's
        // transaction id travels on each request item.
        List<KahunaSetKeyValueRequestItem> items =
        [
            new() { TransactionId = txId, Key = "accounts/alice", Value = "100"u8.ToArray(), ExpiresMs = 0, Flags = KeyValueFlags.None, Durability = KeyValueDurability.Persistent },
            new() { TransactionId = txId, Key = "accounts/bob", Value = "200"u8.ToArray(), ExpiresMs = 0, Flags = KeyValueFlags.None, Durability = KeyValueDurability.Persistent },
        ];

        List<KahunaSetKeyValueResponseItem> setResponses = await kahuna.LocateAndTrySetManyKeyValue(
            items, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.All(setResponses, r => Assert.Equal(KeyValueResponseType.Set, r.Type));

        (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);

        // The registered writes staged their confirmed values, so the transaction took the durable-intent path: a
        // canonical Commit record exists. The manual ticket path would leave no such record.
        TransactionRecord? record = kahuna.DurableTransactionRecordStore.Get(txId, 1);
        Assert.NotNull(record);
        Assert.Equal(TransactionDecision.Commit, record!.Decision);

        // The committed values are durably readable.
        foreach ((string key, string expected) in new[] { ("accounts/alice", "100"), ("accounts/bob", "200") })
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal(Encoding.UTF8.GetBytes(expected), entry!.Value);
        }
    }

    [Fact]
    public async Task RegisteredPointSet_AllPersistent_FinalizesThroughDurableIntentPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("orders/1", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        (_, TransactionHandle handle) = await StartTransaction(kahuna, "reg-tx-point", ct);
        HLCTimestamp txId = handle.TransactionId;

        (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            txId, "orders/1", "created"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setType);

        (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);

        TransactionRecord? record = kahuna.DurableTransactionRecordStore.Get(txId, 1);
        Assert.NotNull(record);
        Assert.Equal(TransactionDecision.Commit, record!.Decision);
    }

    /// <summary>
    /// A registered <c>SET NOREV</c> that finalizes through the durable-intent path must materialize revision-free,
    /// exactly as a direct write would: the flag is now carried on the staged mutation rather than dropped at
    /// staging and hardcoded false in the freeze. The contrast key, written without the flag, retains its prior
    /// revision — proving the suppression is the flag's doing, not a quirk of the durable path.
    /// </summary>
    [Fact]
    public async Task RegisteredSetNoRev_MaterializesRevisionFreeThroughDurablePath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("hist/norev", ct);
        await node.WaitForLeaderForKeyAsync("hist/keep", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        // First transaction: establish revision 0 on both keys with ordinary (history-retaining) sets.
        (_, TransactionHandle first) = await StartTransaction(kahuna, "reg-norev-1", ct);
        foreach (string key in new[] { "hist/norev", "hist/keep" })
        {
            (KeyValueResponseType t, _, _) = await kahuna.LocateAndTrySetKeyValue(
                first.TransactionId, key, "v0"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: first.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, t);
        }
        (KeyValueResponseType firstCommit, _) = await kahuna.LocateAndCommitTransaction(first, ct);
        Assert.Equal(KeyValueResponseType.Committed, firstCommit);

        // Second transaction: overwrite to revision 1 — hist/norev with SET NOREV (suppress history), hist/keep
        // ordinary (retain the prior revision). Both finalize through the durable-intent path.
        (_, TransactionHandle second) = await StartTransaction(kahuna, "reg-norev-2", ct);
        HLCTimestamp secondTxId = second.TransactionId;

        (KeyValueResponseType norevType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            secondTxId, "hist/norev", "v1"u8.ToArray(), null, -1, KeyValueFlags.SetNoRevision, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: second.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, norevType);

        (KeyValueResponseType keepType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            secondTxId, "hist/keep", "v1"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: second.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, keepType);

        (KeyValueResponseType secondCommit, _) = await kahuna.LocateAndCommitTransaction(second, ct);
        Assert.Equal(KeyValueResponseType.Committed, secondCommit);
        Assert.Equal(TransactionDecision.Commit, kahuna.DurableTransactionRecordStore.Get(secondTxId, 1)!.Decision);

        // The NOREV key's prior revision was not retained: reading revision 0 no longer resolves.
        (KeyValueResponseType norevRev0Type, _) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "hist/norev", 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, norevRev0Type);

        // The contrast key kept its prior revision: reading revision 0 still returns the original value.
        (KeyValueResponseType keepRev0Type, ReadOnlyKeyValueEntry? keepRev0) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "hist/keep", 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, keepRev0Type);
        Assert.Equal("v0"u8.ToArray(), keepRev0!.Value);
    }
}
