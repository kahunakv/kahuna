using System.Text;
using Kahuna;
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
/// Tests for a multi-key transaction whose participants span one or more Raft partitions, covering the
/// end-to-end commit/rollback outcome. Every persistent transaction settles through the durable-intent
/// canonical record, whose single atomic decision makes a partial commit unrepresentable — the cases here
/// assert that all-or-nothing property for commit and rollback across partitions.
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
    /// End-to-end acceptance: a transaction writing many keys of one hash key space — all resolving to one
    /// partition — commits and every key is durably readable afterward. This is the motivating bulk-write
    /// shape (a table's rows), settled by the durable-intent canonical record.
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
    /// Cross-partition commit atomicity: committing a persistent interactive transaction that staged writes on
    /// two different partitions applies both as a unit. The canonical record is settled to Commit, so both
    /// partitions' mutations are durable — the atomic decision applied to the whole write set.
    /// </summary>
    [Fact]
    public async Task CrossPartitionCommit_IsAtomic_BothPartitionsDurable()
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

        // Both keys start with a committed value on their respective partitions.
        (KeyValueResponseType sa, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyA, initA, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sa);
        (KeyValueResponseType sb, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyB, initB, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sb);

        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = "xpart-commit",
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
        Assert.Equal(KeyValueResponseType.Committed, commitResult);

        // Both partitions' mutations are durable — the atomic decision applied to the whole write set.
        (KeyValueResponseType ra, ReadOnlyKeyValueEntry? ea) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, keyA, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, ra);
        Assert.Equal(newA, ea!.Value);

        (KeyValueResponseType rb, ReadOnlyKeyValueEntry? eb) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, keyB, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, rb);
        Assert.Equal(newB, eb!.Value);
    }

    /// <summary>
    /// Cross-partition rollback atomicity: aborting a persistent interactive transaction that staged writes on
    /// two different partitions rolls both back as a unit. The canonical record is settled to Abort, so neither
    /// partition's staged mutation is applied — both keys retain their pre-transaction value.
    /// </summary>
    [Fact]
    public async Task CrossPartitionRollback_IsAtomic_NeitherPartitionChanged()
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

        (KeyValueResponseType sa, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyA, initA, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sa);
        (KeyValueResponseType sb, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyB, initB, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sb);

        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = "xpart-rollback",
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 10_000,
            },
            ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        await node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, keyA, "new-a"u8.ToArray(), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        await node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, keyB, "new-b"u8.ToArray(), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        KeyValueResponseType rollbackResult = await node.Kahuna.LocateAndRollbackTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.RolledBack, rollbackResult);

        // Neither partition applied its staged mutation — both keep the pre-transaction value.
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
    /// Rollback releases the write intent: after a persistent interactive transaction that staged a write to a
    /// key is rolled back, the key is unpinned — a fresh direct write to it succeeds and takes effect rather
    /// than conflicting with a leaked intent. Proves the durable-path rollback both discards the staged mutation
    /// and clears the pin so the key is writable again.
    /// </summary>
    [Fact]
    public async Task Rollback_ReleasesWriteIntent_KeyWritableAgain()
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
        byte[] finalA = "final-a"u8.ToArray();

        (KeyValueResponseType sa, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keyA, initA, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, sa);

        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = "rollback-intent",
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 10_000,
            },
            ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        await node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, keyA, "new-a"u8.ToArray(), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        await node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, keyB, "new-b"u8.ToArray(), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        KeyValueResponseType rollbackResult = await node.Kahuna.LocateAndRollbackTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.RolledBack, rollbackResult);

        // The key kept its pre-transaction value...
        (KeyValueResponseType ra, ReadOnlyKeyValueEntry? ea) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, keyA, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, ra);
        Assert.Equal(initA, ea!.Value);

        // ...and the intent was cleared: a fresh direct write succeeds instead of conflicting.
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
}
