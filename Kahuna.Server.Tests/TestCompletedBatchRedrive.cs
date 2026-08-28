using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A many-key batch operation that already completed must never be re-executed by a same-id resend.
/// The batch registers once on the coordinator; its confirmed effects fold exactly once. A resend that
/// reaches the completed registration means the caller's view of the first drive is stale (the first
/// drive's completion landed after the caller stopped waiting for it). Re-executing the batch there
/// mutates participants outside the operation registry: the session freeze cannot see the re-drive, so
/// it can land mid-commit or after a rollback released the keys, re-staging values and re-planting
/// write intents for a transaction that is already finalized — the state behind long-held orphan
/// intents and torn working sets observed under client-timeout collisions. The resend must answer
/// transient and leave both the participant state and the folded working set untouched.
/// </summary>
public sealed class TestCompletedBatchRedrive
{
    private readonly ILoggerFactory loggerFactory;

    public TestCompletedBatchRedrive(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("redrive/row-0", ct);
        return node;
    }

    private static async Task<TransactionHandle> StartTransaction(EmbeddedKahunaNode node, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 10_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static List<KahunaSetKeyValueRequestItem> SetBatch(HLCTimestamp transactionId, string key, string value) =>
    [
        new()
        {
            TransactionId = transactionId,
            Key = key,
            Value = Encoding.UTF8.GetBytes(value),
            CompareRevision = -1,
            Flags = KeyValueFlags.Set,
            ExpiresMs = 0,
            Durability = KeyValueDurability.Persistent
        }
    ];

    [Fact]
    public async Task SameIdResend_OfCompletedSetBatch_IsRefusedAndDoesNotReexecute()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        TransactionHandle handle = await StartTransaction(node, "redrive/tx-set", ct);

        const string key = "redrive/row-0";
        TransactionOperationId operationId = TransactionOperationId.NewRandom();

        // First drive: registers, applies, folds, completes. The fresh key stages at revision 0.
        List<KahunaSetKeyValueResponseItem> first = await node.Kahuna.LocateAndTrySetManyKeyValue(
            SetBatch(handle.TransactionId, key, "v1"), ct, handle.CoordinatorKey, operationId);
        Assert.Single(first);
        Assert.Equal(KeyValueResponseType.Set, first[0].Type);
        Assert.Equal(0, first[0].Revision);

        // Same-id resend of the identical batch: the registration is already completed, so the batch
        // must NOT re-execute (a re-drive would re-stage the key and bump the staged revision outside
        // the folded working set). The refusal answers transient for every item.
        List<KahunaSetKeyValueResponseItem> resent = await node.Kahuna.LocateAndTrySetManyKeyValue(
            SetBatch(handle.TransactionId, key, "v1"), ct, handle.CoordinatorKey, operationId);
        Assert.Single(resent);
        Assert.Equal(KeyValueResponseType.MustRetry, resent[0].Type);

        // The transaction still commits its once-folded write: exactly one revision, the first drive's.
        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.NotNull(entry);
        Assert.Equal("v1", Encoding.UTF8.GetString(entry!.Value ?? []));
        Assert.Equal(0, entry.Revision);
    }

    [Fact]
    public async Task SameIdResend_OfCompletedDeleteBatch_IsRefusedAndDoesNotReexecute()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        // Seed a committed row the transaction will delete.
        const string key = "redrive/row-del";
        (KeyValueResponseType seeded, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("seed"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, seeded);

        TransactionHandle handle = await StartTransaction(node, "redrive/tx-del", ct);

        TransactionOperationId operationId = TransactionOperationId.NewRandom();
        List<KahunaDeleteKeyValueRequestItem> batch =
        [
            new() { TransactionId = handle.TransactionId, Key = key, Durability = KeyValueDurability.Persistent }
        ];

        List<KahunaDeleteKeyValueResponseItem> first = await node.Kahuna.LocateAndTryDeleteManyKeyValue(
            batch, ct, handle.CoordinatorKey, operationId);
        Assert.Single(first);
        Assert.Equal(KeyValueResponseType.Deleted, first[0].Type);

        // Same-id resend after completion: refused as transient, never re-executed.
        List<KahunaDeleteKeyValueResponseItem> resent = await node.Kahuna.LocateAndTryDeleteManyKeyValue(
            batch, ct, handle.CoordinatorKey, operationId);
        Assert.Single(resent);
        Assert.Equal(KeyValueResponseType.MustRetry, resent[0].Type);

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.True(
            readType == KeyValueResponseType.DoesNotExist
            || (readType == KeyValueResponseType.Get && entry?.State == KeyValueState.Deleted),
            $"deleted row must not be readable; got {readType}");
    }
}
