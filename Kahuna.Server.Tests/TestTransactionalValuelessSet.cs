using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transactional set that carries no value commits a key that exists and holds nothing — the same outcome a
/// direct null-value set produces — never a deletion. The durable-intent path used to re-derive set-versus-delete
/// from value presence at freeze, so a registered persistent valueless set committed as a delete and the key
/// vanished; the staged state now carries the operation the caller issued. The delete test is the control: an
/// explicit transactional delete must still remove the key.
/// </summary>
public sealed class TestTransactionalValuelessSet
{
    private readonly ILoggerFactory loggerFactory;

    public TestTransactionalValuelessSet(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaNode CreateNode(ILoggerFactory loggerFactory) => new(new EmbeddedKahunaOptions
    {
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        PartitionExecutorPoolSize = 1,
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    }, loggerFactory);

    private static async Task<TransactionHandle> StartTransaction(KahunaManager kahuna, string coordinatorKey, CancellationToken ct)
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
        return handle;
    }

    [Fact]
    public async Task RegisteredPersistentSet_NullValue_CommitsAsExistingValuelessKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("valueless/1", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        TransactionHandle handle = await StartTransaction(kahuna, "valueless-set-coordinator", ct);

        (KeyValueResponseType setType, long revision, _) = await kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, "valueless/1", value: null, compareValue: null, compareRevision: 0,
            KeyValueFlags.Set, expiresMs: 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, setType);

        (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);

        // The committed key must exist, at the committed revision, with no value — not be deleted.
        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "valueless/1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, getType);
        Assert.NotNull(entry);
        Assert.Equal(KeyValueState.Set, entry!.State);
        Assert.Null(entry.Value);
        Assert.Equal(revision, entry.Revision);
    }

    [Fact]
    public async Task RegisteredPersistentDelete_StillRemovesTheKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("valueless/2", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        (KeyValueResponseType seedType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "valueless/2", "seed"u8.ToArray(), null, 0,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, seedType);

        TransactionHandle handle = await StartTransaction(kahuna, "valueless-delete-coordinator", ct);

        (KeyValueResponseType deleteType, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
            handle.TransactionId, "valueless/2", KeyValueDurability.Persistent, ct,
            handle.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Deleted, deleteType);

        (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);

        (KeyValueResponseType getType, _) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "valueless/2", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.DoesNotExist, getType);
    }
}
