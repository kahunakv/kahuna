using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transactional set flagged <see cref="KeyValueFlags.YieldToOlderReaders"/> gives way to an older transaction
/// that has read the key and is still running: the younger writer is refused at its write, and the older reader can
/// then write the key and commit. Without the flag the younger write is staged and the older reader loses.
/// </summary>
public sealed class TestYieldToOlderReaders
{
    private readonly ILoggerFactory loggerFactory;

    public TestYieldToOlderReaders(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
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
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("yield/x", ct);
        return node;
    }

    private static string NewKey() => "yield/" + Guid.NewGuid().ToString("N")[..8];

    private static async Task<TransactionHandle> Start(IKahuna kahuna, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Timeout = 30_000, Locking = KeyValueTransactionLocking.Optimistic },
            ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static async Task<KeyValueResponseType> TxGet(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            tx.CoordinatorKey, TransactionOperationId.NewRandom());
        return type;
    }

    private static async Task<KeyValueResponseType> TxSet(IKahuna kahuna, TransactionHandle tx, string key, string value, KeyValueFlags flags, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, flags, 0,
            KeyValueDurability.Persistent, ct, coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        return type;
    }

    private static async Task AutoCommitSet(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private const KeyValueFlags Yielding = KeyValueFlags.Set | KeyValueFlags.YieldToOlderReaders;

    [Fact]
    public async Task YoungerWriter_IsRefused_WhileAnOlderReaderRuns_AndTheOlderOneCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle older = await Start(kahuna, key + "/older", ct);
        TransactionHandle younger = await Start(kahuna, key + "/younger", ct);
        Assert.True(older.TransactionId.CompareTo(younger.TransactionId) < 0);

        Assert.Equal(KeyValueResponseType.Get, await TxGet(kahuna, older, key, ct));
        Assert.Equal(KeyValueResponseType.Get, await TxGet(kahuna, younger, key, ct));

        // The read of the older one may be answered off the actor, with its pin sent after it.
        await Task.Delay(100, ct);

        Assert.Equal(KeyValueResponseType.Aborted, await TxSet(kahuna, younger, key, "younger", Yielding, ct));
        await kahuna.LocateAndRollbackTransaction(younger, ct);

        // The older one is not refused by the younger one's pin.
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, older, key, "older", Yielding, ct));
        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(older, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed);
    }

    [Fact]
    public async Task YoungerWriter_WithoutTheFlag_StagesItsWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle older = await Start(kahuna, key + "/older", ct);
        TransactionHandle younger = await Start(kahuna, key + "/younger", ct);

        Assert.Equal(KeyValueResponseType.Get, await TxGet(kahuna, older, key, ct));
        await Task.Delay(100, ct);

        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, younger, key, "younger", KeyValueFlags.Set, ct));
    }

    [Fact]
    public async Task YoungerWriter_IsServed_OnceTheOlderReaderEnded()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle older = await Start(kahuna, key + "/older", ct);
        TransactionHandle younger = await Start(kahuna, key + "/younger", ct);

        Assert.Equal(KeyValueResponseType.Get, await TxGet(kahuna, older, key, ct));
        await Task.Delay(100, ct);
        Assert.Equal(KeyValueResponseType.Aborted, await TxSet(kahuna, younger, key, "younger", Yielding, ct));
        await kahuna.LocateAndRollbackTransaction(younger, ct);

        // The older reader ends without a write on the key: its pin stays on the entry, its session does not.
        await kahuna.LocateAndRollbackTransaction(older, ct);

        TransactionHandle next = await Start(kahuna, key + "/next", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, next, key, "next", Yielding, ct));
        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(next, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed);
    }
}
