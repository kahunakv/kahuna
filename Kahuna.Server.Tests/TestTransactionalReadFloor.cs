using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transaction started with <see cref="KeyValueTransactionOptions.ReadFloor"/> has its registered latest reads
/// held to one point in time: a read of a new key is answered only if the keys read before still have the
/// revisions the transaction read. Without it each key is read at its own time and only the commit refuses.
/// </summary>
public sealed class TestTransactionalReadFloor
{
    private readonly ILoggerFactory loggerFactory;

    public TestTransactionalReadFloor(ITestOutputHelper outputHelper)
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
        await node.WaitForLeaderForKeyAsync("readfloor/x", ct);
        return node;
    }

    private static string NewPrefix() => "readfloor/" + Guid.NewGuid().ToString("N")[..8] + "/";

    private static async Task<TransactionHandle> Start(IKahuna kahuna, string coordinatorKey, CancellationToken ct, bool readFloor = true)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Timeout = 30_000, Locking = KeyValueTransactionLocking.Optimistic, ReadFloor = readFloor },
            ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> TxGet(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct) =>
        kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            tx.CoordinatorKey, TransactionOperationId.NewRandom()).AsTask();

    private static Task<KeyValueProbeResult[]> TxProbe(IKahuna kahuna, TransactionHandle tx, string[] keys, CancellationToken ct) =>
        kahuna.LocateAndTryProbeMany(
            tx.TransactionId, HLCTimestamp.Zero, keys, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct,
            tx.CoordinatorKey, TransactionOperationId.NewRandom());

    private static async Task<KeyValueResponseType> TxSet(IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        return type;
    }

    private static async Task AutoCommitSet(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>Another transaction writes both keys and commits.</summary>
    private static async Task MoveBoth(IKahuna kahuna, string a, string b, string prefix, CancellationToken ct)
    {
        TransactionHandle mover = await Start(kahuna, prefix + "mover", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, mover, a, "50", ct));
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, mover, b, "150", ct));
        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(mover, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed);
    }

    private static string ValueOf((KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry) read)
    {
        Assert.Equal(KeyValueResponseType.Get, read.Type);
        return Encoding.UTF8.GetString(read.Entry!.Value!);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ChangeOfBothKeysBetweenTwoReads(bool readFloor)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string prefix = NewPrefix();
        string a = prefix + "a", b = prefix + "b";
        await AutoCommitSet(kahuna, a, "100", ct);
        await AutoCommitSet(kahuna, b, "100", ct);

        TransactionHandle reader = await Start(kahuna, prefix + "reader", ct, readFloor);
        Assert.Equal("100", ValueOf(await TxGet(kahuna, reader, a, ct)));

        await MoveBoth(kahuna, a, b, prefix, ct);

        (KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry) second = await TxGet(kahuna, reader, b, ct);

        if (readFloor)
        {
            // The first key changed, so the second is not handed out.
            Assert.Equal(KeyValueResponseType.Aborted, second.Type);
            Assert.Null(second.Entry);
        }
        else
        {
            // Old a and new b: 250 in total, which was never committed.
            Assert.Equal("150", ValueOf(second));
        }

        // The commit is refused in both modes.
        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(reader, ct);
        Assert.NotEqual(KeyValueResponseType.Committed, committed);
    }

    [Fact]
    public async Task ChangeOfAnUnreadKey_IsServed_AndTheTransactionCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string prefix = NewPrefix();
        string a = prefix + "a", b = prefix + "b", c = prefix + "c";
        await AutoCommitSet(kahuna, a, "100", ct);
        await AutoCommitSet(kahuna, b, "100", ct);
        await AutoCommitSet(kahuna, c, "0", ct);

        TransactionHandle reader = await Start(kahuna, prefix + "reader", ct);
        Assert.Equal("100", ValueOf(await TxGet(kahuna, reader, a, ct)));

        // Only b changes: a still holds what the reader read, so the new b is consistent with it.
        await AutoCommitSet(kahuna, b, "175", ct);

        Assert.Equal("175", ValueOf(await TxGet(kahuna, reader, b, ct)));
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, reader, c, "275", ct));

        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(reader, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed);
    }

    [Fact]
    public async Task ReadThenWriteOfOneKey_ThenAReadOfAChangedKey_IsServed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string prefix = NewPrefix();
        string a = prefix + "a", b = prefix + "b";
        await AutoCommitSet(kahuna, a, "100", ct);
        await AutoCommitSet(kahuna, b, "100", ct);

        // The shape of a two-row transfer: read a, write a, read b, write b.
        TransactionHandle transfer = await Start(kahuna, prefix + "transfer", ct);
        Assert.Equal("100", ValueOf(await TxGet(kahuna, transfer, a, ct)));
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, transfer, a, "90", ct));

        await AutoCommitSet(kahuna, b, "120", ct);

        // a is this transaction's own staged write over the revision it read: the refresh finds it unchanged.
        Assert.Equal("120", ValueOf(await TxGet(kahuna, transfer, b, ct)));
        Assert.Equal("90", ValueOf(await TxGet(kahuna, transfer, a, ct)));
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, transfer, b, "130", ct));

        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(transfer, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed);
    }

    [Fact]
    public async Task ProbeOfSeveralKeys_AfterOneOfTheEarlierKeysChanged_IsRefused()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string prefix = NewPrefix();
        string a = prefix + "a", b = prefix + "b", c = prefix + "c", missing = prefix + "missing";
        await AutoCommitSet(kahuna, a, "100", ct);
        await AutoCommitSet(kahuna, b, "100", ct);
        await AutoCommitSet(kahuna, c, "7", ct);

        TransactionHandle steady = await Start(kahuna, prefix + "steady", ct);
        KeyValueProbeResult[] first = await TxProbe(kahuna, steady, [a, missing], ct);
        Assert.Equal(KeyValueResponseType.Get, first[0].Type);
        Assert.Equal(KeyValueResponseType.DoesNotExist, first[1].Type);

        // Nothing the transaction read changed: a later probe is served.
        KeyValueProbeResult[] second = await TxProbe(kahuna, steady, [b, c], ct);
        Assert.All(second, result => Assert.Equal(KeyValueResponseType.Get, result.Type));

        TransactionHandle reader = await Start(kahuna, prefix + "reader", ct);
        Assert.Equal(KeyValueResponseType.Get, (await TxProbe(kahuna, reader, [a, missing], ct))[0].Type);

        // The key the reader found missing appears, together with a change of b.
        TransactionHandle writer = await Start(kahuna, prefix + "writer", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, writer, missing, "new", ct));
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, writer, b, "150", ct));
        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(writer, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed);

        KeyValueProbeResult[] refused = await TxProbe(kahuna, reader, [b, c], ct);
        Assert.All(refused, result => Assert.Equal(KeyValueResponseType.Aborted, result.Type));
    }
}
