
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The statement read mode (<see cref="KeyValueReadMode.StatementSnapshot"/>) through the service contract of a
/// single embedded node: a read at a timestamp inside a transaction shows the transaction's own staged writes,
/// serves every other key at the timestamp, neither serves nor checks nor leaves a read pin, and still records
/// what it served when it is registered with the coordinator. The default mode is asserted next to it on every
/// path, so the contract every existing caller relies on is shown unchanged.
/// </summary>
public sealed class TestStatementSnapshotRead
{
    private readonly ILoggerFactory loggerFactory;

    public TestStatementSnapshotRead(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, string keySpace, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableDeferredSettlement = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(keySpace + "/probe", ct);
        return node;
    }

    /// <summary>A key space of its own per test, so a scan over it meets only the test's keys.</summary>
    private static string NewKeySpace() => "stmt/read/" + Guid.NewGuid().ToString("N")[..8];

    private static async Task<TransactionHandle> StartOptimistic(IKahuna kahuna, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Timeout = 10_000, Locking = KeyValueTransactionLocking.Optimistic },
            ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static async Task<KeyValueResponseType> TxSet(IKahuna kahuna, TransactionHandle tx, string key, string value, KeyValueDurability durability, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0,
            durability, ct, coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        return type;
    }

    private static async Task<KeyValueResponseType> TxDelete(IKahuna kahuna, TransactionHandle tx, string key, KeyValueDurability durability, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
            tx.TransactionId, key, durability, ct, coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        return type;
    }

    /// <summary>
    /// A committed write outside any transaction. Returns the stamp the committed head carries, read back from the
    /// leader: a later write on the same actor is strictly above it, so it is a timestamp at which the written value
    /// is the committed state and nothing written afterwards is.
    /// </summary>
    private static async Task<HLCTimestamp> AutoCommitSet(IKahuna kahuna, string key, string value, KeyValueDurability durability, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, durability, ct);
        Assert.Equal(KeyValueResponseType.Set, type);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, durability, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal(value, Text(entry));
        Assert.False(entry!.LastModified.IsNull());
        return entry.LastModified;
    }

    private static Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> Read(
        IKahuna kahuna, HLCTimestamp transactionId, string key, HLCTimestamp readTimestamp, KeyValueReadMode mode, KeyValueDurability durability, CancellationToken ct) =>
        kahuna.LocateAndTryGetValue(transactionId, key, -1, readTimestamp, durability, ct, readMode: mode);

    private static string Text(ReadOnlyKeyValueEntry? entry) => Encoding.UTF8.GetString(entry!.Value!);

    private static async Task<List<(string Key, string Value)>> Scan(
        IKahuna kahuna, HLCTimestamp transactionId, string prefix, HLCTimestamp readTimestamp, KeyValueReadMode mode, KeyValueDurability durability, CancellationToken ct)
    {
        List<(string, string)> rows = [];
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            transactionId, prefix, null, true, null, false, 100, readTimestamp, durability, ct, readMode: mode))
            rows.Add((key, Encoding.UTF8.GetString(entry.Value!)));
        return rows;
    }

    // ── Own staged writes ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The transaction writes a key, then reads it at a timestamp that predates the write. The statement read
    /// shows the staged write; the default as-of read at the same timestamp shows the committed state, as before.
    /// A staged tombstone reads as absent.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task PointRead_StatementMode_ShowsOwnStagedWrite_DefaultModeDoesNot(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;

        string key = space + "/k";
        string gone = space + "/gone";
        HLCTimestamp ts = await AutoCommitSet(kahuna, key, "base", durability, ct);
        await AutoCommitSet(kahuna, gone, "soon-gone", durability, ct);

        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, tx, key, "mine", durability, ct));
        Assert.Equal(KeyValueResponseType.Deleted, await TxDelete(kahuna, tx, gone, durability, ct));

        (KeyValueResponseType statementType, ReadOnlyKeyValueEntry? statement) = await Read(kahuna, tx.TransactionId, key, ts, KeyValueReadMode.StatementSnapshot, durability, ct);
        Assert.Equal(KeyValueResponseType.Get, statementType);
        Assert.Equal("mine", Text(statement));

        (KeyValueResponseType goneType, _) = await Read(kahuna, tx.TransactionId, gone, ts, KeyValueReadMode.StatementSnapshot, durability, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, goneType);

        // The default shape at the same timestamp is the committed state: no own write, as every caller had it.
        (KeyValueResponseType asOfType, ReadOnlyKeyValueEntry? asOf) = await Read(kahuna, tx.TransactionId, key, ts, KeyValueReadMode.Default, durability, ct);
        Assert.Equal(KeyValueResponseType.Get, asOfType);
        Assert.Equal("base", Text(asOf));

        // Another transaction at the same timestamp sees the committed state: the staged write is private.
        TransactionHandle other = await StartOptimistic(kahuna, space + "/other", ct);
        (KeyValueResponseType otherType, ReadOnlyKeyValueEntry? otherEntry) = await Read(kahuna, other.TransactionId, key, ts, KeyValueReadMode.StatementSnapshot, durability, ct);
        Assert.Equal(KeyValueResponseType.Get, otherType);
        Assert.Equal("base", Text(otherEntry));
    }

    /// <summary>
    /// A batched probe of three keys, one of which the transaction wrote: the written key answers the staged
    /// write, the others answer the committed state at the timestamp. The default probe shows no own write.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task ProbeMany_StatementMode_OwnWriteVisible_OtherKeysAtTimestamp(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;

        string[] keys = [space + "/a", space + "/b", space + "/c"];
        HLCTimestamp ts = HLCTimestamp.Zero;
        foreach (string key in keys)
            ts = await AutoCommitSet(kahuna, key, "base-" + key[^1], durability, ct);

        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, tx, keys[1], "mine", durability, ct));

        KeyValueProbeResult[] statement = await kahuna.LocateAndTryProbeMany(
            tx.TransactionId, ts, keys, KeyValueProbeLock.None, 0, durability, ct, readMode: KeyValueReadMode.StatementSnapshot);

        Assert.Equal(3, statement.Length);
        Assert.All(statement, r => Assert.Equal(KeyValueResponseType.Get, r.Type));
        Assert.Equal("base-a", Text(statement[0].Entry));
        Assert.Equal("mine", Text(statement[1].Entry));
        Assert.Equal("base-c", Text(statement[2].Entry));

        KeyValueProbeResult[] asOf = await kahuna.LocateAndTryProbeMany(
            tx.TransactionId, ts, keys, KeyValueProbeLock.None, 0, durability, ct);

        Assert.All(asOf, r => Assert.Equal(KeyValueResponseType.Get, r.Type));
        Assert.Equal("base-b", Text(asOf[1].Entry));
    }

    /// <summary>
    /// A scan and a range probe at a timestamp before the transaction's write include the key it staged, with
    /// the staged value. The default as-of scan showed own staged writes before the mode existed (unlike the
    /// default as-of point read), and still does: the modes differ on a scan only in how an earlier pin is treated.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task Scan_StatementMode_IncludesOwnStagedWrite(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;
        string prefix = space + "/";

        await AutoCommitSet(kahuna, prefix + "r1", "one", durability, ct);
        HLCTimestamp ts = await AutoCommitSet(kahuna, prefix + "r2", "two", durability, ct);

        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, tx, prefix + "r2", "two-mine", durability, ct));
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, tx, prefix + "r3", "three-mine", durability, ct));

        List<(string Key, string Value)> statement = await Scan(kahuna, tx.TransactionId, prefix, ts, KeyValueReadMode.StatementSnapshot, durability, ct);
        Assert.Equal([(prefix + "r1", "one"), (prefix + "r2", "two-mine"), (prefix + "r3", "three-mine")], statement);

        List<(string Key, string Value)> asOf = await Scan(kahuna, tx.TransactionId, prefix, ts, KeyValueReadMode.Default, durability, ct);
        Assert.Equal(statement, asOf);

        // Another transaction at the same timestamp sees only the committed state: the staged writes are private.
        TransactionHandle other = await StartOptimistic(kahuna, space + "/other", ct);
        List<(string Key, string Value)> foreign = await Scan(kahuna, other.TransactionId, prefix, ts, KeyValueReadMode.StatementSnapshot, durability, ct);
        Assert.Equal([(prefix + "r1", "one"), (prefix + "r2", "two")], foreign);

        KeyValueProbeRangeResult[] probed = await kahuna.LocateAndTryProbeManyRanges(
            tx.TransactionId, ts, prefix, [new KeyValueProbeRange(null, true, null, false, 10)],
            KeyValueProbeLock.None, 0, durability, ct, readMode: KeyValueReadMode.StatementSnapshot);

        Assert.Single(probed);
        Assert.Equal(KeyValueResponseType.Get, probed[0].Type);
        Assert.Equal([prefix + "r1", prefix + "r2", prefix + "r3"], probed[0].Items.Select(i => i.Item1));
        Assert.Equal("two-mine", Text(probed[0].Items[1].Item2));
    }

    // ── Earlier reads do not pin ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The transaction read a key in the default mode, which pinned the committed state, and another writer then
    /// committed a new value. A statement read at a later timestamp sees the new value and is not aborted; the
    /// pin is left as it was, so the default latest read still answers <c>Aborted</c>.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task PointRead_StatementMode_DoesNotServeOrCheckAnEarlierPin(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;
        string key = space + "/k";

        await AutoCommitSet(kahuna, key, "v1", durability, ct);

        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);
        (KeyValueResponseType pinType, ReadOnlyKeyValueEntry? pinned) = await Read(kahuna, tx.TransactionId, key, HLCTimestamp.Zero, KeyValueReadMode.Default, durability, ct);
        Assert.Equal(KeyValueResponseType.Get, pinType);
        Assert.Equal("v1", Text(pinned));

        HLCTimestamp later = await AutoCommitSet(kahuna, key, "v2", durability, ct);

        (KeyValueResponseType statementType, ReadOnlyKeyValueEntry? statement) = await Read(kahuna, tx.TransactionId, key, later, KeyValueReadMode.StatementSnapshot, durability, ct);
        Assert.Equal(KeyValueResponseType.Get, statementType);
        Assert.Equal("v2", Text(statement));

        // The default latest read is held to its pin, exactly as before.
        (KeyValueResponseType latestType, _) = await Read(kahuna, tx.TransactionId, key, HLCTimestamp.Zero, KeyValueReadMode.Default, durability, ct);
        Assert.Equal(KeyValueResponseType.Aborted, latestType);
    }

    /// <summary>
    /// A statement read leaves no pin behind: after it, another writer commits the key, and the transaction's
    /// default latest read of the key answers the new value instead of <c>Aborted</c>. A default latest read in
    /// its place would have pinned, and the same sequence would then abort (see the early write-conflict tests).
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task PointRead_StatementMode_LeavesNoPin(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;
        string key = space + "/k";

        HLCTimestamp ts = await AutoCommitSet(kahuna, key, "v1", durability, ct);

        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);
        (KeyValueResponseType statementType, ReadOnlyKeyValueEntry? statement) = await Read(kahuna, tx.TransactionId, key, ts, KeyValueReadMode.StatementSnapshot, durability, ct);
        Assert.Equal(KeyValueResponseType.Get, statementType);
        Assert.Equal("v1", Text(statement));

        await AutoCommitSet(kahuna, key, "v2", durability, ct);

        (KeyValueResponseType latestType, ReadOnlyKeyValueEntry? latest) = await Read(kahuna, tx.TransactionId, key, HLCTimestamp.Zero, KeyValueReadMode.Default, durability, ct);
        Assert.Equal(KeyValueResponseType.Get, latestType);
        Assert.Equal("v2", Text(latest));
    }

    /// <summary>
    /// The scan and the range probe: a default scan pinned every key it returned, another writer committed one of
    /// them, and a statement scan at a later timestamp returns the new value with no <c>Aborted</c>. The default
    /// scan over the same keys still answers <c>Aborted</c>.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task Scan_StatementMode_IgnoresEarlierPins_DefaultScanStillAborts(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;
        string prefix = space + "/";

        await AutoCommitSet(kahuna, prefix + "r1", "one", durability, ct);
        await AutoCommitSet(kahuna, prefix + "r2", "two", durability, ct);

        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);
        List<(string Key, string Value)> pinned = await Scan(kahuna, tx.TransactionId, prefix, HLCTimestamp.Zero, KeyValueReadMode.Default, durability, ct);
        Assert.Equal([(prefix + "r1", "one"), (prefix + "r2", "two")], pinned);

        HLCTimestamp later = await AutoCommitSet(kahuna, prefix + "r2", "two-b", durability, ct);

        List<(string Key, string Value)> statement = await Scan(kahuna, tx.TransactionId, prefix, later, KeyValueReadMode.StatementSnapshot, durability, ct);
        Assert.Equal([(prefix + "r1", "one"), (prefix + "r2", "two-b")], statement);

        KeyValueProbeRangeResult[] probed = await kahuna.LocateAndTryProbeManyRanges(
            tx.TransactionId, later, prefix, [new KeyValueProbeRange(null, true, null, false, 10)],
            KeyValueProbeLock.None, 0, durability, ct, readMode: KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.Get, probed[0].Type);
        Assert.Equal("two-b", Text(probed[0].Items[1].Item2));

        // The default latest page is held to its pins, exactly as before.
        KeyValueGetByRangeResult latest = await kahuna.LocateAndGetByRange(
            tx.TransactionId, prefix, null, true, null, false, 10, HLCTimestamp.Zero, durability, ct);
        Assert.Equal(KeyValueResponseType.Aborted, latest.Type);

        KeyValueProbeRangeResult[] latestProbe = await kahuna.LocateAndTryProbeManyRanges(
            tx.TransactionId, HLCTimestamp.Zero, prefix, [new KeyValueProbeRange(null, true, null, false, 10)],
            KeyValueProbeLock.None, 0, durability, ct);
        Assert.Equal(KeyValueResponseType.Aborted, latestProbe[0].Type);
    }

    // ── Registration and commit validation ────────────────────────────────────────────────────

    /// <summary>
    /// A registered statement read records the revision it served, so a foreign commit on that key after the
    /// read refuses the transaction's commit. A registered default as-of read records nothing, and the same
    /// sequence commits.
    /// </summary>
    [Theory]
    [InlineData(KeyValueReadMode.StatementSnapshot, false)]
    [InlineData(KeyValueReadMode.Default, true)]
    public async Task RegisteredRead_StatementModeRecordsObservation_ForeignCommitRefusesCommit(KeyValueReadMode mode, bool commits)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;
        string key = space + "/k";
        string other = space + "/j";

        HLCTimestamp ts = await AutoCommitSet(kahuna, key, "v1", KeyValueDurability.Persistent, ct);

        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);
        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? read) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1, ts, KeyValueDurability.Persistent, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom(), mode);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal("v1", Text(read));

        TransactionWorkingSet? workingSet = await kahuna.LocateAndGetTransactionWorkingSet(tx.CoordinatorKey, tx.TransactionId, ct);
        Assert.NotNull(workingSet);

        if (mode == KeyValueReadMode.StatementSnapshot)
        {
            KeyValueTransactionReadKey observed = Assert.Single(workingSet!.ReadKeys, r => r.Key == key);
            Assert.True(observed.Exists);
            Assert.Equal(read!.Revision, observed.Revision);
        }
        else
            Assert.DoesNotContain(workingSet!.ReadKeys, r => r.Key == key);

        await AutoCommitSet(kahuna, key, "v2", KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, tx, other, "written", KeyValueDurability.Persistent, ct));

        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(tx, ct);
        Assert.Equal(commits ? KeyValueResponseType.Committed : KeyValueResponseType.Aborted, commit);
    }

    /// <summary>
    /// A registered statement read of a key the transaction itself wrote serves the staged write and records it,
    /// as a registered latest read does. The key is validated as a write, not as a read dependency, and the
    /// observation matches the staging the coordinator confirmed, so the commit goes through.
    /// </summary>
    [Fact]
    public async Task RegisteredStatementRead_OfOwnStagedWrite_DoesNotEnterTheReadSet_AndCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;
        string key = space + "/k";

        HLCTimestamp ts = await AutoCommitSet(kahuna, key, "v1", KeyValueDurability.Persistent, ct);

        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, tx, key, "mine", KeyValueDurability.Persistent, ct));

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? read) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1, ts, KeyValueDurability.Persistent, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom(), KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal("mine", Text(read));

        TransactionWorkingSet? workingSet = await kahuna.LocateAndGetTransactionWorkingSet(tx.CoordinatorKey, tx.TransactionId, ct);
        Assert.NotNull(workingSet);
        Assert.DoesNotContain(workingSet!.ReadKeys, r => r.Key == key);
        Assert.Contains(workingSet.ModifiedKeys, m => m.Key == key);

        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(tx, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);

        (KeyValueResponseType afterType, ReadOnlyKeyValueEntry? after) = await Read(kahuna, HLCTimestamp.Zero, key, HLCTimestamp.Zero, KeyValueReadMode.Default, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, afterType);
        Assert.Equal("mine", Text(after));
    }

    /// <summary>
    /// A registered statement probe and a registered statement scan fold the rows they served, as the point read
    /// does, so commit validation covers them.
    /// </summary>
    [Fact]
    public async Task RegisteredProbeAndScan_StatementMode_RecordObservations()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;
        string prefix = space + "/";

        await AutoCommitSet(kahuna, prefix + "p1", "one", KeyValueDurability.Persistent, ct);
        HLCTimestamp ts = await AutoCommitSet(kahuna, prefix + "p2", "two", KeyValueDurability.Persistent, ct);

        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);

        KeyValueProbeResult[] probed = await kahuna.LocateAndTryProbeMany(
            tx.TransactionId, ts, [prefix + "p1"], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct,
            tx.CoordinatorKey, TransactionOperationId.NewRandom(), KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.Get, probed[0].Type);

        List<(string Key, string Value)> scanned = [];
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId, prefix, prefix + "p2", true, null, false, 10, ts, KeyValueDurability.Persistent, ct,
            tx.CoordinatorKey, TransactionOperationId.NewRandom(), readMode: KeyValueReadMode.StatementSnapshot))
            scanned.Add((key, Encoding.UTF8.GetString(entry.Value!)));
        Assert.Equal([(prefix + "p2", "two")], scanned);

        TransactionWorkingSet? workingSet = await kahuna.LocateAndGetTransactionWorkingSet(tx.CoordinatorKey, tx.TransactionId, ct);
        Assert.NotNull(workingSet);
        Assert.Contains(workingSet!.ReadKeys, r => r.Key == prefix + "p1");
        Assert.Contains(workingSet.ReadKeys, r => r.Key == prefix + "p2");
    }

    // ── Shape rules ───────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A statement read must carry a read timestamp: without one it would silently become a pinning latest read.
    /// Every read path refuses it as malformed before anything is routed.
    /// </summary>
    [Fact]
    public async Task StatementMode_WithoutReadTimestamp_IsInvalidInput_OnEveryPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;
        string prefix = space + "/";
        string key = prefix + "k";

        await AutoCommitSet(kahuna, key, "v1", KeyValueDurability.Persistent, ct);
        TransactionHandle tx = await StartOptimistic(kahuna, space, ct);

        (KeyValueResponseType getType, _) = await Read(kahuna, tx.TransactionId, key, HLCTimestamp.Zero, KeyValueReadMode.StatementSnapshot, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, getType);

        KeyValueProbeResult[] probed = await kahuna.LocateAndTryProbeMany(
            tx.TransactionId, HLCTimestamp.Zero, [key], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct, readMode: KeyValueReadMode.StatementSnapshot);
        Assert.All(probed, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));

        KeyValueGetByRangeResult page = await kahuna.LocateAndGetByRange(
            tx.TransactionId, prefix, null, true, null, false, 10, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, readMode: KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.InvalidInput, page.Type);

        KeyValueProbeRangeResult[] seeks = await kahuna.LocateAndTryProbeManyRanges(
            tx.TransactionId, HLCTimestamp.Zero, prefix, [new KeyValueProbeRange(null, true, null, false, 10)],
            KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct, readMode: KeyValueReadMode.StatementSnapshot);
        Assert.All(seeks, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));

        KahunaServerException refused = await Assert.ThrowsAsync<KahunaServerException>(async () =>
        {
            await foreach ((string _, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                tx.TransactionId, prefix, null, true, null, false, 10, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, readMode: KeyValueReadMode.StatementSnapshot))
            {
            }
        });
        Assert.Equal(KeyValueResponseType.InvalidInput, refused.ResponseType);

        // The transaction is still usable: the refusals touched nothing.
        (KeyValueResponseType latestType, ReadOnlyKeyValueEntry? latest) = await Read(kahuna, tx.TransactionId, key, HLCTimestamp.Zero, KeyValueReadMode.Default, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, latestType);
        Assert.Equal("v1", Text(latest));
    }

    /// <summary>
    /// Outside a transaction the mode changes nothing: there are no own writes and no pins, so a statement read
    /// at a timestamp is the as-of read at that timestamp.
    /// </summary>
    [Fact]
    public async Task StatementMode_WithoutTransaction_IsTheAsOfRead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = NewKeySpace();
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, space, ct);
        IKahuna kahuna = node.Kahuna;
        string key = space + "/k";

        HLCTimestamp ts = await AutoCommitSet(kahuna, key, "v1", KeyValueDurability.Persistent, ct);
        await AutoCommitSet(kahuna, key, "v2", KeyValueDurability.Persistent, ct);

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await Read(kahuna, HLCTimestamp.Zero, key, ts, KeyValueReadMode.StatementSnapshot, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("v1", Text(entry));
    }
}
