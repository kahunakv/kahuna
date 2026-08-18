using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The decide-time range-lock fence, driven by real transactions through the durable-intent commit path.
///
/// The interleaving under test is the one the write-time fence cannot catch: the transaction writes a key while
/// no range lock exists (so <c>TrySetHandler</c>'s check passes), another transaction then acquires a range lock
/// covering that key, and only then does the first transaction commit. The acquire deliberately does not conflict
/// with the already-staged write intent — it defers to the commit-time fence — so without that fence the write
/// lands inside a range someone else holds locked.
///
/// <para><b>Ordering is the whole test.</b> Acquiring the lock before the write exercises the write-time fence
/// instead, which has always worked; such a test passes with the gap wide open. Every case below stages the
/// write first.</para>
///
/// <para>Each conflict case asserts the value never became visible, not merely that the commit reported an
/// abort: a lost write would satisfy a status-only assertion just as well as a correct fence.</para>
/// </summary>
public sealed class TestDurableRangeLockWriteFence
{
    private const string KeySpace = "t:drf";
    private const string StartKey = KeySpace + "/10";
    private const string InsideKey = KeySpace + "/25";
    private const string EndKey = KeySpace + "/50";
    private const string OutsideKey = KeySpace + "/99";
    private const int LockExpiresMs = 30_000;

    private readonly ILoggerFactory loggerFactory;

    public TestDurableRangeLockWriteFence(ITestOutputHelper outputHelper)
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
            InitialPartitions = 4
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(InsideKey, ct);

        return node;
    }

    private static async Task<TransactionHandle> StartSession(
        EmbeddedKahunaNode node, string coordinatorKey, KeyValueTransactionLocking locking, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = locking,
                ReadValidation = ReadValidation.TrackAndValidate,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 10_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, type);

        return handle;
    }

    private static async Task WriteInSession(
        EmbeddedKahunaNode node, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>Acquires a range lock over [StartKey, EndKey) on behalf of <paramref name="owner"/>.</summary>
    private static async Task AcquireRangeLock(
        EmbeddedKahunaNode node, HLCTimestamp owner, RangeLockMode mode, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await ((KahunaManager)node.Kahuna).LocateAndTryAcquireRangeLock(
            owner, KeySpace, StartKey, true, EndKey, false, LockExpiresMs,
            KeyValueDurability.Persistent, mode, ct);

        Assert.Equal(KeyValueResponseType.Locked, type);
    }

    /// <summary>A transaction id that belongs to no session — the foreign range-lock holder.</summary>
    private static HLCTimestamp ForeignHolder(EmbeddedKahunaNode node) =>
        node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

    private static async Task AssertAbsent(EmbeddedKahunaNode node, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.DoesNotExist, type);
    }

    private static async Task AssertPresent(EmbeddedKahunaNode node, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal(value, Encoding.UTF8.GetString(entry!.Value!));
    }

    // ── The fence ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Write, then a foreign exclusive range lock lands, then commit — the commit must not land the write.
    /// Run for both locking modes: a pessimistic transaction skips read-set validation entirely, so the fence
    /// has to be independent of it, and the range-lock acquire steps around a held key lock exactly as it does
    /// an optimistic write intent.
    /// </summary>
    [Theory]
    [InlineData(KeyValueTransactionLocking.Optimistic)]
    [InlineData(KeyValueTransactionLocking.Pessimistic)]
    public async Task ExclusiveRangeLockAcquiredAfterTheWrite_AbortsTheCommit(KeyValueTransactionLocking locking)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        TransactionHandle writer = await StartSession(node, $"{KeySpace}-tx/excl-{locking}", locking, ct);

        await WriteInSession(node, writer, InsideKey, "staged", ct);

        // Only now does the lock arrive: at write time there was nothing for the write-time fence to see.
        await AcquireRangeLock(node, ForeignHolder(node), RangeLockMode.Exclusive, ct);

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(writer, ct);

        Assert.Equal(KeyValueResponseType.Aborted, commit);
        await AssertAbsent(node, InsideKey, ct);
    }

    /// <summary>
    /// A shared range lock blocks the write too: the write needs exclusive on [K,K], which is incompatible with
    /// S as well as X. A fence that only looked at exclusive locks would let this one through.
    /// </summary>
    [Fact]
    public async Task SharedRangeLockAcquiredAfterTheWrite_AbortsTheCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        TransactionHandle writer = await StartSession(node, $"{KeySpace}-tx/shared", KeyValueTransactionLocking.Optimistic, ct);

        await WriteInSession(node, writer, InsideKey, "staged", ct);
        await AcquireRangeLock(node, ForeignHolder(node), RangeLockMode.Shared, ct);

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(writer, ct);

        Assert.Equal(KeyValueResponseType.Aborted, commit);
        await AssertAbsent(node, InsideKey, ct);
    }

    /// <summary>
    /// A write outside the locked bounds still commits. Without this the fence could "pass" every case above by
    /// aborting everything.
    /// </summary>
    [Fact]
    public async Task WriteOutsideTheLockedRange_StillCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        TransactionHandle writer = await StartSession(node, $"{KeySpace}-tx/outside", KeyValueTransactionLocking.Optimistic, ct);

        await WriteInSession(node, writer, OutsideKey, "kept", ct);
        await AcquireRangeLock(node, ForeignHolder(node), RangeLockMode.Exclusive, ct);

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(writer, ct);

        Assert.Equal(KeyValueResponseType.Committed, commit);
        await AssertPresent(node, OutsideKey, "kept", ct);
    }

    /// <summary>
    /// The lock's own holder commits its writes inside it. This is the shape a serializable read-write
    /// transaction uses — take a range lock, scan, write inside the range — so fencing the holder against its
    /// own lock would abort every one of them.
    /// </summary>
    [Fact]
    public async Task TransactionHoldingTheRangeLockCommitsItsOwnWrites()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        TransactionHandle writer = await StartSession(node, $"{KeySpace}-tx/self", KeyValueTransactionLocking.Optimistic, ct);

        await WriteInSession(node, writer, InsideKey, "mine", ct);
        await AcquireRangeLock(node, writer.TransactionId, RangeLockMode.Exclusive, ct);

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(writer, ct);

        Assert.Equal(KeyValueResponseType.Committed, commit);
        await AssertPresent(node, InsideKey, "mine", ct);
    }
}
