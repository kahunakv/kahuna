using System.Diagnostics;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A registered read whose coordinator completion does not land — the routing answered <c>MustRetry</c>
/// without throwing, as it does across a coordinator-partition leader change — must not hand the value back
/// as a success. Doing so leaves the operation record pending with nobody left to complete it: every commit
/// and rollback of the transaction then waits out the whole finalize drain deadline and answers
/// <c>MustRetry</c>, and a same-id retry only sees <c>AlreadyPending</c> and gives up. These tests drive the
/// four registered read shapes (point read, many-read, bucket scan, range scan) through that path on one
/// embedded node and assert the contract the lock and write wrappers already honor: the call answers
/// <c>MustRetry</c>, the record stays pending exactly until a same-id retry recovers it, the recovered read
/// folds its observation exactly once (never for a snapshot read), and one finalize call then reaches a
/// terminal answer without waiting on the drain.
/// </summary>
public sealed class TestReadCompletionNotAcknowledged
{
    private const string Prefix = "rdack";

    // Well under the default finalize drain deadline (the session timeout, 5 s by default): a finalize that
    // waits on a stranded record takes the whole deadline and answers MustRetry, so a terminal answer inside
    // this bound proves the drain saw nothing pending.
    private const int FinalizeBoundMs = 4_000;

    private readonly ILoggerFactory loggerFactory;

    public TestReadCompletionNotAcknowledged(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>Refuses the first completion routed for one operation id and lets every later one land.</summary>
    private sealed class OneShotRefusal(TransactionOperationId target)
    {
        public int Fired { get; private set; }

        public bool Refuse(HLCTimestamp _, TransactionOperationId operationId)
        {
            if (operationId != target || Fired > 0)
                return false;

            Fired++;
            return true;
        }
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaOptions options = new()
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        };

        EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(Prefix + "/k00", ct);
        return node;
    }

    private static async Task Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType set, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, System.Text.Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, set);
    }

    private static async Task<TransactionHandle> StartTx(IKahuna kahuna, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) =
            await kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.False(handle.IsEmpty);
        return handle;
    }

    private static async Task<TransactionWorkingSet> WorkingSet(IKahuna kahuna, TransactionHandle handle, CancellationToken ct)
    {
        TransactionWorkingSet? ws = await kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
        Assert.NotNull(ws);
        return ws!;
    }

    private static OneShotRefusal ArmRefusal(EmbeddedKahunaNode node, TransactionOperationId operationId)
    {
        OneShotRefusal refusal = new(operationId);
        ((KahunaManager)node.Kahuna).KeyValues.Registrar.TestCompletionRefusal = refusal.Refuse;
        return refusal;
    }

    private static async Task AssertRollbackFinalizesPromptly(IKahuna kahuna, TransactionHandle handle, CancellationToken ct)
    {
        Stopwatch clock = Stopwatch.StartNew();
        KeyValueResponseType rolledBack = await kahuna.LocateAndRollbackTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.RolledBack, rolledBack);
        Assert.True(clock.ElapsedMilliseconds < FinalizeBoundMs, $"rollback waited on the finalize drain: {clock.ElapsedMilliseconds} ms");
    }

    private static async Task AssertCommitFinalizesPromptly(IKahuna kahuna, TransactionHandle handle, CancellationToken ct)
    {
        Stopwatch clock = Stopwatch.StartNew();
        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed);
        Assert.True(clock.ElapsedMilliseconds < FinalizeBoundMs, $"commit waited on the finalize drain: {clock.ElapsedMilliseconds} ms");
    }

    // ── Point read ─────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A latest point read whose completion is refused answers <c>MustRetry</c> and leaves its record pending
    /// with no observation folded. The same-id retry recovers: it returns the value, folds the observation,
    /// and drains the record, so one rollback finalizes immediately.
    /// </summary>
    [Fact]
    public async Task PointRead_CompletionNotAcknowledged_AnswersMustRetry_ThenSameIdRetryRecoversAndRollbackFinalizes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = Prefix + "/point";
        await Seed(node.Kahuna, key, "v1", ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        OneShotRefusal refusal = ArmRefusal(node, op);

        (KeyValueResponseType refused, ReadOnlyKeyValueEntry? refusedEntry) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);

        Assert.Equal(1, refusal.Fired);
        Assert.Equal(KeyValueResponseType.MustRetry, refused);
        Assert.Null(refusedEntry);

        TransactionWorkingSet pending = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(1, pending.PendingOperationCount);
        Assert.DoesNotContain(pending.ReadKeys, r => r.Key == key);

        (KeyValueResponseType recovered, ReadOnlyKeyValueEntry? recoveredEntry) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);

        Assert.Equal(1, refusal.Fired);
        Assert.Equal(KeyValueResponseType.Get, recovered);
        Assert.NotNull(recoveredEntry);
        Assert.Equal("v1"u8.ToArray(), recoveredEntry!.Value);

        TransactionWorkingSet drained = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(0, drained.PendingOperationCount);
        KeyValueTransactionReadKey observed = Assert.Single(drained.ReadKeys, r => r.Key == key);
        Assert.True(observed.Exists);
        Assert.Equal(recoveredEntry.Revision, observed.Revision);

        await AssertRollbackFinalizesPromptly(node.Kahuna, handle, ct);
    }

    /// <summary>
    /// A snapshot point read (pinned read timestamp) owns no live read dependency: it folds nothing on its
    /// first completion and nothing on the recovered one, but it still drains the record so a commit reaches
    /// a terminal answer.
    /// </summary>
    [Fact]
    public async Task SnapshotPointRead_CompletionNotAcknowledged_RecoversWithoutFoldingAndCommitFinalizes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string key = Prefix + "/snapshot";
        await Seed(node.Kahuna, key, "v1", ct);
        HLCTimestamp snapshot = manager.Raft.HybridLogicalClock.TrySendOrLocalEvent(manager.Raft.GetLocalNodeId());

        // The answer a plain snapshot read gives at this timestamp is what the recovered read must give too.
        (KeyValueResponseType expected, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        OneShotRefusal refusal = ArmRefusal(node, op);

        (KeyValueResponseType refused, _) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, snapshot, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);

        Assert.Equal(1, refusal.Fired);
        Assert.Equal(KeyValueResponseType.MustRetry, refused);

        TransactionWorkingSet pending = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(1, pending.PendingOperationCount);
        Assert.Empty(pending.ReadKeys);

        (KeyValueResponseType recovered, _) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, snapshot, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);

        Assert.Equal(expected, recovered);

        TransactionWorkingSet drained = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(0, drained.PendingOperationCount);
        Assert.Empty(drained.ReadKeys);

        await AssertCommitFinalizesPromptly(node.Kahuna, handle, ct);
    }

    // ── Many-read ──────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A registered batch read whose completion is refused answers <c>MustRetry</c> for every key and folds
    /// nothing; the same-id retry returns every key, folds one observation per key, drains the record, and a
    /// commit then finalizes immediately.
    /// </summary>
    [Fact]
    public async Task ManyRead_CompletionNotAcknowledged_AnswersMustRetryPerKey_ThenSameIdRetryRecoversAndCommitFinalizes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string k1 = Prefix + "/many-1";
        string k2 = Prefix + "/many-2";
        await Seed(node.Kahuna, k1, "v1", ct);
        await Seed(node.Kahuna, k2, "v2", ct);

        List<(string key, long revision, KeyValueDurability durability)> keys =
            [(k1, -1, KeyValueDurability.Persistent), (k2, -1, KeyValueDurability.Persistent)];

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        OneShotRefusal refusal = ArmRefusal(node, op);

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> refused =
            await node.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, op);

        Assert.Equal(1, refusal.Fired);
        Assert.Equal(2, refused.Count);
        Assert.All(refused, item => Assert.Equal(KeyValueResponseType.MustRetry, item.Item1));

        TransactionWorkingSet pending = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(1, pending.PendingOperationCount);
        Assert.Empty(pending.ReadKeys);

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> recovered =
            await node.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, op);

        Assert.Equal(2, recovered.Count);
        Assert.All(recovered, item => Assert.Equal(KeyValueResponseType.Get, item.Item1));
        Assert.Contains(recovered, item => item.Item2 == k1 && item.Item4 is not null && item.Item4.Value.AsSpan().SequenceEqual("v1"u8));
        Assert.Contains(recovered, item => item.Item2 == k2 && item.Item4 is not null && item.Item4.Value.AsSpan().SequenceEqual("v2"u8));

        TransactionWorkingSet drained = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(0, drained.PendingOperationCount);
        Assert.Equal(2, drained.ReadKeys.Count);
        Assert.Contains(drained.ReadKeys, r => r.Key == k1 && r.Exists);
        Assert.Contains(drained.ReadKeys, r => r.Key == k2 && r.Exists);

        await AssertCommitFinalizesPromptly(node.Kahuna, handle, ct);
    }

    // ── Bucket scan ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A registered bucket scan whose completion is refused answers <c>MustRetry</c> with no items and folds
    /// nothing; the same-id retry returns the items, folds one observation per item, drains the record, and a
    /// rollback then finalizes immediately.
    /// </summary>
    [Fact]
    public async Task BucketScan_CompletionNotAcknowledged_AnswersMustRetry_ThenSameIdRetryRecoversAndRollbackFinalizes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        const string bucket = Prefix + "-bucket";
        string k1 = bucket + "/one";
        string k2 = bucket + "/two";
        await Seed(node.Kahuna, k1, "v1", ct);
        await Seed(node.Kahuna, k2, "v2", ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        OneShotRefusal refusal = ArmRefusal(node, op);

        KeyValueGetByBucketResult refused = await node.Kahuna.LocateAndGetByBucket(
            handle.TransactionId, bucket, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);

        Assert.Equal(1, refusal.Fired);
        Assert.Equal(KeyValueResponseType.MustRetry, refused.Type);
        Assert.Empty(refused.Items);

        TransactionWorkingSet pending = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(1, pending.PendingOperationCount);
        Assert.Empty(pending.ReadKeys);

        KeyValueGetByBucketResult recovered = await node.Kahuna.LocateAndGetByBucket(
            handle.TransactionId, bucket, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);

        Assert.Equal(KeyValueResponseType.Get, recovered.Type);
        Assert.Equal(2, recovered.Items.Count);

        TransactionWorkingSet drained = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(0, drained.PendingOperationCount);
        Assert.Equal(2, drained.ReadKeys.Count);
        Assert.Contains(drained.ReadKeys, r => r.Key == k1);
        Assert.Contains(drained.ReadKeys, r => r.Key == k2);

        await AssertRollbackFinalizesPromptly(node.Kahuna, handle, ct);
    }

    // ── Range scan ─────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A registered range scan page whose completion is refused answers <c>MustRetry</c> with no items and
    /// folds nothing; the same-id retry returns the page, folds one observation per item, drains the record,
    /// and a commit then finalizes immediately.
    /// </summary>
    [Fact]
    public async Task RangeScan_CompletionNotAcknowledged_AnswersMustRetry_ThenSameIdRetryRecoversAndCommitFinalizes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        const string range = Prefix + "-range";
        string k1 = range + "/k01";
        string k2 = range + "/k02";
        await Seed(node.Kahuna, k1, "v1", ct);
        await Seed(node.Kahuna, k2, "v2", ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        OneShotRefusal refusal = ArmRefusal(node, op);

        KeyValueGetByRangeResult refused = await node.Kahuna.LocateAndGetByRange(
            handle.TransactionId, range, null, true, null, false, 10, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            handle.CoordinatorKey, op);

        Assert.Equal(1, refusal.Fired);
        Assert.Equal(KeyValueResponseType.MustRetry, refused.Type);
        Assert.Empty(refused.Items);

        TransactionWorkingSet pending = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(1, pending.PendingOperationCount);
        Assert.Empty(pending.ReadKeys);

        KeyValueGetByRangeResult recovered = await node.Kahuna.LocateAndGetByRange(
            handle.TransactionId, range, null, true, null, false, 10, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            handle.CoordinatorKey, op);

        Assert.Equal(KeyValueResponseType.Get, recovered.Type);
        Assert.Equal(2, recovered.Items.Count);

        TransactionWorkingSet drained = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(0, drained.PendingOperationCount);
        Assert.Equal(2, drained.ReadKeys.Count);
        Assert.Contains(drained.ReadKeys, r => r.Key == k1);
        Assert.Contains(drained.ReadKeys, r => r.Key == k2);

        await AssertCommitFinalizesPromptly(node.Kahuna, handle, ct);
    }
}
