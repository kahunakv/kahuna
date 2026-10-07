using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A snapshot read at T must answer the same way every time it is asked. A transaction's commit timestamp is
/// frozen before any prepare is sent, so between the freeze and the prepare landing the only thing that makes a
/// snapshot read wait for the staged write is the in-memory write intent the stage planted on the key's leader.
/// If that intent is missing — its lease lapsed, a leader change dropped it, or the stage never planted one — a
/// read at a T above the frozen commit timestamp answers the old value, and the commit then lands inside that
/// snapshot: the same read asked again answers the new value.
///
/// Each test drives the real commit path and reads inside the freeze→prepare window through the finalizer's
/// test hook, because no external caller can time a read into that gap. The assertion is the contract itself:
/// two reads at one snapshot agree.
/// </summary>
public sealed class TestSnapshotReadStagedWriteWindow
{
    private readonly ILoggerFactory loggerFactory;

    public TestSnapshotReadStagedWriteWindow(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, int stagedWriteIntentLeaseMs, CancellationToken ct)
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
            StagedWriteIntentLeaseMs = stagedWriteIntentLeaseMs
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("srw/seed", ct);

        return node;
    }

    private static async Task Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>Two keys on different partitions, so the transaction takes the two-phase commit path.</summary>
    private static (string Contested, string Companion) KeysOnTwoPartitions(EmbeddedKahunaNode node)
    {
        string runId = Guid.NewGuid().ToString("N")[..8];
        string contested = $"srw-{runId}-a/k";
        int contestedPartition = node.Raft.GetPartitionKey(contested);

        // Keys route by their parent bucket, so the candidates vary the bucket, not the leaf.
        for (int i = 0; i < 256; i++)
        {
            string candidate = $"srw-{runId}-b{i}/k";
            if (node.Raft.GetPartitionKey(candidate) != contestedPartition)
                return (contested, candidate);
        }

        throw new InvalidOperationException("no key routes to a second partition");
    }

    /// <summary>
    /// A snapshot read at <paramref name="readTimestamp"/>, retried while the key's leader asks it to wait for a
    /// writer. Answers the response type and, for a hit, the value.
    /// </summary>
    private static async Task<(KeyValueResponseType Type, string? Value)> ReadAt(
        IKahuna kahuna, string key, HLCTimestamp readTimestamp, CancellationToken ct)
    {
        while (true)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, readTimestamp, KeyValueDurability.Persistent, ct);

            if (type is KeyValueResponseType.WaitingForReplication or KeyValueResponseType.MustRetry)
            {
                await Task.Delay(10, ct);
                continue;
            }

            return (type, type == KeyValueResponseType.Get ? Encoding.UTF8.GetString(entry!.Value!) : null);
        }
    }

    /// <summary>
    /// Runs <paramref name="commit"/> with a snapshot read of <paramref name="key"/> started inside the
    /// freeze→prepare window, then reads the same snapshot again and asserts both reads agree. The first read is
    /// not awaited past a short bound inside the window: a read that correctly waits for the staged write can
    /// only finish once the commit it waits for proceeds.
    /// </summary>
    private static async Task<KeyValueResponseType> CommitWithReadInWindow(
        EmbeddedKahunaNode node, string key, Func<Task<KeyValueResponseType>> commit, CancellationToken ct)
    {
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;

        Task<(KeyValueResponseType Type, string? Value)>? firstRead = null;
        HLCTimestamp readTimestamp = HLCTimestamp.Zero;

        finalizer.TestAfterPreValidationHook = async hookCt =>
        {
            finalizer.TestAfterPreValidationHook = null;

            // Minted after the freeze, so the snapshot sits above the frozen commit timestamp.
            readTimestamp = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
            firstRead = ReadAt(node.Kahuna, key, readTimestamp, ct);

            await Task.WhenAny(firstRead, Task.Delay(500, hookCt));
        };

        KeyValueResponseType commitType;
        try
        {
            commitType = await commit();
        }
        finally
        {
            finalizer.TestAfterPreValidationHook = null;
        }

        Assert.NotNull(firstRead);

        (KeyValueResponseType Type, string? Value) first = await firstRead;
        (KeyValueResponseType Type, string? Value) second = await ReadAt(node.Kahuna, key, readTimestamp, ct);

        Assert.True(first == second,
            $"two reads at {readTimestamp} disagree: first {first.Type}/{first.Value}, then {second.Type}/{second.Value} (commit answered {commitType})");

        return commitType;
    }

    private static async Task<TransactionHandle> Start(IKahuna kahuna, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    /// <summary>
    /// The staged set's intent lapses before the commit. A read inside the freeze→prepare window finds no live
    /// intent and answers the old value, so the commit must not land inside that read's snapshot.
    /// </summary>
    [Fact]
    public async Task StagedSet_IntentLeaseLapsed_ReadInWindow_IsRepeatable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // A short staged-write intent lease, so the test can lapse it with a small delay instead of the 15 s
        // production default.
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, stagedWriteIntentLeaseMs: 200, ct);

        (string contested, string companion) = KeysOnTwoPartitions(node);
        await Seed(node.Kahuna, contested, "V1", ct);
        await Seed(node.Kahuna, companion, "W1", ct);

        TransactionHandle tx = await Start(node.Kahuna, contested + "/tx", ct);

        foreach ((string key, string value) in new[] { (contested, "V2"), (companion, "W2") })
        {
            (KeyValueResponseType writeType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0,
                KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, writeType);
        }

        // Let the staged intents lapse, as a paused coordinator's would.
        await Task.Delay(400, ct);

        await CommitWithReadInWindow(node, contested, async () =>
        {
            (KeyValueResponseType commitType, _) = await node.Kahuna.LocateAndCommitTransaction(tx, ct);
            return commitType;
        }, ct);
    }

    /// <summary>
    /// A staged delete, with no lease lapse. A read inside the freeze→prepare window must either wait for the
    /// staged tombstone or answer a value that the commit then does not contradict.
    /// </summary>
    [Fact]
    public async Task StagedDelete_ReadInWindow_IsRepeatable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // A lease far longer than the test, so nothing lapses and the transaction must commit.
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, stagedWriteIntentLeaseMs: 30_000, ct);

        (string contested, string companion) = KeysOnTwoPartitions(node);
        await Seed(node.Kahuna, contested, "V1", ct);
        await Seed(node.Kahuna, companion, "W1", ct);

        TransactionHandle tx = await Start(node.Kahuna, contested + "/tx", ct);

        (KeyValueResponseType deleteType, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(
            tx.TransactionId, contested, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Deleted, deleteType);

        (KeyValueResponseType writeType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, companion, "W2"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
            KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, writeType);

        KeyValueResponseType committed = await CommitWithReadInWindow(node, contested, async () =>
        {
            (KeyValueResponseType commitType, _) = await node.Kahuna.LocateAndCommitTransaction(tx, ct);
            return commitType;
        }, ct);

        // Nothing lapsed, so the transaction itself must still commit.
        Assert.Equal(KeyValueResponseType.Committed, committed);
    }
}
