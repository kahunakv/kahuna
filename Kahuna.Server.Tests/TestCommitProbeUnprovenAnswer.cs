
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
/// The commit probe passes a key only on its clean answer. A leader that cannot confirm its leadership for a
/// probed key's partition — a failed read-index, or a partition gated as incomplete — answers MustRetry, which
/// proves nothing: not that a read key is free of a concurrent writer, not that a written key's staged intent
/// still holds, and it reports no leadership term for the one-phase bundle to be fenced to. Such a commit must be
/// refused as MustRetry: never read as clean and committed, and never reported as a conflict.
///
/// Each test stages through the real transaction paths, gates the probed partition on the node that leads it
/// (the window a real detection opens too: the gate stands at once, the relinquish runs detached), and asserts
/// that the commit answers MustRetry. The gate is then cleared and the same transaction, or a re-run of the
/// script, commits — which proves the refusal was retryable and left the staged writes in place.
/// </summary>
public sealed class TestCommitProbeUnprovenAnswer
{
    private readonly ILoggerFactory loggerFactory;

    public TestCommitProbeUnprovenAnswer(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct, int stagedWriteIntentLeaseMs = 30_000)
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

            // By default a lease far longer than the test, so nothing lapses: every refusal is the probe's own.
            StagedWriteIntentLeaseMs = stagedWriteIntentLeaseMs
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("cpu/seed", ct);

        return node;
    }

    private static async Task Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>Two keys on different partitions, as the node routes them.</summary>
    private static (string First, string Second) KeysOnTwoPartitions(EmbeddedKahunaNode node)
    {
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string runId = Guid.NewGuid().ToString("N")[..8];
        string first = $"cpu-{runId}-a/k";
        int firstPartition = manager.KeyValues.RouteKey(first);

        // Keys route by their parent bucket, so the candidates vary the bucket, not the leaf.
        for (int i = 0; i < 256; i++)
        {
            string candidate = $"cpu-{runId}-b{i}/k";
            if (manager.KeyValues.RouteKey(candidate) != firstPartition)
                return (first, candidate);
        }

        throw new InvalidOperationException("no key routes to a second partition");
    }

    /// <summary>Gates the partition the key routes to on the node, which leads it. Dispose clears the gate.</summary>
    private static IDisposable Gate(EmbeddedKahunaNode node, string key)
    {
        KahunaManager manager = (KahunaManager)node.Kahuna;
        return manager.KeyValues.DivergenceContainment.GateForTests(manager.KeyValues.RouteKey(key));
    }

    private static async Task<(KeyValueResponseType Type, string? Value)> Read(IKahuna kahuna, string key, CancellationToken ct)
    {
        while (true)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            if (type is KeyValueResponseType.WaitingForReplication or KeyValueResponseType.MustRetry)
            {
                await Task.Delay(10, ct);
                continue;
            }

            return (type, type == KeyValueResponseType.Get ? Encoding.UTF8.GetString(entry!.Value!) : null);
        }
    }

    private static async Task<TransactionHandle> Start(IKahuna kahuna, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static async Task Stage(IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0,
            KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    private static async Task<KeyValueResponseType> Commit(IKahuna kahuna, TransactionHandle tx, CancellationToken ct)
    {
        (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(tx, ct);
        return commitType;
    }

    /// <summary>
    /// Two written keys on two partitions, so the commit takes the two-phase flow and probes after its prepares
    /// are durable. The written key's partition is gated when the probe runs: its own-staged-intent check cannot
    /// be answered, so the commit must answer MustRetry, not Committed. Once the gate is cleared the same session
    /// commits, reusing the prepares it left installed.
    /// </summary>
    [Fact]
    public async Task TwoPhase_WrittenKeyPartitionGatedAtTheProbe_AnswersMustRetry_AndCommitsOnceClear()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        (string gatedKey, string companion) = KeysOnTwoPartitions(node);
        await Seed(node.Kahuna, gatedKey, "V1", ct);
        await Seed(node.Kahuna, companion, "W1", ct);

        TransactionHandle tx = await Start(node.Kahuna, gatedKey + "/tx", ct);
        await Stage(node.Kahuna, tx, gatedKey, "V2", ct);
        await Stage(node.Kahuna, tx, companion, "W2", ct);

        long unprovenBefore = DurableTransactionMetrics.CommitProbesUnprovenCount;

        using (Gate(node, gatedKey))
            Assert.Equal(KeyValueResponseType.MustRetry, await Commit(node.Kahuna, tx, ct));

        Assert.True(DurableTransactionMetrics.CommitProbesUnprovenCount > unprovenBefore, "the refusal was not the probe's");

        Assert.Equal(KeyValueResponseType.Committed, await Commit(node.Kahuna, tx, ct));

        Assert.Equal((KeyValueResponseType.Get, "V2"), await Read(node.Kahuna, gatedKey, ct));
        Assert.Equal((KeyValueResponseType.Get, "W2"), await Read(node.Kahuna, companion, ct));
    }

    /// <summary>
    /// One written key, so the commit takes the one-phase bundle, which validates before it proposes. The
    /// anchor partition is gated when the probe runs: the leader cannot confirm its leadership for it, so the
    /// probe reports neither a held intent nor a term to fence the bundle to. The bundle must not be proposed —
    /// the hook that runs after a passed validation never fires and no record is written — and the commit must
    /// answer MustRetry. Once the gate is cleared the same session commits through the bundle.
    /// </summary>
    [Fact]
    public async Task OnePhase_AnchorPartitionGatedAtTheProbe_DoesNotProposeTheBundle_AndCommitsOnceClear()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;

        string key = $"cpu-{Guid.NewGuid():N}-1p/k";
        await Seed(node.Kahuna, key, "V1", ct);

        TransactionHandle tx = await Start(node.Kahuna, key + "/tx", ct);
        await Stage(node.Kahuna, tx, key, "V2", ct);

        int validationsPassed = 0;
        finalizer.TestAfterReadSetValidationHook = _ =>
        {
            Interlocked.Increment(ref validationsPassed);
            return Task.CompletedTask;
        };

        try
        {
            long unprovenBefore = DurableTransactionMetrics.CommitProbesUnprovenCount;

            using (Gate(node, key))
                Assert.Equal(KeyValueResponseType.MustRetry, await Commit(node.Kahuna, tx, ct));

            Assert.True(DurableTransactionMetrics.CommitProbesUnprovenCount > unprovenBefore, "the refusal was not the probe's");
            Assert.Equal(0, validationsPassed);
            Assert.Equal(0, manager.DurableTransactionRecordStore.Count);

            Assert.Equal(KeyValueResponseType.Committed, await Commit(node.Kahuna, tx, ct));
            Assert.Equal(1, validationsPassed);
        }
        finally
        {
            finalizer.TestAfterReadSetValidationHook = null;
        }

        Assert.Equal((KeyValueResponseType.Get, "V2"), await Read(node.Kahuna, key, ct));
    }

    /// <summary>
    /// An optimistic transaction reads one key and writes another, on different partitions. The read key's
    /// partition is gated when the probe runs, so its write-skew check — is there a concurrent writer? — cannot be
    /// answered. The commit must answer MustRetry rather than treat the read as validated. Once the gate is
    /// cleared the same session commits.
    /// </summary>
    [Fact]
    public async Task ReadKeyPartitionGatedAtTheProbe_AnswersMustRetry_AndCommitsOnceClear()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        (string readKey, string writtenKey) = KeysOnTwoPartitions(node);
        await Seed(node.Kahuna, readKey, "R1", ct);
        await Seed(node.Kahuna, writtenKey, "V1", ct);

        TransactionHandle tx = await Start(node.Kahuna, writtenKey + "/tx", ct);

        (KeyValueResponseType readType, _) = await node.Kahuna.LocateAndTryGetValue(
            tx.TransactionId, readKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, readType);

        await Stage(node.Kahuna, tx, writtenKey, "V2", ct);

        long unprovenBefore = DurableTransactionMetrics.CommitProbesUnprovenCount;

        using (Gate(node, readKey))
            Assert.Equal(KeyValueResponseType.MustRetry, await Commit(node.Kahuna, tx, ct));

        Assert.True(DurableTransactionMetrics.CommitProbesUnprovenCount > unprovenBefore, "the refusal was not the probe's");

        Assert.Equal(KeyValueResponseType.Committed, await Commit(node.Kahuna, tx, ct));
        Assert.Equal((KeyValueResponseType.Get, "V2"), await Read(node.Kahuna, writtenKey, ct));
    }

    /// <summary>
    /// The same unanswerable probe through a script. The script staged its write before the gate went up (the
    /// finalizer's pre-validation hook raises it, after the staging and before the probe), so the probe is the
    /// first thing the gate refuses. The script runs optimistically: a pessimistic one holds a point lock, whose
    /// proof at commit the gate refuses on its own, and that refusal would hide the probe's. The run answers
    /// MustRetry — not Aborted, since no conflict was found — leaves nothing durable, and a re-run after the gate
    /// is cleared commits.
    ///
    /// The refused run releases its working set while the gate still stands, so that release cannot reach the
    /// key and the staged intent lingers until its lease (a real gate relinquishes leadership, which drops that
    /// in-memory state). The lease is short here and the re-run retries through that window.
    /// </summary>
    [Fact]
    public async Task Script_WrittenKeyPartitionGatedAtTheProbe_AnswersMustRetry_NotAborted_AndARerunCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct, stagedWriteIntentLeaseMs: 500);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;

        string key = $"cpu-{Guid.NewGuid():N}-s/k";
        await Seed(node.Kahuna, key, "V1", ct);

        byte[] script = Encoding.UTF8.GetBytes($"BEGIN (locking=\"optimistic\") SET `{key}` 'V2' COMMIT END");

        IDisposable? gate = null;
        finalizer.TestAfterPreValidationHook = _ =>
        {
            finalizer.TestAfterPreValidationHook = null;
            gate = Gate(node, key);
            return Task.CompletedTask;
        };

        long unprovenBefore = DurableTransactionMetrics.CommitProbesUnprovenCount;

        KeyValueTransactionResult refused;
        try
        {
            refused = await node.Kahuna.TryExecuteTransactionScript(script, null, null);
        }
        finally
        {
            finalizer.TestAfterPreValidationHook = null;
            gate?.Dispose();
        }

        Assert.NotNull(gate);
        Assert.Equal(KeyValueResponseType.MustRetry, refused.Type);
        Assert.True(DurableTransactionMetrics.CommitProbesUnprovenCount > unprovenBefore, "the refusal was not the probe's");
        Assert.Equal((KeyValueResponseType.Get, "V1"), await Read(node.Kahuna, key, ct));

        KeyValueTransactionResult rerun = await node.Kahuna.TryExecuteTransactionScript(script, null, null);
        long deadline = Environment.TickCount64 + 10_000;
        while (rerun.Type == KeyValueResponseType.MustRetry && Environment.TickCount64 < deadline)
        {
            await Task.Delay(50, ct);
            rerun = await node.Kahuna.TryExecuteTransactionScript(script, null, null);
        }

        Assert.Equal(KeyValueResponseType.Set, rerun.Type);
        Assert.Equal((KeyValueResponseType.Get, "V2"), await Read(node.Kahuna, key, ct));
    }
}
