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
/// A pessimistic read-modify-write that locks a key inside the decision→settlement window another transaction's
/// commit leaves open under deferred settlement. The commit has returned and released its in-memory write intent,
/// but its value still lives only in the durable prepared-intent store.
///
/// <para>The lock must not be granted over a head it cannot see: the acquire resolves the durable intent the way a
/// write does, so after it the resident entry holds the committed value. A reader that pinned the key before that
/// commit must learn that its pin is behind the committed head (<c>Aborted</c>), never read the pinned value under
/// its fresh lock. And the settlement that later applies the committed value must not erase the lock holder's pin
/// or lock, which is what let the holder's write, computed from the pre-commit value, overwrite the commit.</para>
/// </summary>
public sealed class TestPointLockOverCommittedUnsettledWrite
{
    private const int LockExpiresMs = 30_000;

    private readonly ILoggerFactory loggerFactory;

    public TestPointLockOverCommittedUnsettledWrite(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
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
            InitialPartitions = 4,
            DurableDeferredSettlement = true
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("plu/seed", ct);

        return node;
    }

    private static string Bucket(string tag) => $"plu-{Guid.NewGuid().ToString("N")[..8]}-{tag}";

    /// <summary>
    /// The lost update end to end. T2 reads the key (its pin holds <c>v0</c>). T1 locks the key, writes <c>v1</c>
    /// and commits; its settlement is held back. T2 then locks the key: the read under that lock must not return
    /// the pinned <c>v0</c>. When the settlement has run, T2 still must not be able to write a value computed from
    /// <c>v0</c>: the final value keeps <c>v1</c>.
    /// </summary>
    /// <param name="twoPartitions">T1 also writes a key on another partition, which keeps its commit on the
    /// two-phase path; false is the single-partition shape.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PinnedReader_LocksAfterACommit_NeverComputesFromItsPin(bool twoPartitions)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("pinned");
        string key = bucket + "/k";
        string? companion = twoPartitions ? CompanionOnAnotherPartition(node, key) : null;

        await Seed(kahuna, key, "v0", ct);
        if (companion is not null)
            await Seed(kahuna, companion, "c0", ct);

        using ResolutionGate gate = new(manager);

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", ct);
        (KeyValueResponseType pinType, ReadOnlyKeyValueEntry? pinned) = await Read(kahuna, t2, key, ct);
        Assert.Equal(KeyValueResponseType.Get, pinType);
        Assert.Equal("v0", Text(pinned));

        await LockWriteCommit(kahuna, bucket + "/t1", key, "v1", companion, ct);

        (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            t2.TransactionId, key, LockExpiresMs, KeyValueDurability.Persistent, ct,
            coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? underLock) = await Read(kahuna, t2, key, ct);

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"two partitions: {twoPartitions}; unsettled intents: {manager.DurablePreparedIntentStore.Count}; " +
            $"T2 lock: {lockType}; T2 read under the lock: {readType} '{Text(underLock)}'");

        Assert.False(readType == KeyValueResponseType.Get && Text(underLock) == "v0",
            "T2 read its pinned pre-commit value under its lock although T1's commit had returned");

        // Let the held settlement land before T2 writes: this is the order that let the replication notice delete
        // T2's pin and lock, after which T2's write was based on the new head and passed every check.
        gate.Release();
        await WaitUntil(() => manager.DurablePreparedIntentStore.Get(key) is null);

        bool t2Committed = false;
        if (lockType == KeyValueResponseType.Locked && readType == KeyValueResponseType.Get)
        {
            (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                t2.TransactionId, key, Encoding.UTF8.GetBytes(Text(underLock) + "+t2"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            if (setType == KeyValueResponseType.Set)
            {
                (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(t2, ct);
                t2Committed = commitType == KeyValueResponseType.Committed;
            }
        }

        if (!t2Committed)
            await kahuna.LocateAndRollbackTransaction(t2, ct);

        string final = await ReadCommitted(kahuna, key, ct);
        TestContext.Current.TestOutputHelper?.WriteLine($"T2 committed: {t2Committed}; final '{final}'");

        Assert.StartsWith("v1", final);
    }

    /// <summary>
    /// The same window without a pin: T2 locks after T1's commit returned, reads the committed <c>v1</c> under the
    /// lock, writes <c>v1+t2</c> and commits. The settlement of T1, held until then, must not disturb T2's result.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Reader_LocksAfterACommit_ComputesFromTheCommittedValue(bool twoPartitions)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("fresh");
        string key = bucket + "/k";
        string? companion = twoPartitions ? CompanionOnAnotherPartition(node, key) : null;

        await Seed(kahuna, key, "v0", ct);
        if (companion is not null)
            await Seed(kahuna, companion, "c0", ct);

        using ResolutionGate gate = new(manager);

        await LockWriteCommit(kahuna, bucket + "/t1", key, "v1", companion, ct);

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", ct);
        (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            t2.TransactionId, key, LockExpiresMs, KeyValueDurability.Persistent, ct,
            coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, lockType);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? underLock) = await Read(kahuna, t2, key, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal("v1", Text(underLock));

        (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            t2.TransactionId, key, "v1+t2"u8.ToArray(), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setType);

        (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(t2, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);

        gate.Release();
        await WaitUntil(() => manager.DurablePreparedIntentStore.Count == 0);

        Assert.Equal("v1+t2", await ReadCommitted(kahuna, key, ct));
    }

    /// <summary>
    /// A durable prepared intent whose transaction has no decision yet covers the key, and no in-memory write
    /// intent does (the constructed shape of a commit whose decision is still in flight). The lock must not be
    /// granted over it: the writer may still commit a value the new holder never saw.
    /// </summary>
    [Fact]
    public async Task Acquire_OverUndecidedDurableIntent_IsNotGranted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("undecided");
        string key = bucket + "/k";
        long seeded = await Seed(kahuna, key, "v0", ct);

        TransactionHandle writer = await StartTransaction(kahuna, bucket + "/writer", ct);
        manager.DurablePreparedIntentStore.ImportIntents([new PreparedIntent(
            writer.TransactionId, Epoch: 0, key, ManifestHash: 0, RecordAnchorKey: bucket + "/writer",
            CommitTimestamp: writer.TransactionId, State: KeyValueState.Set, Value: "v1"u8.ToArray(), Bucket: null,
            Revision: seeded + 1, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: -1,
            BaseState: KeyValueState.Undefined, RecoveryDeadline: HLCTimestamp.Zero,
            Resolution: PreparedIntentResolution.Pending)]);

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", ct);
        (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            t2.TransactionId, key, LockExpiresMs, KeyValueDurability.Persistent, ct,
            coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        TestContext.Current.TestOutputHelper?.WriteLine($"acquire over an undecided durable intent: {lockType}");

        Assert.NotEqual(KeyValueResponseType.Locked, lockType);

        await kahuna.LocateAndRollbackTransaction(t2, ct);
    }

    /// <summary>T1: lock the key (and the companion), write both, commit. The commit must report Committed.</summary>
    private static async Task LockWriteCommit(IKahuna kahuna, string coordinatorKey, string key, string value, string? companion, CancellationToken ct)
    {
        TransactionHandle t1 = await StartTransaction(kahuna, coordinatorKey, ct);

        foreach (string target in companion is null ? [key] : new[] { key, companion })
        {
            (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                t1.TransactionId, target, LockExpiresMs, KeyValueDurability.Persistent, ct,
                coordinatorKey: t1.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, lockType);

            (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                t1.TransactionId, target, Encoding.UTF8.GetBytes(value), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: t1.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, setType);
        }

        (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(t1, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);
    }

    /// <summary>A transactional point read that is not registered with the coordinator: the pessimistic
    /// read-modify-write shape, which relies on the key's pin and the lock rather than on read-set validation.</summary>
    private static Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> Read(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct) =>
        kahuna.LocateAndTryGetValue(tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

    private static async Task<string> ReadCommitted(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, type);
        return Text(entry);
    }

    private static string Text(ReadOnlyKeyValueEntry? entry) =>
        entry?.Value is null ? "" : Encoding.UTF8.GetString(entry.Value);

    /// <summary>A key on a different partition from <paramref name="key"/>. Keys route by their bucket, so the
    /// candidates vary the bucket, not the leaf.</summary>
    private static string CompanionOnAnotherPartition(EmbeddedKahunaNode node, string key)
    {
        int keyPartition = node.Raft.GetPartitionKey(key);
        for (int i = 0; i < 256; i++)
        {
            string candidate = $"{key}-companion{i}/k";
            if (node.Raft.GetPartitionKey(candidate) != keyPartition)
                return candidate;
        }

        Assert.Fail($"no key on a partition other than {keyPartition} among 256 candidates");
        return "";
    }

    private static async Task<TransactionHandle> StartTransaction(IKahuna kahuna, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        return handle;
    }

    private static async Task<long> Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, long revision, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return revision;
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10);
        }

        Assert.True(predicate(), "condition not met in time");
    }

    /// <summary>
    /// Holds every deferred resolution scheduled while it is installed, so the decision→settlement window of a
    /// real commit stays open until <see cref="Release"/>. Disposal releases and uninstalls the hook.
    /// </summary>
    private sealed class ResolutionGate : IDisposable
    {
        private readonly DurableTransactionFinalizer finalizer;

        private readonly TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ResolutionGate(KahunaManager manager)
        {
            finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
            finalizer.TestBeforeDeferredResolutionHook = ct => open.Task.WaitAsync(ct);
        }

        public void Release()
        {
            finalizer.TestBeforeDeferredResolutionHook = null;
            open.TrySetResult();
        }

        public void Dispose() => Release();
    }
}
