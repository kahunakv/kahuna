using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The working-set release that follows a commit on one node. A one-phase commit applies each written key on the
/// leader before it answers, and that apply removes the transaction's write intent and MVCC entry, so the release
/// sends nothing for those keys. Everything else the transaction holds keeps its release: a key it only read, a
/// point lock on a key it did not write, a prefix lock, a range lock, and every key of a transaction that did not
/// commit through the one-phase path.
///
/// <para>The counts read here belong to the coordinator of one node. A key is checked with a release that
/// acknowledges only what the node holds: it answers that nothing is held when the transaction left neither a
/// write intent nor an MVCC entry on the key.</para>
/// </summary>
public sealed class TestOnePhaseCommitWorkingSetRelease : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;

    public TestOnePhaseCommitWorkingSetRelease(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private async Task<EmbeddedKahunaNode> StartNode(CancellationToken ct, int partitions = 1)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = partitions
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("rel0/k0", ct);

        return node;
    }

    private static string Bucket(string tag) => $"rel-{Guid.NewGuid().ToString("N")[..8]}-{tag}";

    /// <summary>The per-key releases the node sent and the written keys it sent none for.</summary>
    private readonly record struct ReleaseCounts(long Sent, long LeftToCommitApply)
    {
        public static ReleaseCounts Of(KahunaManager manager) =>
            new(manager.TransactionCoordinator.PointReleasesSent, manager.TransactionCoordinator.PointReleasesLeftToCommitApply);

        public static ReleaseCounts operator -(ReleaseCounts after, ReleaseCounts before) =>
            new(after.Sent - before.Sent, after.LeftToCommitApply - before.LeftToCommitApply);
    }

    private static async Task<TransactionHandle> Start(IKahuna kahuna, string coordinatorKey, CancellationToken ct, int timeout = 60_000)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = false,
                Timeout = timeout
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static async Task Write(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, "v"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task Read(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Get, type);
    }

    private static async Task Seed(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task LockPoint(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            tx.TransactionId, key, 60_000, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Locked, type);
    }

    /// <summary>
    /// Whether <paramref name="transactionId"/> holds nothing on <paramref name="key"/> on this node: no write
    /// intent and no MVCC entry. The check releases what it finds, so call it last for a key.
    /// </summary>
    private static async Task<bool> HoldsNothing(KahunaManager manager, HLCTimestamp transactionId, string key) =>
        (await manager.KeyValues.TryReleaseExclusiveLockHeldHere(transactionId, key, KeyValueDurability.Persistent)).NothingHeld;

    private static async Task AssertHoldsNothing(KahunaManager manager, TransactionHandle tx, params string[] keys)
    {
        foreach (string key in keys)
            Assert.True(await HoldsNothing(manager, tx.TransactionId, key), $"the transaction still holds something on {key}");
    }

    /// <summary>
    /// A one-phase commit that wrote four keys and took no other lock sends no per-key release, and leaves
    /// nothing of the transaction on any of the keys. A second transaction then writes the same keys.
    /// </summary>
    [Fact]
    public async Task OnePhaseCommit_OfWrittenKeysOnly_SendsNoPerKeyRelease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string bucket = Bucket("written");
        string[] keys = [$"{bucket}/a", $"{bucket}/b", $"{bucket}/c", $"{bucket}/d"];

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        foreach (string key in keys)
            await Write(manager, tx, key, ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);
        ReleaseCounts delta = ReleaseCounts.Of(manager) - before;

        Assert.Equal(new ReleaseCounts(Sent: 0, LeftToCommitApply: keys.Length), delta);
        await AssertHoldsNothing(manager, tx, keys);

        // The keys are free for the next writer.
        TransactionHandle next = await Start(manager, bucket + "/tx", ct);
        foreach (string key in keys)
            await Write(manager, next, key, ct);

        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(next, ct)).Type);
    }

    /// <summary>
    /// A key the transaction read and then wrote is one key of the working set. The apply removes the MVCC entry
    /// that holds both the read and the staged write, so no release is sent for it.
    /// </summary>
    [Fact]
    public async Task OnePhaseCommit_OfAKeyReadThenWritten_SendsNoPerKeyRelease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string key = Bucket("rmw") + "/k";
        await Seed(manager, key, ct);

        TransactionHandle tx = await Start(manager, key, ct);
        await Read(manager, tx, key, ct);
        await Write(manager, tx, key, ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        Assert.Equal(new ReleaseCounts(Sent: 0, LeftToCommitApply: 1), ReleaseCounts.Of(manager) - before);
        await AssertHoldsNothing(manager, tx, key);
    }

    /// <summary>
    /// A key the transaction only read keeps its release: the commit applies nothing on it, and the release is
    /// what removes the read snapshot.
    /// </summary>
    [Fact]
    public async Task OnePhaseCommit_ReleasesAKeyItOnlyRead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string bucket = Bucket("read");
        string readKey = $"{bucket}/r";
        await Seed(manager, readKey, ct);

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Read(manager, tx, readKey, ct);
        await Write(manager, tx, $"{bucket}/a", ct);
        await Write(manager, tx, $"{bucket}/b", ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        Assert.Equal(new ReleaseCounts(Sent: 1, LeftToCommitApply: 2), ReleaseCounts.Of(manager) - before);
        await AssertHoldsNothing(manager, tx, readKey, $"{bucket}/a", $"{bucket}/b");
    }

    /// <summary>
    /// A point lock on a key the transaction did not write keeps its release, and the lock is free after the
    /// commit. A point lock on a key it did write is the intent the apply removed, so it needs none.
    /// </summary>
    [Fact]
    public async Task OnePhaseCommit_ReleasesAPointLockOnAKeyItDidNotWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string bucket = Bucket("lock");
        string lockedOnly = $"{bucket}/locked";
        string lockedAndWritten = $"{bucket}/both";

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await LockPoint(manager, tx, lockedOnly, ct);
        await LockPoint(manager, tx, lockedAndWritten, ct);
        await Write(manager, tx, lockedAndWritten, ct);
        await Write(manager, tx, $"{bucket}/a", ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        Assert.Equal(new ReleaseCounts(Sent: 1, LeftToCommitApply: 2), ReleaseCounts.Of(manager) - before);

        // Another transaction takes the lock at once: a lock that was left out would answer AlreadyLocked.
        TransactionHandle next = await Start(manager, bucket + "/tx", ct);
        await LockPoint(manager, next, lockedOnly, ct);
        Assert.Equal(KeyValueResponseType.RolledBack, await manager.LocateAndRollbackTransaction(next, ct));

        await AssertHoldsNothing(manager, tx, lockedOnly, lockedAndWritten, $"{bucket}/a");
    }

    /// <summary>
    /// The set of released keys names persistent keys. A point lock on the ephemeral key of the same name is
    /// another lock, on another actor, and keeps its release.
    /// </summary>
    [Fact]
    public async Task OnePhaseCommit_ReleasesAnEphemeralLockThatSharesTheNameOfAWrittenKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string key = Bucket("durability") + "/k";

        TransactionHandle tx = await Start(manager, key, ct);

        (KeyValueResponseType locked, _, _, _) = await manager.LocateAndTryAcquireExclusiveLock(
            tx.TransactionId, key, 60_000, KeyValueDurability.Ephemeral, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, locked);

        await Write(manager, tx, key, ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        Assert.Equal(new ReleaseCounts(Sent: 1, LeftToCommitApply: 1), ReleaseCounts.Of(manager) - before);

        Assert.True((await manager.KeyValues.TryReleaseExclusiveLockHeldHere(tx.TransactionId, key, KeyValueDurability.Ephemeral)).NothingHeld,
            "the ephemeral lock was not released");
    }

    /// <summary>
    /// A written key whose apply did not find the transaction's intent keeps its release. Here the intent of one
    /// key is dropped after the commit-time checks passed and before the bundle is proposed, as a lapsed lease
    /// drops it: the commit still lands, the apply of that key reports no release, and the key is sent.
    /// </summary>
    [Fact]
    public async Task OnePhaseCommit_ReleasesAWrittenKeyWhoseApplyFoundNoIntent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        TransactionCoordinator coordinator = manager.TransactionCoordinator;

        string bucket = Bucket("lost");
        string kept = $"{bucket}/kept";
        string lost = $"{bucket}/lost";

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Write(manager, tx, kept, ct);
        await Write(manager, tx, lost, ct);

        bool dropped = false;
        coordinator.DurableFinalizerForTests.TestAfterReadSetValidationHook = async _ =>
        {
            dropped = !(await manager.KeyValues.TryReleaseExclusiveLockHeldHere(tx.TransactionId, lost, KeyValueDurability.Persistent)).NothingHeld;
        };

        ReleaseCounts before = ReleaseCounts.Of(manager);

        try
        {
            Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);
        }
        finally
        {
            coordinator.DurableFinalizerForTests.TestAfterReadSetValidationHook = null;
        }

        Assert.True(dropped, "the hook found no intent to drop");
        Assert.Equal(new ReleaseCounts(Sent: 1, LeftToCommitApply: 1), ReleaseCounts.Of(manager) - before);
        await AssertHoldsNothing(manager, tx, kept, lost);
    }

    /// <summary>
    /// A prefix lock and a range lock are released after a one-phase commit, as before: another transaction takes
    /// each of them at once.
    /// </summary>
    [Fact]
    public async Task OnePhaseCommit_ReleasesItsPrefixLockAndItsRangeLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string bucket = Bucket("shape");
        string prefix = bucket + "-prefix";
        string range = bucket + "-range";

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);

        Assert.Equal(KeyValueResponseType.Locked, await manager.LocateAndTryAcquireExclusivePrefixLock(
            tx.TransactionId, prefix, 60_000, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()));

        (KeyValueResponseType rangeLocked, _) = await manager.LocateAndTryAcquireRangeLock(
            tx.TransactionId, range, null, true, null, false, 60_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, rangeLocked);

        await Write(manager, tx, $"{bucket}/a", ct);
        await Write(manager, tx, $"{bucket}/b", ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        // The commit stayed on the one-phase path, so the two locks were released beside keys that were not.
        Assert.Equal(new ReleaseCounts(Sent: 0, LeftToCommitApply: 2), ReleaseCounts.Of(manager) - before);

        HLCTimestamp other = (await Start(manager, bucket + "/other", ct)).TransactionId;

        Assert.Equal(KeyValueResponseType.Locked, await manager.LocateAndTryAcquireExclusivePrefixLock(
            other, prefix, 60_000, KeyValueDurability.Persistent, ct));

        (KeyValueResponseType rangeRetaken, _) = await manager.LocateAndTryAcquireRangeLock(
            other, range, null, true, null, false, 60_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct);
        Assert.Equal(KeyValueResponseType.Locked, rangeRetaken);
    }

    /// <summary>A rollback releases every key it wrote and read: nothing was applied for it.</summary>
    [Fact]
    public async Task Rollback_ReleasesEveryKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string bucket = Bucket("rollback");
        string readKey = $"{bucket}/r";
        await Seed(manager, readKey, ct);

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Read(manager, tx, readKey, ct);
        await Write(manager, tx, $"{bucket}/a", ct);
        await Write(manager, tx, $"{bucket}/b", ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.Equal(KeyValueResponseType.RolledBack, await manager.LocateAndRollbackTransaction(tx, ct));

        Assert.Equal(new ReleaseCounts(Sent: 3, LeftToCommitApply: 0), ReleaseCounts.Of(manager) - before);
        await AssertHoldsNothing(manager, tx, readKey, $"{bucket}/a", $"{bucket}/b");
    }

    /// <summary>
    /// A commit that aborts on a stale read releases every key. Its one-phase attempt applied nothing, so no key
    /// is left out.
    /// </summary>
    [Fact]
    public async Task CommitThatAborts_ReleasesEveryKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string bucket = Bucket("abort");
        string readKey = $"{bucket}/r";
        await Seed(manager, readKey, ct);

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Read(manager, tx, readKey, ct);
        await Write(manager, tx, $"{bucket}/a", ct);
        await Write(manager, tx, $"{bucket}/b", ct);

        // Another writer moves the key the transaction read.
        await Seed(manager, readKey, ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.Equal(KeyValueResponseType.Aborted, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        Assert.Equal(new ReleaseCounts(Sent: 3, LeftToCommitApply: 0), ReleaseCounts.Of(manager) - before);
        await AssertHoldsNothing(manager, tx, readKey, $"{bucket}/a", $"{bucket}/b");
    }

    /// <summary>
    /// A commit that answers a retry keeps its working set for the retry and releases nothing. The rollback that
    /// ends the session then releases every key: nothing was applied for it.
    /// </summary>
    [Fact]
    public async Task CommitThatAnswersRetry_KeepsEveryKey_AndItsRollbackReleasesThem()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;

        string bucket = Bucket("retry-rollback");

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Write(manager, tx, $"{bucket}/a", ct);
        await Write(manager, tx, $"{bucket}/b", ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        finalizer.TestAfterReadSetValidationHook = _ => Task.FromException(new InvalidOperationException("injected"));

        try
        {
            Assert.Equal(KeyValueResponseType.MustRetry, (await manager.LocateAndCommitTransaction(tx, ct)).Type);
        }
        finally
        {
            finalizer.TestAfterReadSetValidationHook = null;
        }

        Assert.Equal(new ReleaseCounts(Sent: 0, LeftToCommitApply: 0), ReleaseCounts.Of(manager) - before);

        Assert.Equal(KeyValueResponseType.RolledBack, await manager.LocateAndRollbackTransaction(tx, ct));

        Assert.Equal(new ReleaseCounts(Sent: 2, LeftToCommitApply: 0), ReleaseCounts.Of(manager) - before);
        await AssertHoldsNothing(manager, tx, $"{bucket}/a", $"{bucket}/b");
    }

    /// <summary>
    /// A commit that answers a retry and then commits on the second call sends no per-key release: the apply of
    /// the call that committed reports the keys.
    /// </summary>
    [Fact]
    public async Task CommitThatAnswersRetry_ThenCommits_SendsNoPerKeyRelease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;

        string bucket = Bucket("retry-commit");

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Write(manager, tx, $"{bucket}/a", ct);
        await Write(manager, tx, $"{bucket}/b", ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        finalizer.TestAfterReadSetValidationHook = _ => Task.FromException(new InvalidOperationException("injected"));

        try
        {
            Assert.Equal(KeyValueResponseType.MustRetry, (await manager.LocateAndCommitTransaction(tx, ct)).Type);
        }
        finally
        {
            finalizer.TestAfterReadSetValidationHook = null;
        }

        Assert.Equal(new ReleaseCounts(Sent: 0, LeftToCommitApply: 0), ReleaseCounts.Of(manager) - before);

        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        Assert.Equal(new ReleaseCounts(Sent: 0, LeftToCommitApply: 2), ReleaseCounts.Of(manager) - before);
        await AssertHoldsNothing(manager, tx, $"{bucket}/a", $"{bucket}/b");
    }

    /// <summary>A session the reaper ends releases every key it wrote.</summary>
    [Fact]
    public async Task ReapedSession_ReleasesEveryKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        TransactionCoordinator coordinator = manager.TransactionCoordinator;

        string bucket = Bucket("reap");

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct, timeout: 1_000);
        await Write(manager, tx, $"{bucket}/a", ct);
        await Write(manager, tx, $"{bucket}/b", ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);

        await WaitUntilAsync(async () =>
        {
            await coordinator.ReapAbandonedSessions();
            return !coordinator.HasSession(tx.TransactionId);
        }, timeoutMs: 60_000);

        ReleaseCounts delta = ReleaseCounts.Of(manager) - before;
        Assert.Equal(0, delta.LeftToCommitApply);
        Assert.True(delta.Sent >= 2, $"the reaper sent {delta.Sent} per-key releases for two written keys");
        await AssertHoldsNothing(manager, tx, $"{bucket}/a", $"{bucket}/b");
    }

    /// <summary>
    /// A commit over two partitions takes the two-phase path. Its apply runs in the resolution, after the answer
    /// with deferred settlement, so the release is what clears its intents: every key is sent.
    /// </summary>
    [Fact]
    public async Task TwoPhaseCommit_ReleasesEveryKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct, partitions: 2);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string first = Bucket("two");
        int firstPartition = manager.KeyValues.LocateDurablePartition(first + "/k").PartitionId;

        string second = first;
        for (int i = 0; i < 4_096 && second == first; i++)
        {
            string candidate = $"{first}-{i}";
            if (manager.KeyValues.LocateDurablePartition(candidate + "/k").PartitionId != firstPartition)
                second = candidate;
        }

        Assert.NotEqual(first, second);

        TransactionHandle tx = await Start(manager, first + "/tx", ct);
        await Write(manager, tx, first + "/k", ct);
        await Write(manager, tx, second + "/k", ct);

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        Assert.Equal(new ReleaseCounts(Sent: 2, LeftToCommitApply: 0), ReleaseCounts.Of(manager) - before);
        await AssertHoldsNothing(manager, tx, first + "/k", second + "/k");
    }

    /// <summary>
    /// The release leaves out the keys the commit reported and no other. With one of two staged keys reported,
    /// the other is released, and the reported one keeps what the transaction holds on it: the release trusts the
    /// report, so the report must come only from an apply that removed the intent.
    /// </summary>
    [Fact]
    public async Task Release_LeavesOutOnlyTheKeysTheCommitReported()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        TransactionCoordinator coordinator = manager.TransactionCoordinator;

        string bucket = Bucket("subset");
        string reported = $"{bucket}/reported";
        string notReported = $"{bucket}/not-reported";

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Write(manager, tx, reported, ct);
        await Write(manager, tx, notReported, ct);

        Assert.True(coordinator.sessions.TryGetValue(tx.TransactionId, out TransactionContext? context));
        context.KeysReleasedByCommitApply = new HashSet<string>(StringComparer.Ordinal) { reported };

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.True(await coordinator.ReleaseWorkingSet(context));

        Assert.Equal(new ReleaseCounts(Sent: 1, LeftToCommitApply: 1), ReleaseCounts.Of(manager) - before);
        Assert.True(await HoldsNothing(manager, tx.TransactionId, notReported));
        Assert.False(await HoldsNothing(manager, tx.TransactionId, reported));
    }

    /// <summary>
    /// A key the commit reported is not a pending release: when the release of the other keys is not
    /// acknowledged, the list handed to the detached retry names those keys and not the reported one.
    /// </summary>
    [Fact]
    public async Task Release_DoesNotRecordAReportedKeyAsUnacknowledged()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        TransactionCoordinator coordinator = manager.TransactionCoordinator;

        string bucket = Bucket("pending");
        string reported = $"{bucket}/reported";
        string written = $"{bucket}/written";

        // An empty key makes the batched release answer InvalidInput for the whole batch, so no mandatory key of
        // the batch is acknowledged.
        TransactionContext context = new()
        {
            CoordinatorKey = bucket + "/tx",
            TransactionId = ((IRaft)node.Raft).HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId()),
            LocksAcquired = [(string.Empty, KeyValueDurability.Persistent), (reported, KeyValueDurability.Persistent)],
            ModifiedKeys = [(reported, KeyValueDurability.Persistent), (written, KeyValueDurability.Persistent)],
            KeysReleasedByCommitApply = new HashSet<string>(StringComparer.Ordinal) { reported }
        };

        ReleaseCounts before = ReleaseCounts.Of(manager);
        Assert.False(await coordinator.ReleaseWorkingSet(context));

        Assert.Equal(new ReleaseCounts(Sent: 2, LeftToCommitApply: 1), ReleaseCounts.Of(manager) - before);

        List<PendingLockRelease>? pending = context.TakeUnacknowledgedReleases();
        Assert.NotNull(pending);
        Assert.Equal([string.Empty, written], pending.Select(static release => release.Key).Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// A script that writes persistent keys commits through the same finalize and the same release, so its
    /// one-phase commit sends no per-key release either. The script releases after it answers.
    /// </summary>
    [Fact]
    public async Task ScriptOnePhaseCommit_SendsNoPerKeyRelease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string bucket = Bucket("script");
        ReleaseCounts before = ReleaseCounts.Of(manager);

        KeyValueTransactionResult result = await manager.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes($"BEGIN SET `{bucket}/a` 'v1' SET `{bucket}/b` 'v2' COMMIT END"), null, null);

        Assert.Equal(KeyValueResponseType.Set, result.Type);

        await WaitUntilAsync(() => (ReleaseCounts.Of(manager) - before).LeftToCommitApply == 2);
        Assert.Equal(new ReleaseCounts(Sent: 0, LeftToCommitApply: 2), ReleaseCounts.Of(manager) - before);

        (KeyValueResponseType read, ReadOnlyKeyValueEntry? entry) = await manager.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{bucket}/b", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, read);
        Assert.Equal("v2"u8.ToArray(), entry?.Value);
    }

    /// <summary>
    /// An optimistic script that reads a key after it wrote it keeps the key in its read set too. The apply
    /// removed the one MVCC entry that holds the read and the write, so the key is left out as a read snapshot
    /// as well as a staged write.
    /// </summary>
    [Fact]
    public async Task ScriptOnePhaseCommit_OfAKeyWrittenThenRead_SendsNoPerKeyRelease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        string bucket = Bucket("script-read");
        ReleaseCounts before = ReleaseCounts.Of(manager);

        KeyValueTransactionResult result = await manager.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes($$"""
                BEGIN (locking="optimistic")
                  SET `{{bucket}}/a` 'v1'
                  LET seen = GET `{{bucket}}/a`
                  SET `{{bucket}}/b` seen
                  COMMIT
                END
                """), null, null);

        Assert.True(result.Type == KeyValueResponseType.Set, $"{result.Type}: {result.Reason}");

        await WaitUntilAsync(() => (ReleaseCounts.Of(manager) - before).LeftToCommitApply == 2);
        Assert.Equal(new ReleaseCounts(Sent: 0, LeftToCommitApply: 2), ReleaseCounts.Of(manager) - before);

        (KeyValueResponseType read, ReadOnlyKeyValueEntry? entry) = await manager.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{bucket}/b", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, read);
        Assert.Equal("v1"u8.ToArray(), entry?.Value);
    }
}
