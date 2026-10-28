using System.Diagnostics;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// What a snapshot read pays for a live foreign write intent that is not yet claimed for its commit: nothing.
///
/// <para>A write staged above the read's snapshot commits above it, so the read never waits for it
/// (<c>KeyValueWriteIntentSafeTime.MayCommitAtOrBefore</c>). A write staged at or below the snapshot used to
/// hold the read for the writer's whole open time; now the read raises the intent's commit floor to its own
/// timestamp and is served the committed head (<c>KeyValueWriteIntentSafeTime.MustWait</c>), and the writer's
/// commit is minted above the floor. The commit probe claims the intent and reports the floor; a floor at or
/// above the commit timestamp the coordinator already froze sends the commit back for a fresh freeze above
/// it, before anything durable. From the claim on a read that could be inside the commit waits, for the
/// commit's own latency.</para>
///
/// <para>Each shape is checked on the point read and on the streamed range scan, the two reads a table scan and
/// an index probe make, and the commit is driven through the real interactive transaction path on both the
/// one-phase bundle and the two-phase flow.</para>
/// </summary>
public sealed class TestSnapshotReadIntentWait
{
    private const string Prefix = "intentwait";

    /// <summary>A read that does not wait finishes well inside this; one that waits takes at least the hold.</summary>
    private static readonly TimeSpan NoWait = TimeSpan.FromMilliseconds(300);

    /// <summary>How long a writer holds its intent before the test clears it, in the rollback shapes.</summary>
    private static readonly TimeSpan Hold = TimeSpan.FromMilliseconds(700);

    private readonly ILoggerFactory loggerFactory;

    public TestSnapshotReadIntentWait(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct, int partitions = 1)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = partitions
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync($"{Prefix}/k00", ct);
        return node;
    }

    /// <summary>Ten committed rows under the prefix, so a scan with a page size of four crosses pages.</summary>
    private static async Task SeedRows(EmbeddedKahunaNode node, CancellationToken ct)
    {
        for (int i = 0; i < 10; i++)
            await Seed(node, $"{Prefix}/k{i:D2}", $"v{i}", ct);
    }

    private static async Task Seed(EmbeddedKahunaNode node, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, set);
    }

    private static HLCTimestamp Now(KahunaManager manager)
        => manager.Raft.HybridLogicalClock.TrySendOrLocalEvent(manager.Raft.GetLocalNodeId());

    /// <summary>A staged write of a bare transaction id: it plants the key's write intent and never decides by itself.</summary>
    private static async Task Stage(EmbeddedKahunaNode node, HLCTimestamp writer, string key, CancellationToken ct)
    {
        (KeyValueResponseType staged, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            writer, key, "staged"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, staged);
    }

    private static async Task Lock(EmbeddedKahunaNode node, HLCTimestamp writer, string key, CancellationToken ct)
    {
        (KeyValueResponseType locked, _, _, _) = await node.Kahuna.LocateAndTryAcquireExclusiveLock(
            writer, key, 30_000, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Locked, locked);
    }

    /// <summary>Clears the writer's intent on the key, as a rollback would.</summary>
    private static async Task Release(KahunaManager manager, HLCTimestamp writer, string key, CancellationToken ct)
    {
        (KeyValueResponseType released, _) = await manager.KeyValues.LocateAndTryReleaseExclusiveLock(
            writer, key, KeyValueDurability.Persistent, ct);
        Assert.True(released is KeyValueResponseType.Unlocked or KeyValueResponseType.DoesNotExist, $"release answered {released}");
    }

    private static async Task<TransactionHandle> Start(EmbeddedKahunaNode node, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
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

    /// <summary>A write staged by an interactive transaction, which the transaction's commit then decides.</summary>
    private static async Task StageInTransaction(EmbeddedKahunaNode node, TransactionHandle tx, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0,
            KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    private static async Task<KeyValueResponseType> Commit(EmbeddedKahunaNode node, TransactionHandle tx, CancellationToken ct)
    {
        (KeyValueResponseType commitType, _) = await node.Kahuna.LocateAndCommitTransaction(tx, ct);
        return commitType;
    }

    /// <summary>A snapshot read at <paramref name="snapshot"/>, retried while the key's leader asks it to wait.</summary>
    private static async Task<(TimeSpan Elapsed, string Value)> PointRead(EmbeddedKahunaNode node, string key, HLCTimestamp snapshot, CancellationToken ct)
    {
        long started = Stopwatch.GetTimestamp();
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
        TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.NotNull(entry);
        return (elapsed, Encoding.UTF8.GetString(entry.Value!));
    }

    /// <summary>The latest committed value of the key and the stamp its revision carries.</summary>
    private static async Task<(string Value, HLCTimestamp LastModified)> Latest(EmbeddedKahunaNode node, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.NotNull(entry);
        return (Encoding.UTF8.GetString(entry.Value!), entry.LastModified);
    }

    private static async Task<(TimeSpan Elapsed, List<string> Values)> Scan(EmbeddedKahunaNode node, HLCTimestamp snapshot, CancellationToken ct)
    {
        long started = Stopwatch.GetTimestamp();
        List<string> values = [];
        await foreach ((string _, ReadOnlyKeyValueEntry entry) in node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, Prefix, null, true, null, false, pageSize: 4, snapshot, KeyValueDurability.Persistent, ct))
            values.Add(Encoding.UTF8.GetString(entry.Value!));
        return (Stopwatch.GetElapsedTime(started), values);
    }

    private static Task ClearAfterHold(KahunaManager manager, HLCTimestamp writer, string key, CancellationToken ct)
        => Task.Run(async () =>
        {
            await Task.Delay(Hold, ct);
            await Release(manager, writer, key, ct);
        }, ct);

    /// <summary>
    /// The contested row under the prefix and a companion key on another partition, outside the scanned prefix,
    /// so a transaction over both takes the two-phase flow and the scan still sees the ten seeded rows.
    /// </summary>
    private static (string Contested, string Companion) KeysOnTwoPartitions(EmbeddedKahunaNode node)
    {
        string contested = $"{Prefix}/k06";
        int contestedPartition = node.Raft.GetPartitionKey(contested);

        // Keys route by their parent bucket, so the candidates vary the bucket, not the leaf.
        for (int i = 0; i < 256; i++)
        {
            string candidate = $"companion-b{i}/k";
            if (node.Raft.GetPartitionKey(candidate) != contestedPartition)
                return (contested, candidate);
        }

        throw new InvalidOperationException("no key routes to a second partition");
    }

    /// <summary>
    /// The writer's transaction id is below the snapshot, its staged write is above it. The commit will carry
    /// at least the staged stamp, so it lands above the snapshot: neither the point read nor the scan waits,
    /// and both serve the committed value.
    /// </summary>
    [Fact]
    public async Task WriteStagedAfterTheSnapshot_ByAWriterThatBeganBefore_DoesNotBlock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        HLCTimestamp writer = Now(manager);
        HLCTimestamp snapshot = Now(manager);
        await Stage(node, writer, key, ct);

        try
        {
            (TimeSpan pointElapsed, string value) = await PointRead(node, key, snapshot, ct);
            Assert.Equal("v6", value);
            Assert.True(pointElapsed < NoWait, $"the point read waited {pointElapsed.TotalMilliseconds:F0} ms on a write staged above its snapshot");

            (TimeSpan scanElapsed, List<string> values) = await Scan(node, snapshot, ct);
            Assert.Equal(10, values.Count);
            Assert.Equal("v6", values[6]);
            Assert.True(scanElapsed < NoWait, $"the scan waited {scanElapsed.TotalMilliseconds:F0} ms on a write staged above its snapshot");
        }
        finally
        {
            await Release(manager, writer, key, ct);
        }
    }

    /// <summary>
    /// The write is staged below the snapshot by a writer that never claims it for a commit. The read used to
    /// wait for the writer's decision; now it raises the intent's commit floor and is served the committed
    /// value at once, on the point read and on the scan. The writer is rolled back after a hold, which the
    /// read never pays.
    /// </summary>
    [Fact]
    public async Task WriteStagedBeforeTheSnapshot_IsServedAtOnce_WithTheCommittedHead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        HLCTimestamp writer = Now(manager);
        await Stage(node, writer, key, ct);
        HLCTimestamp snapshot = Now(manager);
        Task clear = ClearAfterHold(manager, writer, key, ct);

        (TimeSpan scanElapsed, List<string> values) = await Scan(node, snapshot, ct);
        (TimeSpan pointElapsed, string value) = await PointRead(node, key, snapshot, ct);
        await clear;

        Assert.Equal(10, values.Count);
        Assert.Equal("v6", values[6]);
        Assert.True(scanElapsed < NoWait, $"the scan waited {scanElapsed.TotalMilliseconds:F0} ms on a write staged below its snapshot by an unclaimed writer");

        Assert.Equal("v6", value);
        Assert.True(pointElapsed < NoWait, $"the point read waited {pointElapsed.TotalMilliseconds:F0} ms on a write staged below its snapshot by an unclaimed writer");
    }

    /// <summary>
    /// A lock planted after the snapshot by a writer that began before it stages nothing on the key yet. Any
    /// write it stages later is stamped after the plant, so its commit lands above the snapshot: the read is
    /// served at once.
    /// </summary>
    [Fact]
    public async Task LockPlantedAfterTheSnapshot_ByAWriterThatBeganBefore_DoesNotBlock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        HLCTimestamp writer = Now(manager);
        HLCTimestamp snapshot = Now(manager);
        await Lock(node, writer, key, ct);

        try
        {
            (TimeSpan pointElapsed, string value) = await PointRead(node, key, snapshot, ct);
            Assert.Equal("v6", value);
            Assert.True(pointElapsed < NoWait, $"the point read waited {pointElapsed.TotalMilliseconds:F0} ms on a lock planted above its snapshot");

            (TimeSpan scanElapsed, List<string> values) = await Scan(node, snapshot, ct);
            Assert.Equal(10, values.Count);
            Assert.True(scanElapsed < NoWait, $"the scan waited {scanElapsed.TotalMilliseconds:F0} ms on a lock planted above its snapshot");
        }
        finally
        {
            await Release(manager, writer, key, ct);
        }
    }

    /// <summary>
    /// A lock planted before the snapshot may still be followed by a write, but that write is stamped by the
    /// actor clock the read fenced at its snapshot, so it commits above the snapshot: the read raises the
    /// intent's floor and is served at once instead of waiting for the lock to clear.
    /// </summary>
    [Fact]
    public async Task LockPlantedBeforeTheSnapshot_IsServedAtOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        HLCTimestamp writer = Now(manager);
        await Lock(node, writer, key, ct);
        HLCTimestamp snapshot = Now(manager);
        Task clear = ClearAfterHold(manager, writer, key, ct);

        (TimeSpan elapsed, string value) = await PointRead(node, key, snapshot, ct);
        await clear;

        Assert.Equal("v6", value);
        Assert.True(elapsed < NoWait, $"the point read waited {elapsed.TotalMilliseconds:F0} ms on a lock planted below its snapshot by an unclaimed writer");
    }

    /// <summary>
    /// The write is staged below the snapshot by an interactive transaction that commits after the read, on the
    /// one-phase bundle (one key). The read is served the committed head at once; the commit lands above the
    /// snapshot, so the same read asked again after the commit still answers the head, and the latest read
    /// answers the new value with a stamp above the snapshot.
    /// </summary>
    [Fact]
    public async Task WriteStagedBeforeTheSnapshot_OnePhaseCommit_LandsAboveTheSnapshot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        TransactionHandle tx = await Start(node, key + "/tx", ct);
        await StageInTransaction(node, tx, key, "committed", ct);
        HLCTimestamp snapshot = Now(manager);

        await AssertServedAtOnceThenCommittedAbove(node, tx, key, snapshot, ct);
    }

    /// <summary>
    /// The same contract on the two-phase flow: the transaction writes a second key on another partition, so
    /// its commit prepares each participant before it decides. The staged intents are claimed before the first
    /// prepare, and the commit is minted above the floor the read left.
    /// </summary>
    [Fact]
    public async Task WriteStagedBeforeTheSnapshot_TwoPhaseCommit_LandsAboveTheSnapshot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct, partitions: 4);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        (string contested, string companion) = KeysOnTwoPartitions(node);
        await Seed(node, companion, "w1", ct);

        TransactionHandle tx = await Start(node, contested + "/tx", ct);
        await StageInTransaction(node, tx, contested, "committed", ct);
        await StageInTransaction(node, tx, companion, "w2", ct);
        HLCTimestamp snapshot = Now(manager);

        await AssertServedAtOnceThenCommittedAbove(node, tx, contested, snapshot, ct);

        (string companionValue, HLCTimestamp companionStamp) = await Latest(node, companion, ct);
        Assert.Equal("w2", companionValue);
        Assert.True(companionStamp > snapshot, $"the companion commit was stamped at {companionStamp}, at or below the snapshot {snapshot}");
    }

    private static async Task AssertServedAtOnceThenCommittedAbove(
        EmbeddedKahunaNode node, TransactionHandle tx, string key, HLCTimestamp snapshot, CancellationToken ct)
    {
        (TimeSpan pointElapsed, string value) = await PointRead(node, key, snapshot, ct);
        Assert.Equal("v6", value);
        Assert.True(pointElapsed < NoWait, $"the point read waited {pointElapsed.TotalMilliseconds:F0} ms on a write staged below its snapshot by an open transaction");

        (TimeSpan scanElapsed, List<string> values) = await Scan(node, snapshot, ct);
        Assert.Equal("v6", values[6]);
        Assert.True(scanElapsed < NoWait, $"the scan waited {scanElapsed.TotalMilliseconds:F0} ms on a write staged below its snapshot by an open transaction");

        Assert.Equal(KeyValueResponseType.Committed, await Commit(node, tx, ct));

        (string latest, HLCTimestamp stamp) = await Latest(node, key, ct);
        Assert.Equal("committed", latest);
        Assert.True(stamp > snapshot, $"the commit was stamped at {stamp}, at or below the snapshot {snapshot} a read was served at");

        (_, string again) = await PointRead(node, key, snapshot, ct);
        Assert.Equal("v6", again);

        (_, List<string> scanAgain) = await Scan(node, snapshot, ct);
        Assert.Equal("v6", scanAgain[6]);
    }

    /// <summary>
    /// The read arrives after the coordinator froze the commit timestamp and before the commit probe claimed
    /// the intent, with a snapshot minted after the freeze: above the frozen timestamp. The read is served the
    /// head at once; the probe then reports a floor at or above the frozen timestamp, the commit is frozen
    /// again above the floor, and lands above the snapshot. One-phase bundle.
    /// </summary>
    [Fact]
    public async Task ReadMintedAfterTheFreeze_OnePhase_IsServedAtOnce_AndTheCommitIsFrozenAgainAboveIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        TransactionHandle tx = await Start(node, key + "/tx", ct);
        await StageInTransaction(node, tx, key, "committed", ct);

        await AssertReadInTheFreezeWindowPushesTheCommit(node, tx, key, ct);
    }

    /// <summary>The same interleaving on the two-phase flow, where the claim precedes the prepares.</summary>
    [Fact]
    public async Task ReadMintedAfterTheFreeze_TwoPhase_IsServedAtOnce_AndTheCommitIsFrozenAgainAboveIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct, partitions: 4);
        await SeedRows(node, ct);
        (string contested, string companion) = KeysOnTwoPartitions(node);
        await Seed(node, companion, "w1", ct);

        TransactionHandle tx = await Start(node, contested + "/tx", ct);
        await StageInTransaction(node, tx, contested, "committed", ct);
        await StageInTransaction(node, tx, companion, "w2", ct);

        HLCTimestamp snapshot = await AssertReadInTheFreezeWindowPushesTheCommit(node, tx, contested, ct);

        (string companionValue, HLCTimestamp companionStamp) = await Latest(node, companion, ct);
        Assert.Equal("w2", companionValue);
        Assert.True(companionStamp > snapshot, $"the companion commit was stamped at {companionStamp}, at or below the snapshot {snapshot}");
    }

    private static async Task<HLCTimestamp> AssertReadInTheFreezeWindowPushesTheCommit(
        EmbeddedKahunaNode node, TransactionHandle tx, string key, CancellationToken ct)
    {
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;

        Task<(TimeSpan Elapsed, string Value)>? read = null;
        HLCTimestamp snapshot = HLCTimestamp.Zero;

        // Runs after the commit timestamp was frozen and before the probe claims the intent: the window no
        // external caller can time.
        finalizer.TestAfterPreValidationHook = async hookCt =>
        {
            finalizer.TestAfterPreValidationHook = null;

            snapshot = Now(manager);
            read = PointRead(node, key, snapshot, ct);

            await Task.WhenAny(read, Task.Delay(2_000, hookCt));
        };

        long refreezesBefore = DurableTransactionMetrics.CommitFloorRefreezeCount;

        KeyValueResponseType committed;
        try
        {
            committed = await Commit(node, tx, ct);
        }
        finally
        {
            finalizer.TestAfterPreValidationHook = null;
        }

        Assert.NotNull(read);
        (TimeSpan elapsed, string value) = await read;

        Assert.Equal("v6", value);
        Assert.True(elapsed < NoWait, $"the read waited {elapsed.TotalMilliseconds:F0} ms inside the freeze window");
        Assert.Equal(KeyValueResponseType.Committed, committed);
        Assert.True(DurableTransactionMetrics.CommitFloorRefreezeCount > refreezesBefore, "the commit was not frozen again above the read's floor");

        (string latest, HLCTimestamp stamp) = await Latest(node, key, ct);
        Assert.Equal("committed", latest);
        Assert.True(stamp > snapshot, $"the commit was stamped at {stamp}, at or below the snapshot {snapshot} a read was served at");

        (_, string again) = await PointRead(node, key, snapshot, ct);
        Assert.Equal("v6", again);

        return snapshot;
    }

    /// <summary>
    /// A snapshot read left a commit floor on the staged intent. A commit probe that asks whether the intent is
    /// held without asking for the floor cannot mint its commit above it (an older coordinator), so over a
    /// persistent key it is told the intent is not held and the commit is refused; the probe that asks for the
    /// floor is answered held. A pushed intent on an ephemeral key stays held either way: the manual prepare
    /// re-stamps the write above the floor itself.
    /// </summary>
    [Fact]
    public async Task ClaimWithoutTheFloorCheck_OverAPushedPersistentIntent_IsRefused()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        HLCTimestamp writer = Now(manager);
        await Stage(node, writer, key, ct);

        try
        {
            // Before any read passes over the intent, a claim without the floor check is answered held. (A read
            // after this claim would wait: the intent is now claimed, and that is the subject of another test.)
            Assert.Equal(KeyValueResponseType.DoesNotExist, await Claim(node, writer, key, KeyValueConflictChecks.OwnStagedIntent, ct));
        }
        finally
        {
            await Release(manager, writer, key, ct);
        }

        // A fresh intent, pushed by a read before any claim.
        writer = Now(manager);
        await Stage(node, writer, key, ct);

        try
        {
            (_, string value) = await PointRead(node, key, Now(manager), ct);
            Assert.Equal("v6", value);

            Assert.Equal(KeyValueResponseType.Unlocked, await Claim(node, writer, key, KeyValueConflictChecks.OwnStagedIntent, ct));
            Assert.Equal(KeyValueResponseType.DoesNotExist, await Claim(node, writer, key, KeyValueConflictChecks.OwnStagedIntent | KeyValueConflictChecks.CommitFloor, ct));
        }
        finally
        {
            await Release(manager, writer, key, ct);
        }
    }

    private static async Task<KeyValueResponseType> Claim(EmbeddedKahunaNode node, HLCTimestamp writer, string key, KeyValueConflictChecks checks, CancellationToken ct)
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> answers = await node.Kahuna.LocateAndTryCheckManyWriteIntents(
            writer, [new KeyValueConflictProbe(key, KeyValueDurability.Persistent, checks)], ct);

        Assert.Single(answers);
        return answers[0].type;
    }

    /// <summary>
    /// The read arrives after the commit probe claimed the intent, with a snapshot above the frozen commit
    /// timestamp. The claim ended the push window: the read waits for the commit instead of answering the
    /// head, and then sees the committed value, which is what the same read answers afterwards too.
    /// </summary>
    [Fact]
    public async Task ReadOverAClaimedIntent_StillWaitsForTheCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        TransactionHandle tx = await Start(node, key + "/tx", ct);
        await StageInTransaction(node, tx, key, "committed", ct);

        Task<(TimeSpan Elapsed, string Value)>? read = null;
        HLCTimestamp snapshot = HLCTimestamp.Zero;

        // Runs after the probe claimed the intent and before the bundle is proposed; only the one-phase path
        // runs it, so a transaction that took the two-phase flow fails the test instead of passing it.
        finalizer.TestAfterReadSetValidationHook = async hookCt =>
        {
            finalizer.TestAfterReadSetValidationHook = null;

            snapshot = Now(manager);
            read = PointRead(node, key, snapshot, ct);

            // The read cannot finish before the bundle applies: it holds no more than this bound here.
            await Task.WhenAny(read, Task.Delay(300, hookCt));
            Assert.False(read.IsCompleted, "a read over a claimed intent was served before the commit applied");
        };

        KeyValueResponseType committed;
        try
        {
            committed = await Commit(node, tx, ct);
        }
        finally
        {
            finalizer.TestAfterReadSetValidationHook = null;
        }

        Assert.NotNull(read);
        Assert.Equal(KeyValueResponseType.Committed, committed);

        (_, string value) = await read;
        Assert.Equal("committed", value);

        (_, string again) = await PointRead(node, key, snapshot, ct);
        Assert.Equal("committed", again);
    }
}
