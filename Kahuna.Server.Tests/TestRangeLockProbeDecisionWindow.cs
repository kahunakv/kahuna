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
/// Covers the window between a writer's commit-time range-lock probe and its commit decision.
///
/// <para>A Shared range-lock acquire deliberately does not conflict with a foreign write intent: it defers to
/// the writer's commit-time probe (<c>CheckCommitConflicts</c>), which aborts the writer when a foreign range
/// lock covers a key it wrote. That probe runs inside the read-set validation, and the decision comes after
/// it. A reader whose Shared lock lands between the two is never checked: it holds its lock, reads the value
/// committed before the writer, and the writer commits anyway. A strict-2PL reader must not both keep that
/// lock and miss a write that committed while it held it.</para>
///
/// <para>This is the interleaving behind write-skew cycles Elle found in CamusDB's serializable, pessimistic
/// transactions (Caraxes <c>append-rw-8keys</c>): CamusDB takes a Shared point range lock for every
/// serializable read. The finalizer's test hook places the reader inside the window deterministically; no
/// external caller can time it.</para>
/// </summary>
public sealed class TestRangeLockProbeDecisionWindow
{
    private const int RangeLockExpiresMs = 30_000;

    /// <summary>How long the reader may wait inside the hook. The writer is parked in its finalize until the hook
    /// returns, so a reader that waits for the writer can never finish; past this bound the wait itself is the
    /// answer, and it is the correct one.</summary>
    private static readonly TimeSpan ReaderBound = TimeSpan.FromSeconds(3);

    private readonly ILoggerFactory loggerFactory;

    public TestRangeLockProbeDecisionWindow(ITestOutputHelper outputHelper)
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
        await node.WaitForLeaderForKeyAsync("rlw/seed", ct);

        return node;
    }

    private static async Task Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>What the reader got inside the window.</summary>
    private sealed class ReaderObservation
    {
        public KeyValueResponseType? LockType;

        public KeyValueResponseType? ReadType;

        public string? ReadValue;

        /// <summary>The reader was still waiting when <see cref="ReaderBound"/> ran out.</summary>
        public bool Blocked;

        public override string ToString() =>
            $"lock={LockType?.ToString() ?? "-"}, read={ReadType?.ToString() ?? "-"}, value={ReadValue ?? "null"}, blocked={Blocked}";
    }

    /// <summary>
    /// The one-phase path: a transaction whose whole write set is one key validates its read set before the bundle
    /// is proposed.
    /// </summary>
    [Fact]
    public async Task SharedLockInsideProbeDecisionWindow_OnePhase_IsNotGrantedOverAWriteThatCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string runId = Guid.NewGuid().ToString("N")[..8];
        string bucket = $"rlw-{runId}-a";
        string key = bucket + "/k";

        await RunWindowScenario(node, bucket, key, companion: null, ct);
    }

    /// <summary>
    /// The 2PC path: a second written key on another partition keeps the transaction off the one-phase bundle, so
    /// the read-set validation runs after every prepare is durable.
    /// </summary>
    [Fact]
    public async Task SharedLockInsideProbeDecisionWindow_TwoPhase_IsNotGrantedOverAWriteThatCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string runId = Guid.NewGuid().ToString("N")[..8];
        string bucket = $"rlw-{runId}-a";
        string key = bucket + "/k";

        await RunWindowScenario(node, bucket, key, CompanionOnAnotherPartition(node, runId, key), ct);
    }

    /// <summary>
    /// The absent-key form of the cycle, on the one-phase path: neither key exists. T1 reads <c>b</c> and finds it
    /// absent, then creates the key; inside T1's window T2 reads the key, then creates <c>b</c>. If both commit, each
    /// missed the other's insert. This is the shape Elle reported in Caraxes <c>append-rw-contended</c>: T1 appended
    /// to list 549 and read list 548 as absent, T2 created 548 and read 549 as absent, and both committed.
    ///
    /// <para>A read of an absent key takes the same Shared point lock, but the key has no resident entry until a
    /// writer plants one, so this is the case where the acquire must find the writer's intent on the key it is
    /// creating.</para>
    /// </summary>
    [Fact]
    public async Task SharedLockInsideProbeDecisionWindow_OnePhase_AbsentKeys_IsNotGrantedOverAnInsertThatCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string runId = Guid.NewGuid().ToString("N")[..8];
        string bucket = $"rlw-{runId}-a";
        string key = bucket + "/k";

        await RunWindowScenario(node, bucket, key, companion: null, ct, absentKeys: true);
    }

    /// <summary>The absent-key form of the cycle on the 2PC path. See the one-phase form.</summary>
    [Fact]
    public async Task SharedLockInsideProbeDecisionWindow_TwoPhase_AbsentKeys_IsNotGrantedOverAnInsertThatCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string runId = Guid.NewGuid().ToString("N")[..8];
        string bucket = $"rlw-{runId}-a";
        string key = bucket + "/k";

        await RunWindowScenario(node, bucket, key, CompanionOnAnotherPartition(node, runId, key), ct, absentKeys: true);
    }

    /// <summary>A read value for a failure message: quoted when present, <c>absent</c> when the key did not exist.</summary>
    private static string Shown(string? value) => value is null ? "absent" : $"'{value}'";

    /// <summary>
    /// A second key on a different partition from <paramref name="key"/>. Written by T1, it keeps the transaction off
    /// the one-phase bundle. Keys route by their parent bucket, so the candidates vary the bucket, not the leaf.
    /// </summary>
    private static string CompanionOnAnotherPartition(EmbeddedKahunaNode node, string runId, string key)
    {
        int keyPartition = node.Raft.GetPartitionKey(key);
        for (int i = 0; i < 256; i++)
        {
            string candidate = $"rlw-{runId}-b{i}/k";
            if (node.Raft.GetPartitionKey(candidate) != keyPartition)
                return candidate;
        }

        Assert.Fail($"no key on a partition other than {keyPartition} among 256 candidates");
        return "";
    }

    /// <summary>
    /// Control for the two window tests: the same transactions, but T2 takes its lock and reads before T1 commits,
    /// so T1's commit-time probe sees the lock. The scenario must then end without the cycle. A failure here
    /// means the scenario itself is wrong, and the window tests prove nothing.
    /// </summary>
    [Fact]
    public async Task Control_SharedLockBeforeProbe_NoWriteSkew()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string runId = Guid.NewGuid().ToString("N")[..8];
        string bucket = $"rlw-{runId}-a";
        string key = bucket + "/k";

        await RunWindowScenario(node, bucket, key, companion: null, ct, readInsideWindow: false);
    }

    /// <summary>
    /// The write-skew cycle, built on the window. Both transactions are pessimistic and read under Shared point
    /// range locks, exactly as CamusDB serializable transactions do.
    ///
    /// <list type="number">
    /// <item>T1 reads <c>b</c>, then locks and writes <paramref name="key"/> (and <paramref name="companion"/>
    /// when given), then commits.</item>
    /// <item>Inside T1's finalize, after its commit-time probe passed, T2 takes a Shared lock on
    /// <paramref name="key"/> and reads it.</item>
    /// <item>After T1 is decided, T2 locks and writes <c>b</c>, then commits.</item>
    /// </list>
    ///
    /// Correct outcomes: T2 is refused or blocked in the window, or reads T1's value, or one of the two aborts.
    /// The defect: both commit, T2 read <paramref name="key"/> from before T1, and T1 read <c>b</c> from before T2
    /// — a G2 cycle no serial order explains.
    ///
    /// <para>With <paramref name="absentKeys"/>, <paramref name="key"/> and <c>b</c> are not seeded: "from before"
    /// then means "absent", and each write creates its key.</para>
    /// </summary>
    private static async Task RunWindowScenario(
        EmbeddedKahunaNode node, string bucket, string key, string? companion, CancellationToken ct,
        bool readInsideWindow = true, bool absentKeys = false)
    {
        IKahuna kahuna = node.Kahuna;

        // Same bucket as the key, so it adds no partition to the write sets.
        string other = bucket + "/b";

        if (!absentKeys)
        {
            await Seed(kahuna, key, "1", ct);
            await Seed(kahuna, other, "1", ct);
        }
        if (companion is not null)
            await Seed(kahuna, companion, "1", ct);

        (KeyValueResponseType startType, TransactionHandle t1) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = key + "/t1",
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType t1LockB, _) = await kahuna.LocateAndTryAcquireRangeLock(
            t1.TransactionId, bucket, other, true, other, true, RangeLockExpiresMs,
            KeyValueDurability.Persistent, RangeLockMode.Shared, ct,
            t1.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, t1LockB);

        (KeyValueResponseType t1ReadType, ReadOnlyKeyValueEntry? t1ReadEntry) = await kahuna.LocateAndTryGetValue(
            t1.TransactionId, other, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: t1.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(absentKeys ? KeyValueResponseType.DoesNotExist : KeyValueResponseType.Get, t1ReadType);
        string? t1SawOther = t1ReadEntry?.Value is null ? null : Encoding.UTF8.GetString(t1ReadEntry.Value);

        foreach (string written in companion is null ? [key] : new[] { key, companion })
        {
            (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                t1.TransactionId, written, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: t1.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, lockType);

            (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                t1.TransactionId, written, "2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct,
                coordinatorKey: t1.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, setType);
        }

        (KeyValueResponseType t2Start, TransactionHandle t2) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = key + "/t2",
                Locking = KeyValueTransactionLocking.Pessimistic,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, t2Start);

        ReaderObservation reader = new();
        bool hookRan = false;

        KahunaManager manager = (KahunaManager)kahuna;
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;

        async Task ReadKeyAsT2(CancellationToken hookCt)
        {
            using CancellationTokenSource bound = CancellationTokenSource.CreateLinkedTokenSource(hookCt);
            bound.CancelAfter(ReaderBound);

            try
            {
                (KeyValueResponseType lockType, _) = await kahuna.LocateAndTryAcquireRangeLock(
                    t2.TransactionId, bucket, key, true, key, true, RangeLockExpiresMs,
                    KeyValueDurability.Persistent, RangeLockMode.Shared, bound.Token,
                    t2.CoordinatorKey, TransactionOperationId.NewRandom()).WaitAsync(bound.Token);
                reader.LockType = lockType;

                if (lockType != KeyValueResponseType.Locked)
                    return;

                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
                    t2.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, bound.Token,
                    coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom()).WaitAsync(bound.Token);
                reader.ReadType = readType;
                reader.ReadValue = entry?.Value is null ? null : Encoding.UTF8.GetString(entry.Value);
            }
            catch (OperationCanceledException) when (!hookCt.IsCancellationRequested)
            {
                reader.Blocked = true;
            }
        }

        if (readInsideWindow)
        {
            // One-shot: the hook clears itself so no other finalize replays it.
            finalizer.TestAfterReadSetValidationHook = async hookCt =>
            {
                finalizer.TestAfterReadSetValidationHook = null;
                hookRan = true;
                await ReadKeyAsT2(hookCt);
            };
        }
        else
        {
            // Control: the same read before T1 commits, so T1's commit-time probe can see T2's lock.
            await ReadKeyAsT2(ct);
        }

        KeyValueResponseType commitType;
        try
        {
            (commitType, _) = await kahuna.LocateAndCommitTransaction(t1, ct);
        }
        finally
        {
            finalizer.TestAfterReadSetValidationHook = null;
        }

        if (readInsideWindow)
        {
            Assert.True(hookRan, $"the finalize never reached the probe→decision window (T1 answered {commitType})");

            // The reader-side half of strict two-phase locking: inside the window T1's write intent on the key
            // is live and undecided, so T2's Shared acquire is refused and names T1 as the holder. Asserted
            // before the cycle check so a regression that grants the lock is reported as such, not only as
            // its consequence.
            Assert.Equal(KeyValueResponseType.AlreadyLocked, reader.LockType);
        }

        // T2 continues only if it is still alive and read something; T1 is decided, so its locks are gone.
        KeyValueResponseType? t2WriteType = null;
        KeyValueResponseType? t2CommitType = null;
        if (reader.LockType == KeyValueResponseType.Locked
            && reader.ReadType is KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist)
        {
            (KeyValueResponseType t2LockB, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                t2.TransactionId, other, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            if (t2LockB == KeyValueResponseType.Locked)
            {
                (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                    t2.TransactionId, other, "3"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct,
                    coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
                t2WriteType = setType;

                if (setType == KeyValueResponseType.Set)
                    (t2CommitType, _) = await kahuna.LocateAndCommitTransaction(t2, ct);
            }
            else
            {
                t2WriteType = t2LockB;
            }
        }

        if (t2CommitType != KeyValueResponseType.Committed)
            await kahuna.LocateAndRollbackTransaction(t2, ct);

        bool t1Committed = commitType == KeyValueResponseType.Committed;
        bool t2Committed = t2CommitType == KeyValueResponseType.Committed;

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"partitions key={node.Raft.GetPartitionKey(key)} anchor={node.Raft.GetPartitionKey(t1.CoordinatorKey)} " +
            $"companion={(companion is null ? "-" : node.Raft.GetPartitionKey(companion).ToString())}; " +
            $"window={readInsideWindow} absent={absentKeys} hookRan={hookRan} reader: {reader}; T1: {commitType}; " +
            $"T2 write: {t2WriteType?.ToString() ?? "-"}, commit: {t2CommitType?.ToString() ?? "-"}");

        // "Missed the other's write": the seeded value, or no value at all when the keys started absent.
        string? before = absentKeys ? null : "1";
        bool t2MissedT1 = reader.ReadType is KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist
                          && reader.ReadValue == before;

        Assert.False(t1Committed && t2Committed && t2MissedT1 && t1SawOther == before,
            $"write skew: T1 read {other}={Shown(t1SawOther)} and wrote {key}; T2 read {key}={Shown(reader.ReadValue)} inside " +
            $"T1's probe→decision window and wrote {other}; both committed. Reader: {reader}. " +
            $"T1: {commitType}. T2 write: {t2WriteType?.ToString() ?? "-"}, commit: {t2CommitType?.ToString() ?? "-"}.");
    }
}
