using System.Text;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Yielding transactions: a transaction started with <see cref="TransactionConflictPolicy.Yield"/> loses every
/// point-key write-intent conflict with a transaction that does not yield. The foreground transaction takes the
/// key over instead of being denied, and the yielding transaction reliably fails — at its next operation on the
/// lost key, or at its finalize pin — and never commits a write to a key it lost.
///
/// These drive <see cref="KahunaManager"/> through the register-remote path (a coordinator key and a
/// per-operation id), because that is the only path that carries the coordinator's authoritative policy to the
/// participant intent. A single embedded node is enough: the takeover, the pin, and the abort all run in the
/// key's actor turn on that node.
/// </summary>
public sealed class TestYieldingTransactions
{
    private readonly ILoggerFactory loggerFactory;

    public TestYieldingTransactions(ITestOutputHelper outputHelper)
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
        await node.WaitForLeaderForKeyAsync("yield", ct);
        return node;
    }

    private static async Task<TransactionHandle> StartTx(
        KahunaManager kahuna, string coordinatorKey, TransactionConflictPolicy policy,
        KeyValueTransactionLocking locking, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = locking,
                ConflictPolicy = policy
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> Acquire(
        KahunaManager kahuna, TransactionHandle handle, string key, CancellationToken ct)
        => kahuna.LocateAndTryAcquireExclusiveLock(
            handle.TransactionId, key, 60_000, KeyValueDurability.Persistent, ct,
            handle.CoordinatorKey, TransactionOperationId.NewRandom());

    private static Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> AcquireWithLease(
        KahunaManager kahuna, TransactionHandle handle, string key, int expiresMs, CancellationToken ct)
        => kahuna.LocateAndTryAcquireExclusiveLock(
            handle.TransactionId, key, expiresMs, KeyValueDurability.Persistent, ct,
            handle.CoordinatorKey, TransactionOperationId.NewRandom());

    private static Task<(KeyValueResponseType, long, HLCTimestamp)> Set(
        KahunaManager kahuna, TransactionHandle handle, string key, string value, CancellationToken ct)
        => kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom());

    /// <summary>
    /// Runs the finalize pin against one key for the transaction that owns it, the way the coordinator does at
    /// the start of a yielding transaction's commit. This is the pin seam: it drives the same actor-turn path
    /// (<c>TryCheckWriteIntentHandler</c> with <see cref="KeyValueConflictChecks.PinOwnIntent"/>) that the real
    /// finalize uses, so a test can order a pin and a takeover on one key deterministically. Answers
    /// <c>Aborted</c> when the key was already taken over, and a non-conflict type when the intent was pinned.
    /// </summary>
    private static async Task<KeyValueResponseType> Pin(
        KahunaManager kahuna, TransactionHandle handle, string key, CancellationToken ct)
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results =
            await kahuna.LocateAndTryCheckManyWriteIntents(
                handle.TransactionId,
                [new KeyValueConflictProbe(key, KeyValueDurability.Persistent, KeyValueConflictChecks.PinOwnIntent)],
                ct);
        Assert.Single(results);
        return results[0].type;
    }

    // ── Acceptance criterion 1: a normal pessimistic acquire takes over an un-pinned yielding intent ──

    [Fact]
    public async Task NormalAcquire_TakesOverYieldingLock_AndTheYieldingTransactionLosesTheKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "yield/lock/1";

        TransactionHandle yielding = await StartTx(kahuna, "yield/lock/1/y", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType lockType, _, _, _) = await Acquire(kahuna, yielding, key, ct);
        Assert.Equal(KeyValueResponseType.Locked, lockType);

        TransactionHandle foreground = await StartTx(kahuna, "yield/lock/1/n", TransactionConflictPolicy.Normal, KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType foregroundType, _, _, _) = await Acquire(kahuna, foreground, key, ct);

        // The foreground transaction takes the key over rather than being denied.
        Assert.Equal(KeyValueResponseType.Locked, foregroundType);

        // The yielding transaction learns of the loss on its next operation on the key.
        (KeyValueResponseType retryType, _, _, _) = await Acquire(kahuna, yielding, key, ct);
        Assert.Equal(KeyValueResponseType.Aborted, retryType);
    }

    // ── Acceptance criterion 2: a normal set proceeds and the yielding transaction's commit aborts ──

    [Fact]
    public async Task NormalSet_TakesOverYieldingSet_AndTheYieldingCommitAborts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "yield/set/1";

        TransactionHandle yielding = await StartTx(kahuna, "yield/set/1/y", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Set, (await Set(kahuna, yielding, key, "from-yielding", ct)).Item1);

        TransactionHandle foreground = await StartTx(kahuna, "yield/set/1/n", TransactionConflictPolicy.Normal, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Set, (await Set(kahuna, foreground, key, "from-foreground", ct)).Item1);

        (KeyValueResponseType foregroundCommit, _) = await kahuna.LocateAndCommitTransaction(foreground, ct);
        Assert.Equal(KeyValueResponseType.Committed, foregroundCommit);

        (KeyValueResponseType yieldingCommit, _) = await kahuna.LocateAndCommitTransaction(yielding, ct);
        Assert.Equal(KeyValueResponseType.Aborted, yieldingCommit);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal("from-foreground", Encoding.UTF8.GetString(entry!.Value!));
    }

    // ── Acceptance criterion 3: blind-write race — the loser aborts at the pin, never touching the key again ──

    [Fact]
    public async Task BlindWriteRace_LoserAbortsAtThePin_AndTheWinnersValueIsTheHead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "yield/blind/1";

        // The yielding transaction stages a blind write and then never touches the key again.
        TransactionHandle yielding = await StartTx(kahuna, "yield/blind/1/y", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Set, (await Set(kahuna, yielding, key, "stale", ct)).Item1);

        // The foreground transaction takes the key over and commits.
        TransactionHandle foreground = await StartTx(kahuna, "yield/blind/1/n", TransactionConflictPolicy.Normal, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Set, (await Set(kahuna, foreground, key, "winner", ct)).Item1);
        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(foreground, ct)).Item1);

        // The yielding commit aborts at the pin even though it never re-touched the key.
        Assert.Equal(KeyValueResponseType.Aborted, (await kahuna.LocateAndCommitTransaction(yielding, ct)).Item1);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal("winner", Encoding.UTF8.GetString(entry!.Value!));
    }

    // ── Acceptance criterion 6: two yielding transactions conflict exactly as two normal ones ──

    [Fact]
    public async Task TwoYieldingTransactions_DoNotStealFromEachOther()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "yield/pair/1";

        TransactionHandle first = await StartTx(kahuna, "yield/pair/1/a", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Locked, (await Acquire(kahuna, first, key, ct)).Item1);

        TransactionHandle second = await StartTx(kahuna, "yield/pair/1/b", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType secondType, _, _, _) = await Acquire(kahuna, second, key, ct);

        // A yielding transaction never takes over another yielding transaction's intent, so the second is
        // denied on the live holder exactly as a second normal transaction would be.
        Assert.NotEqual(KeyValueResponseType.Locked, secondType);
        Assert.Equal(KeyValueResponseType.AlreadyLocked, secondType);
    }

    // ── Acceptance criterion 7: a yielding transaction cannot hold a prefix or range lock ──

    [Fact]
    public async Task YieldingTransaction_PrefixLockAndRangeLock_AreRejected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        TransactionHandle yielding = await StartTx(kahuna, "yield/predlock/y", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);

        KeyValueResponseType prefixType = await kahuna.LocateAndTryAcquireExclusivePrefixLock(
            yielding.TransactionId, "yieldpfx", 60_000, KeyValueDurability.Persistent, ct,
            yielding.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Errored, prefixType);

        (KeyValueResponseType rangeType, _) = await kahuna.LocateAndTryAcquireRangeLock(
            yielding.TransactionId, "yieldrng", "yieldrng/a", true, "yieldrng/z", true, 60_000,
            KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            yielding.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Errored, rangeType);
    }

    [Fact]
    public async Task NormalTransaction_PrefixLock_StillWorks()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        TransactionHandle normal = await StartTx(kahuna, "yield/predlock/n", TransactionConflictPolicy.Normal, KeyValueTransactionLocking.Pessimistic, ct);

        KeyValueResponseType prefixType = await kahuna.LocateAndTryAcquireExclusivePrefixLock(
            normal.TransactionId, "normalpfx", 60_000, KeyValueDurability.Persistent, ct,
            normal.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, prefixType);
    }

    // ── A yielding transaction with no conflict commits normally ──

    [Fact]
    public async Task YieldingTransaction_WithNoConflict_Commits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "yield/clean/1";

        TransactionHandle yielding = await StartTx(kahuna, "yield/clean/1/y", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Set, (await Set(kahuna, yielding, key, "kept", ct)).Item1);
        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(yielding, ct)).Item1);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal("kept", Encoding.UTF8.GetString(entry!.Value!));
    }

    // ── Acceptance criterion 4: pin race — takeover-first aborts the pin, pin-first makes the requester wait ──

    [Fact]
    public async Task PinRace_TakeoverFirst_TheLaterPinAborts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "yield/pinrace/takeover-first";

        TransactionHandle yielding = await StartTx(kahuna, key + "/y", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Locked, (await Acquire(kahuna, yielding, key, ct)).Item1);

        // The foreground writer takes the key over before the yielding transaction's pin runs.
        TransactionHandle foreground = await StartTx(kahuna, key + "/n", TransactionConflictPolicy.Normal, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Locked, (await Acquire(kahuna, foreground, key, ct)).Item1);

        // The pin that runs afterwards finds the key lost and aborts.
        Assert.Equal(KeyValueResponseType.Aborted, await Pin(kahuna, yielding, key, ct));
    }

    [Fact]
    public async Task PinRace_PinFirst_TheForegroundWaitsThenSucceedsAfterTheDecision()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "yield/pinrace/pin-first";

        TransactionHandle yielding = await StartTx(kahuna, key + "/y", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Locked, (await Acquire(kahuna, yielding, key, ct)).Item1);

        // The pin runs first and claims the intent; it is not a loss, so it does not abort.
        Assert.NotEqual(KeyValueResponseType.Aborted, await Pin(kahuna, yielding, key, ct));

        // A foreground acquire now meets a pinned intent. It is told to wait, not denied: it never takes the
        // key over, and it never reports AlreadyLocked. Its bounded wait then expires with the retryable
        // MustRetry, because the yielding transaction's decision has not landed yet.
        TransactionHandle foreground = await StartTx(kahuna, key + "/n", TransactionConflictPolicy.Normal, KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType waitingType, _, _, _) = await Acquire(kahuna, foreground, key, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, waitingType);

        // The yielding transaction's decision lands (here, a rollback releases the pinned intent).
        Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(yielding, ct));

        // The foreground acquire now succeeds.
        (KeyValueResponseType afterType, _, _, _) = await Acquire(kahuna, foreground, key, ct);
        Assert.Equal(KeyValueResponseType.Locked, afterType);
    }

    // ── Acceptance criterion 5: a pinned intent whose coordinator dies is released by its lease ──

    [Fact]
    public async Task PinnedIntent_WhoseCoordinatorNeverDecides_IsReleasedByItsLease_AndTheWaiterSucceeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "yield/pinned-lease/1";

        // The yielding transaction takes a short-leased lock and pins it, then never decides — its coordinator
        // has effectively died. The lease is well under the acquire wait budget, so a waiting foreground
        // acquire outlives it.
        TransactionHandle yielding = await StartTx(kahuna, key + "/y", TransactionConflictPolicy.Yield, KeyValueTransactionLocking.Pessimistic, ct);
        Assert.Equal(KeyValueResponseType.Locked, (await AcquireWithLease(kahuna, yielding, key, 1_000, ct)).Item1);
        Assert.NotEqual(KeyValueResponseType.Aborted, await Pin(kahuna, yielding, key, ct));

        // The foreground acquire waits out the pinned intent; once the lease passes, the orphaned intent stops
        // holding the key and the acquire takes it.
        TransactionHandle foreground = await StartTx(kahuna, key + "/n", TransactionConflictPolicy.Normal, KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType afterType, _, _, _) = await Acquire(kahuna, foreground, key, ct);
        Assert.Equal(KeyValueResponseType.Locked, afterType);
    }

    // ── Acceptance criterion 8: the wire decodes an absent or unknown policy as Normal ──

    [Fact]
    public void ConflictPolicyWire_RoundTripsAndDecodesUnknownAsNormal()
    {
        Assert.Equal(TransactionConflictPolicy.Normal, TransactionConflictPolicyWire.FromGrpc(TransactionConflictPolicyWire.ToGrpc(TransactionConflictPolicy.Normal)));
        Assert.Equal(TransactionConflictPolicy.Yield, TransactionConflictPolicyWire.FromGrpc(TransactionConflictPolicyWire.ToGrpc(TransactionConflictPolicy.Yield)));

        // The zero value an old peer sends decodes as Normal, never Yield.
        Assert.Equal(TransactionConflictPolicy.Normal, TransactionConflictPolicyWire.FromGrpc(GrpcTransactionConflictPolicy.TransactionConflictPolicyUnspecified));

        // An unknown ordinal from a newer peer also decodes as Normal, never Yield.
        Assert.Equal(TransactionConflictPolicy.Normal, TransactionConflictPolicyWire.FromGrpc((GrpcTransactionConflictPolicy)999));
    }
}
