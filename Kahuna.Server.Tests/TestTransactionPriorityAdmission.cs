using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The admission gate as a running node sees it: transactions submitted through the real script and
/// interactive entry points, against a node configured with a genuine concurrency ceiling.
///
/// <para>The component tests cover the orderer's own policy. What these cover is the wiring — that the gate is
/// actually in the path, that its slot is returned on every way a transaction can end (including the ways that
/// do not involve the client cooperating), and that a node left in the default configuration behaves exactly as
/// it did before the gate existed.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTransactionPriorityAdmission
{
    private readonly ILoggerFactory loggerFactory;

    public TestTransactionPriorityAdmission(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaOptions BaseOptions(int maxConcurrentTransactions = 0, int maxConcurrentSessions = 0, int maxQueued = 0) => new()
    {
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        PartitionExecutorPoolSize = 1,
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 4,
        MaxConcurrentTransactions = maxConcurrentTransactions,
        MaxConcurrentSessions = maxConcurrentSessions,
        TransactionPriorityMaxQueued = maxQueued
    };

    private static KeyValuesManager KeyValuesOf(EmbeddedKahunaNode node) => ((KahunaManager)node.Kahuna).KeyValues;

    /// <summary>Spins until <paramref name="condition"/> holds, so a test waits on the state it actually cares
    /// about instead of on a sleep long enough to "probably" be sufficient.</summary>
    private static async Task WaitUntil(Func<bool> condition, string what, CancellationToken cancellationToken, int timeoutSeconds = 15)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);

        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                Assert.Fail($"timed out waiting for {what}");

            await Task.Delay(10, cancellationToken);
        }
    }

    [Fact]
    public async Task SaturatedNode_StartsHighPriorityWorkBeforeQueuedLowPriorityWork()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // One slot, so exactly one transaction runs at a time and completion order is start order.
        await using EmbeddedKahunaNode node = new(BaseOptions(maxConcurrentTransactions: 1), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("prio/blocker", ct);
        await node.WaitForLeaderForKeyAsync("prio/low", ct);
        await node.WaitForLeaderForKeyAsync("prio/high", ct);

        TransactionPriorityOrderer orderer = KeyValuesOf(node).scriptOrderer;

        // Occupies the only slot for long enough that both contenders are queued behind it.
        Task<KeyValueTransactionResult> blocker = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SLEEP 1500 SET `prio/blocker` 'v' COMMIT END"), null, null);

        await WaitUntil(() => orderer.InFlight == 1, "the blocking transaction to occupy the slot", ct);

        // Low is submitted first so that arrival order alone would have it start first.
        Task<KeyValueTransactionResult> low = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN (priority=\"low\") SET `prio/low` 'v' COMMIT END"), null, null);

        await WaitUntil(() => orderer.QueuedAt(TransactionPriority.Low) == 1, "the low-priority transaction to queue", ct);

        Task<KeyValueTransactionResult> high = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN (priority=\"high\") SET `prio/high` 'v' COMMIT END"), null, null);

        await WaitUntil(() => orderer.QueuedAt(TransactionPriority.High) == 1, "the high-priority transaction to queue", ct);

        await blocker;

        Task firstToFinish = await Task.WhenAny(low, high);

        // With a single slot the loser cannot even have started, so finishing first means starting first.
        Assert.Same(high, firstToFinish);

        Assert.Equal(KeyValueResponseType.Set, (await high).Type);
        Assert.Equal(KeyValueResponseType.Set, (await low).Type);
    }

    [Fact]
    public async Task WithHeadroom_TransactionsAreNeverDelayedByTheGate()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(BaseOptions(maxConcurrentTransactions: 8), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("head/k", ct);

        TransactionPriorityOrderer orderer = KeyValuesOf(node).scriptOrderer;

        for (int i = 0; i < 4; i++)
        {
            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN (priority=\"background\") SET `head/k{i}` 'v' COMMIT END"), null, null);

            Assert.Equal(KeyValueResponseType.Set, result.Type);
        }

        // Priority is recorded, but below the ceiling nothing ever waited for it.
        Assert.Equal(0, orderer.Queued);
        Assert.Equal(0, orderer.MaxQueueDepthAt(TransactionPriority.Background));
        Assert.Equal(4, orderer.AdmittedAt(TransactionPriority.Background));
    }

    [Fact]
    public async Task TransactionThatFailsMidExecution_ReturnsItsSlot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(BaseOptions(maxConcurrentTransactions: 1), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("fault/k", ct);

        TransactionPriorityOrderer orderer = KeyValuesOf(node).scriptOrderer;

        // A script that cannot execute: it holds a slot, then fails. If the failure path skipped the release,
        // the node would be permanently down to zero capacity.
        KeyValueTransactionResult failed = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN LET x = to_int('not-a-number') SET `fault/k` x COMMIT END"), null, null);

        Assert.NotEqual(KeyValueResponseType.Set, failed.Type);

        await WaitUntil(() => orderer.InFlight == 0, "the failed transaction to return its slot", ct);

        // The real proof: the node still admits work. With a ceiling of one, a leaked slot would hang here.
        KeyValueTransactionResult after = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `fault/k` 'v' COMMIT END"), null, null).WaitAsync(TimeSpan.FromSeconds(20), ct);

        Assert.Equal(KeyValueResponseType.Set, after.Type);
        Assert.Equal(0, orderer.InFlight);
    }

    [Fact]
    public async Task IdleInteractiveSessions_DoNotStarveScriptTransactions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // The two gates are deliberately separate. If sessions and script transactions shared one pool, the two
        // sessions opened below would occupy every slot and this node would stall on the first script.
        await using EmbeddedKahunaNode node = new(BaseOptions(maxConcurrentTransactions: 1, maxConcurrentSessions: 2), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("idle/k", ct);

        KeyValuesManager keyValues = KeyValuesOf(node);

        (KeyValueResponseType firstType, _) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { Timeout = 30_000 }, ct);
        (KeyValueResponseType secondType, _) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { Timeout = 30_000 }, ct);

        Assert.Equal(KeyValueResponseType.Set, firstType);
        Assert.Equal(KeyValueResponseType.Set, secondType);
        Assert.Equal(2, keyValues.sessionOrderer.InFlight);

        // Both session slots are held by sessions that will never do anything, and script work proceeds anyway.
        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `idle/k` 'v' COMMIT END"), null, null).WaitAsync(TimeSpan.FromSeconds(20), ct);

        Assert.Equal(KeyValueResponseType.Set, result.Type);
        Assert.Equal(0, keyValues.scriptOrderer.InFlight);
    }

    [Fact]
    public async Task CommittedAndRolledBackSessions_ReturnTheirSlots()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(BaseOptions(maxConcurrentSessions: 1), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("sess/k", ct);

        TransactionPriorityOrderer orderer = KeyValuesOf(node).sessionOrderer;

        // Only one session may be open at a time, so each of these can only succeed if the previous one gave
        // its slot back.
        (KeyValueResponseType startedType, Shared.KeyValue.TransactionHandle committed) =
            await node.Kahuna.LocateAndStartTransaction(new KeyValueTransactionOptions { Timeout = 30_000 }, ct);
        Assert.Equal(KeyValueResponseType.Set, startedType);

        (KeyValueResponseType commitType, _) = await node.Kahuna.LocateAndCommitTransaction(committed, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);
        await WaitUntil(() => orderer.InFlight == 0, "the committed session to return its slot", ct);

        (KeyValueResponseType secondType, Shared.KeyValue.TransactionHandle rolledBack) =
            await node.Kahuna.LocateAndStartTransaction(new KeyValueTransactionOptions { Timeout = 30_000 }, ct);
        Assert.Equal(KeyValueResponseType.Set, secondType);

        Assert.Equal(KeyValueResponseType.RolledBack, await node.Kahuna.LocateAndRollbackTransaction(rolledBack, ct));
        await WaitUntil(() => orderer.InFlight == 0, "the rolled-back session to return its slot", ct);

        (KeyValueResponseType thirdType, _) =
            await node.Kahuna.LocateAndStartTransaction(new KeyValueTransactionOptions { Timeout = 30_000 }, ct);
        Assert.Equal(KeyValueResponseType.Set, thirdType);
    }

    [Fact]
    public async Task AbandonedSession_ReturnsItsSlotWhenTheReaperReclaimsIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        EmbeddedKahunaOptions options = BaseOptions(maxConcurrentSessions: 1);
        options.CollectionInterval = TimeSpan.FromSeconds(1);

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("reap/k", ct);

        TransactionPriorityOrderer orderer = KeyValuesOf(node).sessionOrderer;

        // Started and then abandoned: no commit, no rollback, no client. Capacity has to come back without any
        // cooperation from the caller, or a crashed client would cost the node a slot permanently.
        (KeyValueResponseType startedType, _) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { Timeout = 1_000 }, ct);

        Assert.Equal(KeyValueResponseType.Set, startedType);
        Assert.Equal(1, orderer.InFlight);

        // The reaper reclaims a session only once its timeout plus the fixed reap grace has elapsed, so this
        // necessarily takes tens of seconds of wall clock — the price of observing the reclamation path for
        // real rather than simulating it.
        await WaitUntil(() => orderer.InFlight == 0, "the reaper to reclaim the abandoned session's slot", ct, timeoutSeconds: 90);

        (KeyValueResponseType afterType, _) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { Timeout = 5_000 }, ct);

        Assert.Equal(KeyValueResponseType.Set, afterType);
    }

    [Fact]
    public async Task DefaultConfiguration_LeavesTheGateInert()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // No ceilings configured — the shipped default. Priority must be recorded and otherwise inconsequential.
        await using EmbeddedKahunaNode node = new(BaseOptions(), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("pass/k", ct);

        KeyValuesManager keyValues = KeyValuesOf(node);

        Assert.True(keyValues.scriptOrderer.IsPassThrough);
        Assert.True(keyValues.sessionOrderer.IsPassThrough);

        List<Task<KeyValueTransactionResult>> concurrent = [];

        for (int i = 0; i < 12; i++)
            concurrent.Add(node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN (priority=\"background\") SET `pass/k{i}` 'v' COMMIT END"), null, null));

        KeyValueTransactionResult[] results = await Task.WhenAll(concurrent).WaitAsync(TimeSpan.FromSeconds(60), ct);

        Assert.All(results, r => Assert.Equal(KeyValueResponseType.Set, r.Type));

        // Twelve background transactions ran at once against a ceiling that does not exist; nothing queued.
        Assert.Equal(0, keyValues.scriptOrderer.Queued);
        Assert.Equal(12, keyValues.scriptOrderer.AdmittedAt(TransactionPriority.Background));
    }

    [Fact]
    public async Task ASaturatedNodeWithAFullQueue_ShedsLoadRetryablyInsteadOfGrowing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // One slot and room for a single waiter. Everything past that must be refused rather than parked, or
        // the queue behind the ceiling becomes the memory problem the ceiling exists to prevent.
        await using EmbeddedKahunaNode node = new(BaseOptions(maxConcurrentTransactions: 1, maxQueued: 1), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("shed/k", ct);

        TransactionPriorityOrderer orderer = KeyValuesOf(node).scriptOrderer;

        Task<KeyValueTransactionResult> blocker = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SLEEP 2000 SET `shed/blocker` 'v' COMMIT END"), null, null);

        await WaitUntil(() => orderer.InFlight == 1, "the blocking transaction to occupy the slot", ct);

        Task<KeyValueTransactionResult> queued = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `shed/queued` 'v' COMMIT END"), null, null);

        await WaitUntil(() => orderer.Queued == 1, "the second transaction to take the only queue slot", ct);

        // The queue is full, so this one is turned away immediately rather than waiting.
        KeyValueTransactionResult shed = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `shed/rejected` 'v' COMMIT END"), null, null)
            .WaitAsync(TimeSpan.FromSeconds(10), ct);

        Assert.Equal(KeyValueResponseType.MustRetry, shed.Type);
        Assert.Equal(1, orderer.RejectedQueueFullAt(TransactionPriority.Normal));

        Assert.Equal(KeyValueResponseType.Set, (await blocker).Type);
        Assert.Equal(KeyValueResponseType.Set, (await queued).Type);

        // Shedding must not have cost the node anything: capacity is intact afterwards.
        await WaitUntil(() => orderer.InFlight == 0 && orderer.Queued == 0, "the node to drain", ct);

        KeyValueTransactionResult after = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `shed/k` 'v' COMMIT END"), null, null).WaitAsync(TimeSpan.FromSeconds(20), ct);

        Assert.Equal(KeyValueResponseType.Set, after.Type);
    }

    [Theory]
    [InlineData(99)]
    [InlineData(-1)]
    public async Task AnOutOfRangeNumericPriority_CannotClaimReservedCapacity(int raw)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // The value a REST payload can carry as a raw number, or an embedded caller as a cast enum. Neither
        // passes through the gRPC conversion that would have normalized it.
        TransactionPriority hostile = (TransactionPriority)raw;

        EmbeddedKahunaOptions options = BaseOptions(maxConcurrentTransactions: 2, maxConcurrentSessions: 2);
        options.TransactionPriorityReservedSlots = 1;

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("hostile/k", ct);

        KeyValuesManager keyValues = KeyValuesOf(node);

        // Script path.
        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `hostile/k` 'v' COMMIT END"), null, null, hostile)
            .WaitAsync(TimeSpan.FromSeconds(20), ct);

        Assert.Equal(KeyValueResponseType.Set, result.Type);

        // It ran as ordinary work: had it been read as Critical it would have been counted there and would
        // have been eligible for the reserved slot.
        Assert.Equal(1, keyValues.scriptOrderer.AdmittedAt(TransactionPriority.Normal));
        Assert.Equal(0, keyValues.scriptOrderer.AdmittedAt(TransactionPriority.Critical));

        // Interactive path: accepted rather than rejected, and likewise as ordinary work.
        (KeyValueResponseType startType, Shared.KeyValue.TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { Timeout = 30_000, Priority = hostile }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        Assert.Equal(1, keyValues.sessionOrderer.AdmittedAt(TransactionPriority.Normal));
        Assert.Equal(0, keyValues.sessionOrderer.AdmittedAt(TransactionPriority.Critical));

        Assert.Equal(KeyValueResponseType.RolledBack, await node.Kahuna.LocateAndRollbackTransaction(handle, ct));
    }

    [Fact]
    public async Task AStandaloneCommandScript_RunsUngatedAndDoesNotConsumeASlot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Documented contract: the gate governs transactions. A single standalone command holds no
        // transaction and is deliberately not admission-gated, so it must run even with the ceiling occupied.
        await using EmbeddedKahunaNode node = new(BaseOptions(maxConcurrentTransactions: 1), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("ungated/k", ct);

        TransactionPriorityOrderer orderer = KeyValuesOf(node).scriptOrderer;

        Task<KeyValueTransactionResult> blocker = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SLEEP 1500 SET `ungated/blocker` 'v' COMMIT END"), null, null);

        await WaitUntil(() => orderer.InFlight == 1, "the blocking transaction to occupy the only slot", ct);

        KeyValueTransactionResult standalone = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("SET `ungated/k` 'v'"), null, null, TransactionPriority.Background)
            .WaitAsync(TimeSpan.FromSeconds(10), ct);

        Assert.Equal(KeyValueResponseType.Set, standalone.Type);

        // It neither waited for nor occupied a slot, and its priority was ignored rather than recorded.
        Assert.Equal(1, orderer.InFlight);
        Assert.Equal(0, orderer.Queued);
        Assert.Equal(0, orderer.AdmittedAt(TransactionPriority.Background));

        await blocker;
    }

    [Fact]
    public async Task AnUnknownPriorityOption_IsRejectedAsMalformedScript()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(BaseOptions(), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("bad/k", ct);

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN (priority=\"urgent\") SET `bad/k` 'v' COMMIT END"), null, null);

        Assert.Equal(KeyValueResponseType.Errored, result.Type);
        Assert.Contains("priority", result.Reason ?? "", StringComparison.OrdinalIgnoreCase);
    }
}
