
using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for the range-lock lease hand-off between the renewal sweep and cleanup
/// (Defect A: renewal stops at the finalize fence but cleanup hasn't taken over yet) and for
/// the bounded-parallel, deadline-bounded renewal sweep (Defect B: sequential sweep can exceed
/// the lease window under load, and a slow participant can block the reaper). Each test is written
/// so that reverting the production edit it covers causes the assertion to fail.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRangeLockLeaseHandoff : BaseCluster
{
    private const string Space = "t:rlh";
    private const string InsideKey = Space + "/m";

    private static readonly double TimingScale = GetTimingScale();
    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double scale) ? scale : 1.0;
    }

    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeLockLeaseHandoff(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b =>
            b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger   = lf.CreateLogger<IRaft>();
        kahunaLogger = lf.CreateLogger<IKahuna>();
    }

    // ── Defect A: lease must survive the finalize drain ─────────────────────────────────────

    /// <summary>
    /// A range lock must stay held through a finalize drain that outlasts the <b>original acquire TTL</b>.
    /// The lock is acquired with a short TTL; a conflicting acquire from another transaction is only attempted
    /// after that TTL has fully elapsed, so it is denied at that instant <b>only if renewal kept the lock alive
    /// through the drain</b>. Before the fix, <c>SnapshotRenewableRangeLocks</c> returned empty for a
    /// <c>Finalizing</c> session, so every renewal sweep was a no-op, the lock lapsed at its acquire TTL, and a
    /// conflicting acquire slipped through. After the fix, renewal continues through the drain until
    /// <c>ReleaseWorkingSet</c> atomically takes ownership via <c>MarkRenewalExcluded</c>.
    ///
    /// Runs on a single embedded node so the range lock, the coordinator session, and the conflict probe share
    /// one context with no leader election or split/merge remapping: the only thing that can keep the lock alive
    /// past its short acquire TTL is the renewal sweep, which makes this a faithful signal for the gate.
    /// </summary>
    [Fact]
    public async Task RangeLockLease_SurvivesFinalizeDrain_StaysHeldUntilCleanup()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // CollectionInterval=100ms → RangeLockRenewalTtlMs=200ms. The acquire TTL is deliberately shorter than
        // the time the drain is held, so without renewal the lock is guaranteed to have lapsed by probe time.
        const int CollectionMs = 100;
        int acquireTtlMs = (int)(250 * TimingScale);

        EmbeddedKahunaOptions options = new()
        {
            Storage                   = "memory",
            WalStorage                = "memory",
            InitialPartitions         = 1,
            CollectionInterval        = TimeSpan.FromMilliseconds(CollectionMs * TimingScale),
            DefaultTransactionTimeout = (int)(10_000 * TimingScale),
        };

        await using EmbeddedKahunaNode embedded = new(options);
        await embedded.StartAsync(ct);

        KahunaManager node = (KahunaManager)embedded.Kahuna;
        TransactionCoordinator coordinator = node.KeyValues.Coordinator;

        (_, TransactionHandle txA) = await node.LocateAndStartTransaction(
            new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
        Assert.False(txA.IsEmpty);

        TransactionOperationId lockOp = TransactionOperationId.NewRandom();
        (KeyValueResponseType lockType, _) = await node.LocateAndTryAcquireRangeLock(
            txA.TransactionId, Space, null, true, null, false,
            acquireTtlMs, KeyValueDurability.Persistent, RangeLockMode.Exclusive,
            ct, txA.CoordinatorKey, lockOp);
        Assert.Equal(KeyValueResponseType.Locked, lockType);

        // Register a pending operation but never complete it → the finalize drain blocks on it.
        TransactionOperationId pendingOp = TransactionOperationId.NewRandom();
        (OperationRegistrationOutcome regOutcome, _, _, _, _) = await node.LocateAndBeginOperation(
            txA.CoordinatorKey, txA.TransactionId, pendingOp, OperationKind.Set, null, ct);
        Assert.Equal(OperationRegistrationOutcome.New, regOutcome);

        // Start the rollback: it enters the finalize fence (Finalizing) then blocks on the drain.
        Task<KeyValueResponseType> rollbackTask = node.LocateAndRollbackTransaction(txA, ct);
        await Task.Delay((int)(50 * TimingScale), ct);

        // Drive renewal repeatedly while the session is Finalizing, holding the drain open well beyond the
        // acquire TTL. With the fix each sweep re-extends the lock; without it (SnapshotRenewableRangeLocks
        // returns empty for a Finalizing session) every sweep is a no-op and the lock lapses at acquireTtlMs.
        long holdUntil = Environment.TickCount64 + (long)(acquireTtlMs * 2.5);
        while (Environment.TickCount64 < holdUntil)
        {
            await coordinator.RenewSessionRangeLocks();
            await Task.Delay((int)(CollectionMs * 0.7 * TimingScale), ct);
        }

        // The original acquire TTL has long elapsed. A conflicting exclusive acquire over the same range from a
        // different transaction must still be denied — the lock is held only because renewal kept it alive
        // through the drain. This probe reads the exact range-lock record that renewal refreshes, so it is a
        // faithful signal: revert the SnapshotRenewableRangeLocks gate and the lock lapses at acquireTtlMs,
        // letting this acquire succeed.
        Assert.Equal(KeyValueResponseType.AlreadyLocked, await ProbeConflictingAcquire(node, ct));

        // Unblock the drain → ReleaseWorkingSet runs → MarkRenewalExcluded then releases the lock.
        await node.LocateAndCompleteOperation(txA.CoordinatorKey, txA.TransactionId, pendingOp, new OperationCompletionPayload(), ct);
        Assert.Equal(KeyValueResponseType.RolledBack, await rollbackTask);

        // After cleanup the range lock is released: a conflicting acquire now succeeds.
        Assert.Equal(KeyValueResponseType.Locked, await ProbeConflictingAcquire(node, ct));
    }

    // ── Defect B: renewal sweep must be bounded in concurrency and time ──────────────────────

    /// <summary>
    /// The renewal sweep must self-impose a time deadline and pass it into each acquire, so a slow
    /// participant is cancelled at the budget instead of making the sweep as slow as the slowest RPC.
    /// The renewal is routed to a <b>remote</b> range-lock leader (the injected transport latency only
    /// applies to remote acquires), and the test asserts the latency hook actually fired — otherwise a
    /// local, fast acquire would let a broken deadline pass unnoticed.
    /// </summary>
    [Fact]
    public async Task RenewalSweep_SlowRemoteParticipant_CompletesWithinDeadline()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int CollectionMs = 120;   // → RangeLockRenewalTtlMs=240ms, sweep budget=120ms
        const int SlowDelayMs  = 800;    // per-remote-acquire latency, far exceeds the budget

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3, MemoryInterNodeCommmunication transport) =
            await AssembleThreNodeClusterWithTransportAndConfig("memory", 8, raftLogger, kahunaLogger, cfg =>
            {
                cfg.CollectionInterval = TimeSpan.FromMilliseconds(CollectionMs * TimingScale);
                cfg.DefaultTransactionTimeout = (int)(10_000 * TimingScale);
            });

        IKahuna[] kahunas = [k1, k2, k3];

        try
        {
            // Find a node whose range-lock renewal routes remotely (its acquire fires the transport hook),
            // and hold several sessions there so the sweep has real work.
            (KahunaManager host, List<TransactionHandle> handles) =
                await SetupRemoteRoutedSessions(kahunas, transport, sessionCount: 4, CollectionMs, ct);

            // Injected latency that honors the sweep-deadline token: a bounded sweep cancels it at the
            // budget; an unbounded one waits the full SlowDelayMs per acquire.
            int hookCalls = 0;
            transport.AcquireRangeLockHook = async (_, _, token) =>
            {
                Interlocked.Increment(ref hookCalls);
                await Task.Delay((int)(SlowDelayMs * TimingScale), token);
            };

            long startTick = Environment.TickCount64;
            await host.KeyValues.Coordinator.RenewSessionRangeLocks();
            long elapsedMs = Environment.TickCount64 - startTick;

            transport.AcquireRangeLockHook = null;

            // The hook must have fired — otherwise the acquire was local/fast and the test proves nothing.
            Assert.True(Volatile.Read(ref hookCalls) > 0, "renewal acquire did not route remotely; the latency hook never fired");

            // The sweep must return near its deadline (~CollectionInterval), not SlowDelayMs.
            int maxAllowedMs = (int)((CollectionMs * TimingScale) + (250 * TimingScale)); // budget + slack
            Assert.True(
                elapsedMs < maxAllowedMs,
                $"Sweep took {elapsedMs}ms but should be bounded by ~{maxAllowedMs}ms (deadline≈{CollectionMs}ms); SlowDelayMs={SlowDelayMs}ms");

            foreach (TransactionHandle h in handles)
                await host.LocateAndRollbackTransaction(h, ct);
        }
        finally
        {
            await LeaveAll([r1, r2, r3], ct);
        }
    }

    /// <summary>
    /// A slow renewal sweep must not block the reaper from running on the same actor tick. Renewal is
    /// routed to a slow remote participant; the combined renewal + reap (exactly what the reaper actor's
    /// Receive does) must still return promptly because the sweep self-caps at its deadline.
    /// </summary>
    [Fact]
    public async Task RenewalSweep_SlowRemoteParticipant_DoesNotBlockReaper()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int CollectionMs = 120;
        const int SlowDelayMs  = 800;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3, MemoryInterNodeCommmunication transport) =
            await AssembleThreNodeClusterWithTransportAndConfig("memory", 8, raftLogger, kahunaLogger, cfg =>
            {
                cfg.CollectionInterval = TimeSpan.FromMilliseconds(CollectionMs * TimingScale);
                cfg.DefaultTransactionTimeout = (int)(10_000 * TimingScale);
            });

        IKahuna[] kahunas = [k1, k2, k3];

        try
        {
            (KahunaManager host, List<TransactionHandle> handles) =
                await SetupRemoteRoutedSessions(kahunas, transport, sessionCount: 1, CollectionMs, ct);

            int hookCalls = 0;
            transport.AcquireRangeLockHook = async (_, _, token) =>
            {
                Interlocked.Increment(ref hookCalls);
                await Task.Delay((int)(SlowDelayMs * TimingScale), token);
            };

            long startTick = Environment.TickCount64;
            await host.KeyValues.Coordinator.RenewSessionRangeLocks();
            await host.KeyValues.Coordinator.ReapAbandonedSessions();
            long elapsedMs = Environment.TickCount64 - startTick;

            transport.AcquireRangeLockHook = null;

            Assert.True(Volatile.Read(ref hookCalls) > 0, "renewal acquire did not route remotely; the latency hook never fired");

            int maxAllowedMs = (int)(CollectionMs * 2 * TimingScale + 250 * TimingScale);
            Assert.True(
                elapsedMs < maxAllowedMs,
                $"Renewal+reap took {elapsedMs}ms but should complete within {maxAllowedMs}ms; SlowDelayMs={SlowDelayMs}ms shows the sweep was bounded, not sequential");

            foreach (TransactionHandle h in handles)
                await host.LocateAndRollbackTransaction(h, ct);
        }
        finally
        {
            await LeaveAll([r1, r2, r3], ct);
        }
    }

    /// <summary>
    /// Starts a fresh transaction and attempts an exclusive range lock over the whole test space, then rolls
    /// it back so a successful acquire leaves no lingering lock. Returns the acquire outcome: <c>AlreadyLocked</c>
    /// while another transaction still holds the range, <c>Locked</c> once it is free.
    /// </summary>
    private static async Task<KeyValueResponseType> ProbeConflictingAcquire(KahunaManager node, CancellationToken ct)
    {
        (_, TransactionHandle probe) = await node.LocateAndStartTransaction(
            new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

        TransactionOperationId op = TransactionOperationId.NewRandom();
        (KeyValueResponseType type, _) = await node.LocateAndTryAcquireRangeLock(
            probe.TransactionId, Space, null, true, null, false,
            5_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, probe.CoordinatorKey, op);

        await node.LocateAndRollbackTransaction(probe, ct);
        return type;
    }

    // ── harness ──────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Starts sessions (each holding a range lock over a distinct prefix) on a node whose range-lock
    /// renewal routes to a <b>remote</b> leader, so the transport latency hook fires during the sweep.
    /// Round-robins the nodes, keeping only sessions the probing node actually hosts (so its sweep owns
    /// them), until one node's renewal acquire routes remotely.
    ///
    /// The search is bounded by wall-clock, not a fixed attempt count, and each acquire retries transient
    /// <c>MustRetry</c>: on a loaded runner a single-shot acquire can miss on election churn, leaving the
    /// probe with nothing to route and starving the search of any remote hit.
    /// </summary>
    private async Task<(KahunaManager Host, List<TransactionHandle> Handles)> SetupRemoteRoutedSessions(
        IKahuna[] kahunas, MemoryInterNodeCommmunication transport, int sessionCount, int collectionMs, CancellationToken ct)
    {
        int acquireTtlMs = (int)(collectionMs * 4 * TimingScale);
        long deadline = Environment.TickCount64 + (long)(30_000 * TimingScale);

        for (int round = 0; Environment.TickCount64 < deadline; round++)
        {
            KahunaManager host = (KahunaManager)kahunas[round % kahunas.Length];
            List<TransactionHandle> handles = [];

            // Assemble sessions that are actually hosted on this node — only those are renewed by its
            // sweep — each holding a range lock over a distinct prefix.
            for (int i = 0; i < sessionCount && Environment.TickCount64 < deadline; i++)
            {
                if (await StartLocallyHostedTransaction(host, ct) is not { } h)
                    break;

                KeyValueResponseType lt = await AcquireRangeLockUntilLocked(
                    host, h, $"{Space}/{round}/{i}", acquireTtlMs, ct);
                if (lt == KeyValueResponseType.Locked)
                    handles.Add(h);
                else
                    await host.LocateAndRollbackTransaction(h, ct);
            }

            if (handles.Count == sessionCount)
            {
                // Probe: does this host's renewal route remotely for at least one session? Counting hook, no delay.
                int probeCalls = 0;
                transport.AcquireRangeLockHook = (_, _, _) => { Interlocked.Increment(ref probeCalls); return Task.CompletedTask; };
                await host.KeyValues.Coordinator.RenewSessionRangeLocks();
                transport.AcquireRangeLockHook = null;

                if (Volatile.Read(ref probeCalls) > 0)
                    return (host, handles);
            }

            // No remote route (all locks happened to be local, or partial setup) — roll back and try again.
            foreach (TransactionHandle h in handles)
                await host.LocateAndRollbackTransaction(h, ct);
        }

        throw new InvalidOperationException("could not find a node whose range-lock renewal routes remotely");
    }

    /// <summary>
    /// Starts a transaction and returns its handle only when the session landed on <paramref name="host"/>
    /// itself, so that node's renewal sweep owns it. A session that routed to a remote coordinator-partition
    /// leader is rolled back and null is returned. Bounded retry over transient <c>MustRetry</c> starts.
    /// </summary>
    private static async Task<TransactionHandle?> StartLocallyHostedTransaction(KahunaManager host, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 5_000;
        while (Environment.TickCount64 < deadline)
        {
            (_, TransactionHandle h) = await host.LocateAndStartTransaction(
                new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            if (h.IsEmpty)
            {
                await Task.Delay(25, ct);
                continue;
            }

            if (host.KeyValues.Coordinator.sessions.ContainsKey(h.TransactionId))
                return h;

            // Hosted on a remote coordinator-partition leader — this node's sweep would not renew it.
            await host.LocateAndRollbackTransaction(h, ct);
        }

        return null;
    }

    /// <summary>
    /// Acquires an exclusive range lock over <paramref name="prefix"/>, retrying transient <c>MustRetry</c>
    /// within a bounded deadline. Returns the terminal acquire outcome (<c>Locked</c> on success).
    /// </summary>
    private static async Task<KeyValueResponseType> AcquireRangeLockUntilLocked(
        KahunaManager host, TransactionHandle h, string prefix, int ttlMs, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 5_000;
        while (true)
        {
            TransactionOperationId op = TransactionOperationId.NewRandom();
            (KeyValueResponseType lt, _) = await host.LocateAndTryAcquireRangeLock(
                h.TransactionId, prefix, null, true, null, false,
                ttlMs, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, h.CoordinatorKey, op);

            if (lt != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return lt;

            await Task.Delay(25, ct);
        }
    }

    private async Task<(IRaft, IRaft, IRaft, IKahuna, IKahuna, IKahuna, MemoryInterNodeCommmunication)>
        AssembleThreNodeClusterWithTransportAndConfig(string walStorage, int partitions,
            ILogger<IRaft> raftLog, ILogger<IKahuna> kahunaLog,
            Action<KahunaConfiguration> configure)
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster(walStorage, partitions, raftLog, kahunaLog, configure);

        MemoryInterNodeCommmunication transport = ((KahunaManager)k1).GetInterNodeCommunication();
        return (r1, r2, r3, k1, k2, k3, transport);
    }

    private static async Task LeaveAll(IRaft[] rafts, CancellationToken ct)
    {
        foreach (IRaft raft in rafts)
        {
            try { await raft.LeaveCluster(true, ct); }
            catch (ObjectDisposedException) { }
        }
    }
}
