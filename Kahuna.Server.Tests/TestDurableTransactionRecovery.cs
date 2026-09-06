using System.Diagnostics;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for <see cref="DurableTransactionRecovery"/>, the participant-side recovery sweep: it resolves due
/// unresolved intents to their canonical decision, presumes-abort only for undecided-past-deadline or orphan
/// records, skips undecided-within-deadline, and always takes the winner the record actually became (a
/// concurrent commit is honored even while recovery is trying to abort).
/// </summary>
public sealed class TestDurableTransactionRecovery
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private const int PartitionId = 3;
    private const long ManifestHash = 4242;
    private const string Anchor = "acct/1";

    private sealed class Seam
    {
        public readonly ConcurrentQueue<(int Partition, string Type)> Calls = new();

        // Failure injection by log type and by that type's call ordinal (1-based), so a test can refuse every
        // materialization, only the second one, or only the settle delta. A refused call applies nothing.
        public Func<string, int, bool>? Fail;

        private readonly ConcurrentDictionary<string, int> callsByType = new();

        // The intent store the seam applies settle deltas to, mirroring the production scheduler-completion apply
        // owner: recovery no longer applies itself, so its resolve/remove delta lands through this single ordered
        // seam. Key/value materialization records are recorded but not applied here (they are a different store).
        public PreparedIntentStore? Store;

        public Task<bool> Replicate(int partitionId, string logType, byte[] data, WriteAdmissionClass admissionClass, CancellationToken ct)
        {
            Calls.Enqueue((partitionId, logType));
            int ordinal = callsByType.AddOrUpdate(logType, 1, static (_, n) => n + 1);
            if (Fail is not null && Fail(logType, ordinal))
                return Task.FromResult(false);

            if (Store is not null && logType == ReplicationTypes.PreparedIntent)
                Store.Replicate(partitionId, new RaftLog { LogType = logType, LogData = data });
            return Task.FromResult(true);
        }
    }

    private static PreparedIntent PendingIntent(string key, HLCTimestamp recoveryDeadline) =>
        PendingIntent(key, recoveryDeadline, Ts(1000));

    private static PreparedIntent PendingIntent(string key, HLCTimestamp recoveryDeadline, HLCTimestamp transactionId) =>
        new(transactionId, 1, key, ManifestHash, Anchor, CommitTimestamp: Ts(1100),
            State: KeyValueState.Set, Value: [1, 2], Bucket: null, Revision: 3, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: 2, BaseState: KeyValueState.Set, RecoveryDeadline: recoveryDeadline,
            Resolution: PreparedIntentResolution.Pending);

    private static TransactionRecord Record(TransactionDecision decision, HLCTimestamp decisionDeadline, TransactionAbortClass abortClass = TransactionAbortClass.None) =>
        new(Ts(1000), 1, "coord", Anchor, Ts(1100), decisionDeadline, ManifestHash, [],
            ManifestPresent: true, decision, abortClass, WinningOpId: Ts(1500), CreatedAt: Ts(1000), DecidedAt: Ts(1500));

    private static DurableTransactionRecovery Recovery(
        PreparedIntentStore store, Seam seam, TransactionRecord? lookup, TransactionRecord? afterAbort,
        DurableTransactionFinalizer.ApplyCommitLocally? applyCommitLocally = null)
    {
        seam.Store = store;
        DurableTransactionRecovery.LookupRecordDelegate lookupDelegate = (_, _, _, _) => Task.FromResult(lookup);
        DurableTransactionRecovery.DriveAbortDelegate driveDelegate = (_, _, _) => Task.FromResult(afterAbort);
        return new DurableTransactionRecovery(store, seam.Replicate, lookupDelegate, driveDelegate, applyCommitLocally);
    }

    private static PreparedIntentStore StoreWith(params PreparedIntent[] intents)
    {
        PreparedIntentStore store = new();
        foreach (PreparedIntent intent in intents)
            store.Apply(new PrepareIntentCommand(intent));
        return store;
    }

    [Fact]
    public async Task CommittedRecord_ResolvesCommitted_AndMaterializes()
    {
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(5000)));
        Seam seam = new();
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Commit, Ts(9000)), afterAbort: null);

        int resolved = await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);

        Assert.Equal(1, resolved);
        Assert.Null(store.Get("acct/1")); // resolved-committed and removed
        Assert.Contains(seam.Calls, c => c.Type == ReplicationTypes.KeyValues); // materialized
    }

    [Fact]
    public async Task AbortRecord_ResolvesAborted_NoMaterialization()
    {
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(5000)));
        Seam seam = new();
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Abort, Ts(9000), TransactionAbortClass.Conflict), afterAbort: null);

        await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);

        Assert.Null(store.Get("acct/1")); // resolved-aborted and removed
        Assert.DoesNotContain(seam.Calls, c => c.Type == ReplicationTypes.KeyValues);
    }

    [Fact]
    public async Task UndecidedPastDeadline_DrivesPresumedAbort_ResolvesAborted()
    {
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(5000)));
        Seam seam = new();
        // Record is undecided with a deadline already in the past; the abort drive lands an Abort.
        DurableTransactionRecovery recovery = Recovery(store, seam,
            lookup: Record(TransactionDecision.Undecided, Ts(6000)),
            afterAbort: Record(TransactionDecision.Abort, Ts(6000), TransactionAbortClass.PresumedAbort));

        await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);

        Assert.Null(store.Get("acct/1")); // resolved-aborted and removed
    }

    [Fact]
    public async Task UndecidedWithinDeadline_IsSkipped()
    {
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(5000)));
        Seam seam = new();
        // Intent is due, but the record's decision deadline is still in the future: leave it for the coordinator.
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Undecided, Ts(50000)), afterAbort: null);

        int resolved = await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);

        Assert.Equal(0, resolved);
        Assert.Equal(PreparedIntentResolution.Pending, store.Get("acct/1")!.Resolution);
    }

    [Fact]
    public async Task OrphanPrepare_NoRecord_DrivesAbortTombstone_ResolvesAborted()
    {
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(5000)));
        Seam seam = new();
        DurableTransactionRecovery recovery = Recovery(store, seam,
            lookup: null, // no canonical record — an orphan prepare
            afterAbort: Record(TransactionDecision.Abort, Ts(6000), TransactionAbortClass.PresumedAbort));

        await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);

        Assert.Null(store.Get("acct/1")); // resolved-aborted and removed
    }

    [Fact]
    public async Task PresumedAbortRace_ConcurrentCommitWins_ResolvesCommitted()
    {
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(5000)));
        Seam seam = new();
        // Recovery sees undecided-past-deadline and tries to abort, but a concurrent commit already won.
        DurableTransactionRecovery recovery = Recovery(store, seam,
            lookup: Record(TransactionDecision.Undecided, Ts(6000)),
            afterAbort: Record(TransactionDecision.Commit, Ts(6000)));

        await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);

        Assert.Null(store.Get("acct/1")); // resolved-committed and removed
        Assert.Contains(seam.Calls, c => c.Type == ReplicationTypes.KeyValues);
    }

    [Fact]
    public async Task UndecidedPastDeadline_CountsDeadlineExpiryAbort()
    {
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(5000)));
        Seam seam = new();
        DurableTransactionRecovery recovery = Recovery(store, seam,
            lookup: Record(TransactionDecision.Undecided, Ts(6000)),
            afterAbort: Record(TransactionDecision.Abort, Ts(6000), TransactionAbortClass.PresumedAbort));

        long aborts = await MeasureCounter("kahuna.durable_tx.deadline_expiry_aborts",
            () => recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None));

        Assert.Equal(1, aborts);
    }

    [Fact]
    public async Task OrphanPrepare_DoesNotCountDeadlineExpiryAbort()
    {
        // No canonical record → an orphan prepare (anchor init never landed), not a deadline expiry: it is aborted
        // but must not be attributed to a deadline that never existed.
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(5000)));
        Seam seam = new();
        DurableTransactionRecovery recovery = Recovery(store, seam,
            lookup: null,
            afterAbort: Record(TransactionDecision.Abort, Ts(6000), TransactionAbortClass.PresumedAbort));

        long aborts = await MeasureCounter("kahuna.durable_tx.deadline_expiry_aborts",
            () => recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None));

        Assert.Equal(0, aborts);
    }

    [Fact]
    public async Task PresumedAbortRace_CommitWins_DoesNotCountDeadlineExpiryAbort()
    {
        // Recovery tried to abort an undecided-past-deadline record but a concurrent commit won: not an abort at
        // all, so it must not be counted.
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(5000)));
        Seam seam = new();
        DurableTransactionRecovery recovery = Recovery(store, seam,
            lookup: Record(TransactionDecision.Undecided, Ts(6000)),
            afterAbort: Record(TransactionDecision.Commit, Ts(6000)));

        long aborts = await MeasureCounter("kahuna.durable_tx.deadline_expiry_aborts",
            () => recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None));

        Assert.Equal(0, aborts);
    }

    // Sums the increments of a named counter on the "Kahuna" meter emitted while <paramref name="action"/> runs.
    private static async Task<long> MeasureCounter(string instrumentName, Func<Task> action)
    {
        long total = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Kahuna" && instrument.Name == instrumentName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => Interlocked.Add(ref total, measurement));
        listener.Start();

        await action();

        listener.Dispose();
        return Interlocked.Read(ref total);
    }

    [Fact]
    public async Task NotDueIntent_IsNotSwept()
    {
        PreparedIntentStore store = StoreWith(PendingIntent("acct/1", Ts(50000))); // recovery deadline in the future
        Seam seam = new();
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Commit, Ts(9000)), afterAbort: null);

        int resolved = await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);

        Assert.Equal(0, resolved);
        Assert.Equal(PreparedIntentResolution.Pending, store.Get("acct/1")!.Resolution);
    }

    // -----------------------------------------------------------------------
    // Committed-leg survival across record retention
    // -----------------------------------------------------------------------

    /// <summary>
    /// A committed transaction's unmaterialized leg must survive the loss of its canonical record. The
    /// interleaving is reachable through retention GC: leg A materializes and settles at commit time; leg B's
    /// deferred resolution fails transiently (a frozen peer), leaving a PENDING intent whose materialization
    /// never ran; the retention window elapses and the anchor leader purges the terminal record — its
    /// completion-receipt release stage cannot block on leg B, because receipts are recorded at
    /// materialization and leg B never materialized, and its local-settlement guard cannot see an intent held
    /// on a partition this node does not replicate. Recovery then finds a due pending intent with NO record.
    ///
    /// Absence of a record after retention is NOT evidence of an abort: the transaction committed, the client
    /// was told Committed, and this intent is the only durable copy of the committed value. Driving the
    /// presumed-abort protocol here creates an abort tombstone over a committed transaction's history and
    /// discards the value forever — observed downstream as a transfer with one leg missing (a durable
    /// SUM(balance) deficit in the bank soak). The sweep must leave such an intent alone (or materialize it),
    /// never resolve it to abort.
    /// </summary>
    [Fact]
    public async Task CommittedButUnmaterializedLeg_RecordPurgedByRetention_IsNeverResolvedToAbort()
    {
        HLCTimestamp txId = Ts(1000);
        const string legKey = "acct/credit-leg";

        PreparedIntentStore store = StoreWith(PendingIntent(legKey, recoveryDeadline: Ts(5000)));

        // The real record store, driven through the same transitions production applies: the transaction
        // initializes, COMMITS, and later its terminal record is purged by the retention GC's own command.
        TransactionRecordStore records = new();
        records.Apply(new InitializeTransactionCommand(
            txId, 1, "coord", Anchor, CommitTimestamp: Ts(1100), DecisionDeadline: Ts(9000),
            ManifestHash, [], OpId: Ts(1200), CreatedAt: Ts(1000)));
        records.Apply(new CommitTransactionCommand(txId, 1, ManifestHash, OpId: Ts(1200), AttemptHlc: Ts(1500)));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);

        records.Apply(new PurgeTransactionCommand(txId, 1));
        Assert.Null(records.Get(txId, 1)); // retention reclaimed the committed record

        Seam seam = new();
        seam.Store = store;

        // Lookup and abort-drive mirror the production wiring: the lookup reads the (now purged) record store,
        // and the drive applies the presumed-abort through the record state machine — which can mint a
        // tombstone from absence — then reads back the winner.
        DurableTransactionRecovery recovery = new(
            store,
            seam.Replicate,
            (transactionId, epoch, _, _) => Task.FromResult(records.Get(transactionId, epoch)),
            (abort, _, _) =>
            {
                records.Apply(abort);
                return Task.FromResult(records.Get(abort.TransactionId, abort.Epoch));
            });

        await recovery.SweepAsync(PartitionId, now: Ts(1_000_000), CancellationToken.None);

        // The committed value must still exist somewhere durable: either the sweep materialized it (a
        // key/value record was replicated), or the intent is still held for a later, better-informed pass.
        // If neither is true, the committed leg was silently discarded — a durable lost write.
        bool materialized = seam.Calls.Any(c => c.Type == ReplicationTypes.KeyValues);
        bool intentStillHeld = store.Get(legKey) is { Resolution: not PreparedIntentResolution.Aborted };

        Assert.True(materialized || intentStillHeld,
            "a committed transaction's unmaterialized leg was resolved to abort after its record aged out of retention — the committed value is lost");
    }

    // -----------------------------------------------------------------------
    // Confirmed settlement: the helper and the sweep report only what landed
    // -----------------------------------------------------------------------

    private const string BlockedKey = "acct/blocked";

    private static readonly HLCTimestamp RequesterTx = Ts(2000);

    private const string SettledNothing = "kahuna.durable_tx.helping_settled_nothing";

    /// <summary>A blocked finalize's intent on <see cref="BlockedKey"/>: the key a committed-but-unsettled
    /// blocker (transaction 1000) still holds.</summary>
    private static PreparedIntent Requester(string key = BlockedKey) => PendingIntent(key, Ts(5000), RequesterTx);

    private static Task<int> Help(DurableTransactionRecovery recovery, params string[] keys) =>
        recovery.TryResolveDecidedBlockersAsync(PartitionId, [.. keys.Select(Requester)], RequesterTx, 1, CancellationToken.None);

    /// <summary>
    /// Every materialization of the committed blocker is refused. The helper must report zero — not the size
    /// of the group it attempted — so the blocked finalize sleeps through its backoff instead of re-proposing
    /// at once; the blocker's intent stays exactly where it was, and the pass is counted by its cause.
    /// </summary>
    [Fact]
    public async Task Helper_EveryMaterializationRefused_ReportsZero_LeavesBlocker()
    {
        PreparedIntentStore store = StoreWith(PendingIntent(BlockedKey, Ts(5000)));
        Seam seam = new() { Fail = (type, _) => type == ReplicationTypes.KeyValues };
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Commit, Ts(9000)), afterAbort: null);

        using MetricCapture capture = new("cause", SettledNothing);

        int settled = await Help(recovery, BlockedKey);

        Assert.Equal(0, settled);
        Assert.Equal(PreparedIntentResolution.Pending, store.Get(BlockedKey)!.Resolution);
        Assert.Equal(Ts(1000), store.Get(BlockedKey)!.TransactionId);
        Assert.DoesNotContain(seam.Calls, c => c.Type == ReplicationTypes.PreparedIntent); // no settle was even proposed
        Assert.True(capture.Total(SettledNothing, "materialize_failed") >= 1);
    }

    /// <summary>The leader-local apply does not confirm (leadership lost mid-pass): the value is durable in the log,
    /// but the intent must not be settled over an unapplied commit, and the helper reports zero.</summary>
    [Fact]
    public async Task Helper_LocalApplyDoesNotConfirm_ReportsZero_LeavesBlocker()
    {
        PreparedIntentStore store = StoreWith(PendingIntent(BlockedKey, Ts(5000)));
        Seam seam = new();
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Commit, Ts(9000)), afterAbort: null,
            applyCommitLocally: (_, _) => Task.FromResult(false));

        using MetricCapture capture = new("cause", SettledNothing);

        int settled = await Help(recovery, BlockedKey);

        Assert.Equal(0, settled);
        Assert.Contains(seam.Calls, c => c.Type == ReplicationTypes.KeyValues); // materialized (durable)
        Assert.DoesNotContain(seam.Calls, c => c.Type == ReplicationTypes.PreparedIntent); // but not settled
        Assert.Equal(PreparedIntentResolution.Pending, store.Get(BlockedKey)!.Resolution);
        Assert.True(capture.Total(SettledNothing, "apply_failed") >= 1);
    }

    /// <summary>
    /// The materializations land but the settle delta is refused (scheduler backpressure on the blocker's
    /// partition). The helper reports zero: the values are durable, every intent remains, and the next sweep
    /// settles them — its re-materialization is idempotent and leaves the store in the same settled state.
    /// </summary>
    [Fact]
    public async Task Helper_SettleRefused_ReportsZero_ValuesDurable_SweepSettlesLater()
    {
        PreparedIntentStore store = StoreWith(PendingIntent(BlockedKey, Ts(5000)));
        Seam seam = new() { Fail = (type, _) => type == ReplicationTypes.PreparedIntent };
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Commit, Ts(9000)), afterAbort: null);

        using MetricCapture capture = new("cause", SettledNothing);

        int settled = await Help(recovery, BlockedKey);

        Assert.Equal(0, settled);
        Assert.Contains(seam.Calls, c => c.Type == ReplicationTypes.KeyValues); // the value reached the log
        Assert.Contains(seam.Calls, c => c.Type == ReplicationTypes.PreparedIntent); // the settle was proposed and refused
        Assert.Equal(PreparedIntentResolution.Pending, store.Get(BlockedKey)!.Resolution); // and nothing settled
        Assert.True(capture.Total(SettledNothing, "settle_failed") >= 1);

        // Backpressure clears; the periodic sweep finishes the settlement and reports exactly what it settled.
        seam.Fail = null;
        int swept = await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);

        Assert.Equal(1, swept);
        Assert.Null(store.Get(BlockedKey));
    }

    /// <summary>Three blocked keys held by one blocker; the second materialization is refused. The helper settles
    /// exactly the two that landed, reports two, and leaves the third for the sweep.</summary>
    [Fact]
    public async Task Helper_TwoOfThreeMaterialize_ReportsTwo_SettlesExactlyThose()
    {
        PreparedIntentStore store = StoreWith(
            PendingIntent("acct/a", Ts(5000)), PendingIntent("acct/b", Ts(5000)), PendingIntent("acct/c", Ts(5000)));
        Seam seam = new() { Fail = (type, ordinal) => type == ReplicationTypes.KeyValues && ordinal == 2 };
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Commit, Ts(9000)), afterAbort: null);

        using MetricCapture capture = new("cause", SettledNothing);

        int settled = await Help(recovery, "acct/a", "acct/b", "acct/c");

        Assert.Equal(2, settled);
        Assert.Null(store.Get("acct/a"));
        Assert.Null(store.Get("acct/c"));
        Assert.Equal(PreparedIntentResolution.Pending, store.Get("acct/b")!.Resolution); // refused, kept for recovery
    }

    /// <summary>The sweep reports the same confirmed counts: one of two due intents whose materialization was
    /// refused is not counted, and a refused settle counts nothing.</summary>
    [Fact]
    public async Task Sweep_ReportsOnlyConfirmedSettlement()
    {
        PreparedIntentStore store = StoreWith(PendingIntent("acct/a", Ts(5000)), PendingIntent("acct/b", Ts(5000)));
        Seam seam = new() { Fail = (type, ordinal) => type == ReplicationTypes.KeyValues && ordinal == 2 };
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Commit, Ts(9000)), afterAbort: null);

        // The sweep visits the due set in store order, so which key's materialization is the refused second
        // call is not fixed; what is fixed is that exactly one settles and exactly one remains.
        int first = await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);
        Assert.Equal(1, first);
        Assert.Equal(1, store.Count);
        string remaining = store.Get("acct/a") is not null ? "acct/a" : "acct/b";

        seam.Fail = (type, _) => type == ReplicationTypes.PreparedIntent;
        int second = await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);
        Assert.Equal(0, second);
        Assert.NotNull(store.Get(remaining));

        seam.Fail = null;
        int third = await recovery.SweepAsync(PartitionId, now: Ts(10000), CancellationToken.None);
        Assert.Equal(1, third);
        Assert.Equal(0, store.Count);
    }

    // -----------------------------------------------------------------------
    // Windowed materialization in the helping pass
    // -----------------------------------------------------------------------

    /// <summary>A seam whose materializations take real time and that records how many were in flight at once,
    /// so a test can tell one coalesced window from one durable round per key.</summary>
    private sealed class SlowSeam
    {
        private int inFlight;
        public int MaxInFlight;
        public int DelayMs = 5;
        public PreparedIntentStore? Store;

        public async Task<bool> Replicate(int partitionId, string logType, byte[] data, WriteAdmissionClass admissionClass, CancellationToken ct)
        {
            if (logType == ReplicationTypes.KeyValues)
            {
                int now = Interlocked.Increment(ref inFlight);
                int seen;
                do
                {
                    seen = Volatile.Read(ref MaxInFlight);
                } while (now > seen && Interlocked.CompareExchange(ref MaxInFlight, now, seen) != seen);

                await Task.Delay(DelayMs, ct);
                Interlocked.Decrement(ref inFlight);
                return true;
            }

            Store?.Replicate(partitionId, new RaftLog { LogType = logType, LogData = data });
            return true;
        }
    }

    private static DurableTransactionRecovery SlowRecovery(PreparedIntentStore store, SlowSeam seam, int maxItems)
    {
        seam.Store = store;
        return new DurableTransactionRecovery(
            store, seam.Replicate,
            (_, _, _, _) => Task.FromResult<TransactionRecord?>(Record(TransactionDecision.Commit, Ts(9000))),
            (_, _, _) => Task.FromResult<TransactionRecord?>(null),
            maxMaterializationBatchItems: maxItems);
    }

    /// <summary>
    /// A blocker holding five keys is materialized as one window: every record is submitted before the window is
    /// awaited, so the five replications overlap and the pass takes about one round trip, not five. The same
    /// group under a two-item cap advances two at a time, which is the scheduler-sized window the finalizer's
    /// own resolution honors.
    /// </summary>
    [Fact]
    public async Task Helper_MaterializesABlockerGroupInWindows_NotOneRoundPerKey()
    {
        string[] keys = ["acct/w1", "acct/w2", "acct/w3", "acct/w4", "acct/w5"];

        PreparedIntentStore wide = StoreWith([.. keys.Select(k => PendingIntent(k, Ts(5000)))]);
        SlowSeam wideSeam = new();
        DurableTransactionRecovery windowed = SlowRecovery(wide, wideSeam, maxItems: 512);

        long start = Stopwatch.GetTimestamp();
        int settled = await Help(windowed, keys);
        double elapsedMs = Stopwatch.GetElapsedTime(start).TotalMilliseconds;

        Assert.Equal(5, settled);
        Assert.Equal(5, wideSeam.MaxInFlight);
        Assert.True(elapsedMs < 5 * wideSeam.DelayMs, $"five 5 ms materializations took {elapsedMs:F1} ms; they were not overlapped");
        Assert.Equal(0, wide.Count);

        PreparedIntentStore capped = StoreWith([.. keys.Select(k => PendingIntent(k, Ts(5000)))]);
        SlowSeam cappedSeam = new();
        DurableTransactionRecovery twoAtATime = SlowRecovery(capped, cappedSeam, maxItems: 2);

        Assert.Equal(5, await Help(twoAtATime, keys));
        Assert.Equal(2, cappedSeam.MaxInFlight);
        Assert.Equal(0, capped.Count);
    }

    /// <summary>Two different decided blockers hold the blocked keys: the helper resolves their groups concurrently
    /// (bounded), so their materializations share the same window instead of queueing one blocker behind another.</summary>
    [Fact]
    public async Task Helper_ResolvesIndependentBlockersConcurrently()
    {
        PreparedIntentStore store = StoreWith(
            PendingIntent("acct/b1", Ts(5000), Ts(1000)),
            PendingIntent("acct/b2", Ts(5000), Ts(1001)));
        SlowSeam seam = new();
        DurableTransactionRecovery recovery = SlowRecovery(store, seam, maxItems: 512);

        Assert.Equal(2, await Help(recovery, "acct/b1", "acct/b2"));
        Assert.Equal(2, seam.MaxInFlight);
        Assert.Equal(0, store.Count);
    }

    /// <summary>The range-move barrier counts a decided intent whose settle did not land as still unsettled, so a
    /// cutover cannot copy a range that still carries it.</summary>
    [Fact]
    public async Task SettleSupplied_RefusedSettle_IsReportedUnsettled()
    {
        PreparedIntent intent = PendingIntent(BlockedKey, Ts(5000));
        PreparedIntentStore store = StoreWith(intent);
        Seam seam = new() { Fail = (type, _) => type == ReplicationTypes.PreparedIntent };
        DurableTransactionRecovery recovery = Recovery(store, seam, lookup: Record(TransactionDecision.Commit, Ts(9000)), afterAbort: null);

        int unsettled = await recovery.SettleSuppliedIntentsAsync(PartitionId, [intent], now: Ts(10000), CancellationToken.None);

        Assert.Equal(1, unsettled);
        Assert.NotNull(store.Get(BlockedKey));

        seam.Fail = null;
        Assert.Equal(0, await recovery.SettleSuppliedIntentsAsync(PartitionId, [intent], now: Ts(10000), CancellationToken.None));
        Assert.Null(store.Get(BlockedKey));
    }
}
