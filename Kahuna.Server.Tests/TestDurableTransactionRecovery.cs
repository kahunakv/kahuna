using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
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

        // The intent store the seam applies settle deltas to, mirroring the production scheduler-completion apply
        // owner: recovery no longer applies itself, so its resolve/remove delta lands through this single ordered
        // seam. Key/value materialization records are recorded but not applied here (they are a different store).
        public PreparedIntentStore? Store;

        public Task<bool> Replicate(int partitionId, string logType, byte[] data, CancellationToken ct)
        {
            Calls.Enqueue((partitionId, logType));
            if (Store is not null && logType == ReplicationTypes.PreparedIntent)
                Store.Replicate(partitionId, new RaftLog { LogType = logType, LogData = data });
            return Task.FromResult(true);
        }
    }

    private static PreparedIntent PendingIntent(string key, HLCTimestamp recoveryDeadline) =>
        new(Ts(1000), 1, key, ManifestHash, Anchor, CommitTimestamp: Ts(1100),
            State: KeyValueState.Set, Value: [1, 2], Bucket: null, Revision: 3, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: 2, BaseState: KeyValueState.Set, RecoveryDeadline: recoveryDeadline,
            Resolution: PreparedIntentResolution.Pending);

    private static TransactionRecord Record(TransactionDecision decision, HLCTimestamp decisionDeadline, TransactionAbortClass abortClass = TransactionAbortClass.None) =>
        new(Ts(1000), 1, "coord", Anchor, Ts(1100), decisionDeadline, ManifestHash, [],
            ManifestPresent: true, decision, abortClass, WinningOpId: Ts(1500), CreatedAt: Ts(1000), DecidedAt: Ts(1500));

    private static DurableTransactionRecovery Recovery(
        PreparedIntentStore store, Seam seam, TransactionRecord? lookup, TransactionRecord? afterAbort)
    {
        seam.Store = store;
        DurableTransactionRecovery.LookupRecordDelegate lookupDelegate = (_, _, _, _) => Task.FromResult(lookup);
        DurableTransactionRecovery.DriveAbortDelegate driveDelegate = (_, _, _) => Task.FromResult(afterAbort);
        return new DurableTransactionRecovery(store, seam.Replicate, lookupDelegate, driveDelegate);
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
}
