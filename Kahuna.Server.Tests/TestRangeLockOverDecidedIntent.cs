using System.Diagnostics;
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
/// Range-lock acquires over the decision→settlement window that deferred settlement leaves open after every
/// commit: for a moment every key the transaction wrote still carries its write intent, while its outcome is
/// already durable.
///
/// <para>A Shared acquire is granted over such an intent. The value under it is fixed, and every read under the
/// lock resolves the intent inline to the committed value (or the pre-image on abort), so the lock protects
/// exactly what the reads observe. An Exclusive acquire cannot place its own per-key intent while the
/// predecessor's occupies the slot, so it waits, but the wait is bounded by that predecessor's resolution: the
/// acquire loop settles the decided intents itself instead of waiting for the background settlement to reach
/// them. An undecided writer keeps its answers: a Shared or Exclusive acquire is refused with the holder.</para>
///
/// <para>The constructed-window tests build the state directly in the durable stores, so the window stays open
/// for the whole test instead of closing on whatever the background resolution finishes first; the end-to-end
/// tests drive real commits and measure the acquire that follows.</para>
/// </summary>
public sealed class TestRangeLockOverDecidedIntent
{
    private const int KeyCount = 500;

    private const int RangeLockExpiresMs = 30_000;

    private const string SharedGrantsInstrument = "kahuna.transactions.range_lock_shared_grants_over_decided_intent";

    private const string ExclusiveWaitsInstrument = "kahuna.transactions.range_lock_exclusive_settlement_waits";

    private const string BlockersSettledInstrument = "kahuna.transactions.range_lock_acquire_blockers_settled";

    private static double Sum(MetricCapture capture, string instrument)
    {
        double total = 0;
        foreach (double sample in capture.Samples(instrument))
            total += sample;
        return total;
    }

    private readonly ILoggerFactory loggerFactory;

    public TestRangeLockOverDecidedIntent(ITestOutputHelper outputHelper)
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
            InitialPartitions = 4,
            DurableDeferredSettlement = true
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("rld/seed", ct);

        return node;
    }

    private static string Bucket(string tag) => $"rld-{Guid.NewGuid().ToString("N")[..8]}-{tag}";

    private static string Key(string bucket, int i) => $"{bucket}/k{i:D4}";

    // ── End to end: a real 500-key commit, then the acquire ─────────────────────────────────────────────

    /// <summary>
    /// The consumer shape end to end: transactions commit 500 keys into one bucket, and the next transaction
    /// scans them under a Shared range lock. The deferred resolution is held back, so the acquire meets the
    /// durable intents still pending after the commit returned. The acquire is granted at once, the scan under
    /// it resolves every committed value through the pending intents, and the resolution then settles them
    /// under the reader's lock. A commit on this path releases its in-memory write intents before it returns,
    /// so the in-memory decided intent that forces the handler's decision is covered by the constructed
    /// windows below; this arm covers the real commit and read paths around it.
    /// </summary>
    [Fact]
    public async Task SharedAcquire_ImmediatelyAfterLargeCommit_IsGrantedAndReadsEveryCommittedValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string bucket = Bucket("shared");
        using MetricCapture metrics = new("cause", SharedGrantsInstrument);
        using ResolutionGate gate = new((KahunaManager)kahuna);

        TransactionHandle reader = await StartTransaction(kahuna, bucket + "/reader", ct);
        await CommitKeys(node, bucket, "v1", ct);

        long start = Stopwatch.GetTimestamp();
        (KeyValueResponseType lockType, HLCTimestamp holder) = await kahuna.LocateAndTryAcquireRangeLock(
            reader.TransactionId, bucket, null, true, null, true, RangeLockExpiresMs,
            KeyValueDurability.Persistent, RangeLockMode.Shared, ct,
            reader.CoordinatorKey, TransactionOperationId.NewRandom());

        TimeSpan elapsed = Stopwatch.GetElapsedTime(start);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Shared acquire after a {KeyCount}-key commit: {lockType} (holder {holder}) in {elapsed.TotalMilliseconds:F1} ms; " +
            $"unsettled intents at grant: {((KahunaManager)kahuna).DurablePreparedIntentStore.Count}; " +
            $"grants over a decided intent counted: {Sum(metrics, SharedGrantsInstrument)}");

        Assert.Equal(KeyValueResponseType.Locked, lockType);

        await AssertBucketReads(kahuna, reader, bucket, "v1", ct);

        // The held resolution now runs under the reader's Shared lock: settlement is not fenced by it.
        gate.Release();
        await WaitUntil(() => ((KahunaManager)kahuna).DurablePreparedIntentStore.Count == 0);

        await AssertBucketReads(kahuna, reader, bucket, "v1", ct);
        await kahuna.LocateAndRollbackTransaction(reader, ct);
    }

    /// <summary>
    /// The Exclusive form under a settlement backlog: two 500-key commits land with their deferred resolutions
    /// held back, and an Exclusive acquire over the first bucket follows. The background settlement is not
    /// going to reach either bucket. The acquire is granted, its elapsed time is printed so the bound can be
    /// read off the run, and the held resolutions then run under the writer's Exclusive lock, which must be
    /// harmless: settlement is never fenced by a range lock. The helping round an Exclusive acquire runs over
    /// an in-memory decided intent is covered by the constructed windows below.
    /// </summary>
    [Fact]
    public async Task ExclusiveAcquire_ImmediatelyAfterLargeCommit_WithSettlementBacklog_IsGranted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        const int rounds = 10;
        double[] elapsedMs = new double[rounds];
        using MetricCapture metrics = new("cause", ExclusiveWaitsInstrument, BlockersSettledInstrument);

        for (int round = 0; round < rounds; round++)
        {
            string target = Bucket($"x{round}");
            string backlog = Bucket($"b{round}");

            TransactionHandle writer = await StartTransaction(kahuna, target + "/writer", ct);

            using ResolutionGate gate = new(manager);
            await CommitKeys(node, backlog, "v1", ct);
            await CommitKeys(node, target, "v1", ct);

            double settledBefore = Sum(metrics, BlockersSettledInstrument);
            long start = Stopwatch.GetTimestamp();
            int unsettledAtStart = manager.DurablePreparedIntentStore.Count;

            (KeyValueResponseType lockType, HLCTimestamp holder) = await kahuna.LocateAndTryAcquireRangeLock(
                writer.TransactionId, target, null, true, null, true, RangeLockExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
                writer.CoordinatorKey, TransactionOperationId.NewRandom());

            elapsedMs[round] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            double settledByAcquire = Sum(metrics, BlockersSettledInstrument) - settledBefore;
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"round {round}: {lockType} in {elapsedMs[round]:F1} ms, {unsettledAtStart} unsettled intents when the acquire started, " +
                $"{settledByAcquire} settled by the acquire");

            Assert.True(lockType == KeyValueResponseType.Locked,
                $"round {round}: Exclusive acquire answered {lockType} (holder {holder}) after {elapsedMs[round]:F1} ms");

            await AssertBucketReads(kahuna, writer, target, "v1", ct);

            gate.Release();
            await WaitUntil(() => manager.DurablePreparedIntentStore.Count == 0);

            await AssertBucketReads(kahuna, writer, target, "v1", ct);
            await kahuna.LocateAndRollbackTransaction(writer, ct);
        }

        Array.Sort(elapsedMs);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Exclusive acquire after a {KeyCount}-key commit with a second {KeyCount}-key commit queued for settlement, " +
            $"{rounds} rounds: min {elapsedMs[0]:F1} ms, median {elapsedMs[rounds / 2]:F1} ms, max {elapsedMs[rounds - 1]:F1} ms; " +
            $"settlement waits counted: {Sum(metrics, ExclusiveWaitsInstrument)}, blockers settled by the acquires: {Sum(metrics, BlockersSettledInstrument)}");
    }

    // ── Constructed windows: the state is built in the stores and never settles on its own ───────────────

    /// <summary>A committed-but-unsettled writer: the Shared acquire is granted on the spot, and the reads under
    /// the lock resolve the committed value even though nothing has materialized it.</summary>
    [Fact]
    public async Task SharedAcquire_OverCommittedUnsettledWriter_IsGrantedAndReadsCommittedValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        string bucket = Bucket("committed");
        ConstructedWindow window = await BuildWindow(kahuna, bucket, TransactionDecision.Commit, ct);

        TransactionHandle reader = await StartTransaction(kahuna, bucket + "/reader", ct);
        using MetricCapture metrics = new("cause", SharedGrantsInstrument);

        long start = Stopwatch.GetTimestamp();
        (KeyValueResponseType lockType, _) = await kahuna.LocateAndTryAcquireRangeLock(
            reader.TransactionId, bucket, null, true, null, true, RangeLockExpiresMs,
            KeyValueDurability.Persistent, RangeLockMode.Shared, ct,
            reader.CoordinatorKey, TransactionOperationId.NewRandom());

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Shared acquire over {KeyCount} committed-unsettled keys: {lockType} in {Stopwatch.GetElapsedTime(start).TotalMilliseconds:F1} ms");

        Assert.Equal(KeyValueResponseType.Locked, lockType);

        // Nothing settled the window: the grant did not depend on it, and it was counted as a grant over a
        // decided intent.
        Assert.Equal(KeyCount, CountWindowIntents(kahuna, window));
        Assert.True(Sum(metrics, SharedGrantsInstrument) >= 1, "the grant over a decided intent was not counted");

        await AssertBucketReads(kahuna, reader, bucket, "v1", ct);
    }

    /// <summary>An aborted-but-unsettled writer: the Shared acquire is granted, and the reads under the lock
    /// return the pre-image.</summary>
    [Fact]
    public async Task SharedAcquire_OverAbortedUnsettledWriter_IsGrantedAndReadsPreImage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        string bucket = Bucket("aborted");
        ConstructedWindow window = await BuildWindow(kahuna, bucket, TransactionDecision.Abort, ct);

        TransactionHandle reader = await StartTransaction(kahuna, bucket + "/reader", ct);

        (KeyValueResponseType lockType, _) = await kahuna.LocateAndTryAcquireRangeLock(
            reader.TransactionId, bucket, null, true, null, true, RangeLockExpiresMs,
            KeyValueDurability.Persistent, RangeLockMode.Shared, ct,
            reader.CoordinatorKey, TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Locked, lockType);
        Assert.Equal(KeyCount, CountWindowIntents(kahuna, window));

        await AssertBucketReads(kahuna, reader, bucket, "v0", ct);
    }

    /// <summary>
    /// The control: a writer whose intents are prepared but whose decision is withheld is a live concurrent
    /// writer. A Shared acquire is refused and names it; an Exclusive acquire is refused too and leaves no
    /// intent behind on the keys it had already stamped before it met the writer.
    /// </summary>
    [Fact]
    public async Task Acquire_OverUndecidedWriter_IsRefusedWithTheHolderAndPlacesNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        string bucket = Bucket("undecided");

        // Sorts before every writer key, so an Exclusive acquire stamps it first and must roll it back.
        string freeKey = bucket + "/a-free";
        await Seed(kahuna, freeKey, "free", ct);

        ConstructedWindow window = await BuildWindow(kahuna, bucket, decision: null, ct);

        TransactionHandle reader = await StartTransaction(kahuna, bucket + "/reader", ct);
        (KeyValueResponseType sharedType, HLCTimestamp sharedHolder) = await kahuna.LocateAndTryAcquireRangeLock(
            reader.TransactionId, bucket, null, true, null, true, RangeLockExpiresMs,
            KeyValueDurability.Persistent, RangeLockMode.Shared, ct,
            reader.CoordinatorKey, TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.AlreadyLocked, sharedType);
        Assert.Equal(window.Holder, sharedHolder);

        TransactionHandle writer = await StartTransaction(kahuna, bucket + "/writer", ct);
        (KeyValueResponseType exclusiveType, HLCTimestamp exclusiveHolder) = await kahuna.LocateAndTryAcquireRangeLock(
            writer.TransactionId, bucket, null, true, null, true, RangeLockExpiresMs,
            KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            writer.CoordinatorKey, TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.AlreadyLocked, exclusiveType);
        Assert.Equal(window.Holder, exclusiveHolder);

        // The refused Exclusive acquire stamped the free key before it met the writer and rolled that stamp
        // back: a third transaction takes the free key's point lock at once.
        TransactionHandle third = await StartTransaction(kahuna, bucket + "/third", ct);
        (KeyValueResponseType freeLock, _, _, HLCTimestamp freeHolder) = await kahuna.LocateAndTryAcquireExclusiveLock(
            third.TransactionId, freeKey, 10_000, KeyValueDurability.Persistent, ct,
            coordinatorKey: third.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.True(freeLock == KeyValueResponseType.Locked, $"free key answered {freeLock} (holder {freeHolder})");

        // And the writer's own keys still carry only the writer's intents.
        Assert.Equal(KeyCount, CountWindowIntents(kahuna, window));
    }

    /// <summary>
    /// An Exclusive acquire over a committed-but-unsettled writer: the acquire settles the writer's intents itself
    /// (nothing else ever will in this constructed state) and is granted, and the committed value is materialized
    /// by that settlement.
    /// </summary>
    [Fact]
    public async Task ExclusiveAcquire_OverCommittedUnsettledWriter_SettlesTheWriterAndIsGranted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        string bucket = Bucket("exclusive");
        ConstructedWindow window = await BuildWindow(kahuna, bucket, TransactionDecision.Commit, ct);

        TransactionHandle writer = await StartTransaction(kahuna, bucket + "/writer", ct);
        using MetricCapture metrics = new("cause", ExclusiveWaitsInstrument, BlockersSettledInstrument);

        long start = Stopwatch.GetTimestamp();
        (KeyValueResponseType lockType, HLCTimestamp holder) = await kahuna.LocateAndTryAcquireRangeLock(
            writer.TransactionId, bucket, null, true, null, true, RangeLockExpiresMs,
            KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            writer.CoordinatorKey, TransactionOperationId.NewRandom());

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Exclusive acquire over {KeyCount} committed-unsettled keys: {lockType} (holder {holder}) in {Stopwatch.GetElapsedTime(start).TotalMilliseconds:F1} ms");

        Assert.Equal(KeyValueResponseType.Locked, lockType);

        // The acquire's helping pass settled the predecessor: its intents are gone and its value is visible.
        Assert.Equal(0, CountWindowIntents(kahuna, window));
        Assert.True(Sum(metrics, ExclusiveWaitsInstrument) >= 1, "the settlement wait was not counted");
        Assert.True(Sum(metrics, BlockersSettledInstrument) >= KeyCount, $"blockers settled by the acquire: {Sum(metrics, BlockersSettledInstrument)}");

        await AssertBucketReads(kahuna, writer, bucket, "v1", ct);
    }

    /// <summary>An Exclusive acquire over an aborted-but-unsettled writer settles it the same way and reads the
    /// pre-image afterwards.</summary>
    [Fact]
    public async Task ExclusiveAcquire_OverAbortedUnsettledWriter_SettlesTheWriterAndIsGranted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        string bucket = Bucket("exclusive-abort");
        ConstructedWindow window = await BuildWindow(kahuna, bucket, TransactionDecision.Abort, ct);

        TransactionHandle writer = await StartTransaction(kahuna, bucket + "/writer", ct);

        (KeyValueResponseType lockType, HLCTimestamp holder) = await kahuna.LocateAndTryAcquireRangeLock(
            writer.TransactionId, bucket, null, true, null, true, RangeLockExpiresMs,
            KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            writer.CoordinatorKey, TransactionOperationId.NewRandom());

        Assert.True(lockType == KeyValueResponseType.Locked, $"Exclusive acquire answered {lockType} (holder {holder})");
        Assert.Equal(0, CountWindowIntents(kahuna, window));

        await AssertBucketReads(kahuna, writer, bucket, "v0", ct);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Holds every deferred resolution scheduled while it is installed, so the decision→settlement window of a
    /// real commit stays open until <see cref="Release"/>. Disposal releases and uninstalls the hook, so a
    /// failed assertion never leaves the node's resolutions parked.
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

    private sealed record ConstructedWindow(HLCTimestamp Holder, string[] Keys);

    /// <summary>
    /// A writer that holds the point lock on every key of <paramref name="bucket"/> (its in-memory write intents),
    /// with a durable prepared intent for each and — unless <paramref name="decision"/> is null — a terminal
    /// canonical record. No finalize ran, so nothing will ever settle these intents on its own.
    /// </summary>
    private static async Task<ConstructedWindow> BuildWindow(KahunaManager kahuna, string bucket, TransactionDecision? decision, CancellationToken ct)
    {
        string anchor = bucket + "/holder";
        string[] keys = new string[KeyCount];
        long[] seededRevisions = new long[KeyCount];

        for (int i = 0; i < KeyCount; i++)
        {
            keys[i] = Key(bucket, i);
            seededRevisions[i] = await Seed(kahuna, keys[i], "v0", ct);
        }

        TransactionHandle holder = await StartTransaction(kahuna, anchor, ct);
        foreach (string key in keys)
        {
            (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                holder.TransactionId, key, 60_000, KeyValueDurability.Persistent, ct,
                coordinatorKey: holder.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, lockType);
        }

        PreparedIntent[] intents = new PreparedIntent[KeyCount];
        TransactionParticipantRef[] participants = new TransactionParticipantRef[KeyCount];
        for (int i = 0; i < KeyCount; i++)
        {
            intents[i] = new PreparedIntent(
                holder.TransactionId, Epoch: 0, keys[i], ManifestHash: 0, RecordAnchorKey: anchor,
                CommitTimestamp: holder.TransactionId, State: KeyValueState.Set, Value: "v1"u8.ToArray(), Bucket: null,
                Revision: seededRevisions[i] + 1, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: -1,
                BaseState: KeyValueState.Undefined, RecoveryDeadline: HLCTimestamp.Zero,
                Resolution: PreparedIntentResolution.Pending);
            participants[i] = new TransactionParticipantRef(keys[i], KeyValueDurability.Persistent);
        }

        kahuna.DurablePreparedIntentStore.ImportIntents(intents);

        if (decision is { } terminal)
        {
            kahuna.DurableTransactionRecordStore.ImportRecords([new TransactionRecord(
                holder.TransactionId, Epoch: 0, CoordinatorKey: anchor, RecordAnchorKey: anchor,
                CommitTimestamp: holder.TransactionId, DecisionDeadline: HLCTimestamp.Zero, ManifestHash: 0,
                Participants: participants, ManifestPresent: true,
                Decision: terminal,
                AbortClass: terminal == TransactionDecision.Abort ? TransactionAbortClass.ExplicitRollback : TransactionAbortClass.None,
                WinningOpId: holder.TransactionId, CreatedAt: holder.TransactionId, DecidedAt: holder.TransactionId)]);
        }

        return new ConstructedWindow(holder.TransactionId, keys);
    }

    private static int CountWindowIntents(KahunaManager kahuna, ConstructedWindow window)
    {
        int count = 0;
        foreach (string key in window.Keys)
        {
            if (kahuna.DurablePreparedIntentStore.Get(key) is { } intent && intent.TransactionId == window.Holder)
                count++;
        }

        return count;
    }

    /// <summary>
    /// Script transactions write every key of the bucket, each one plus a companion key on another partition,
    /// and commit. The companion keeps each commit on the two-phase path. The script path is the write shape
    /// whose per-key write intents outlive the commit's return: they are cleared by the deferred resolution,
    /// not by a commit-time lock release, so with the resolution held the keys carry exactly the
    /// decided-but-unsettled intents this class is about. The parser bounds a statement list, so the bucket is
    /// committed in several scripts; the range then holds the unsettled intents of several decided writers.
    /// </summary>
    private static async Task CommitKeys(EmbeddedKahunaNode node, string bucket, string value, CancellationToken ct)
    {
        const int keysPerScript = 100;
        string companion = CompanionOnAnotherPartition(node, bucket);

        for (int first = 0; first < KeyCount; first += keysPerScript)
        {
            StringBuilder script = new("BEGIN ");
            for (int i = first; i < Math.Min(first + keysPerScript, KeyCount); i++)
                script.Append("SET `").Append(Key(bucket, i)).Append("` '").Append(value).Append("' ");
            script.Append("SET `").Append(companion).Append("` '").Append(value).Append("' COMMIT END");

            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script.ToString()), null, null).WaitAsync(ct);

            Assert.True(result.Type == KeyValueResponseType.Set, $"script commit of {bucket} answered {result.Type}: {result.Reason}");
        }
    }

    /// <summary>A key on a different partition from the bucket's keys. Keys route by their bucket, so the
    /// candidates vary the bucket, not the leaf.</summary>
    private static string CompanionOnAnotherPartition(EmbeddedKahunaNode node, string bucket)
    {
        int bucketPartition = node.Raft.GetPartitionKey(Key(bucket, 0));
        for (int i = 0; i < 256; i++)
        {
            string candidate = $"{bucket}-companion{i}/k";
            if (node.Raft.GetPartitionKey(candidate) != bucketPartition)
                return candidate;
        }

        Assert.Fail($"no key on a partition other than {bucketPartition} among 256 candidates");
        return "";
    }

    private static async Task<TransactionHandle> StartTransaction(IKahuna kahuna, string coordinatorKey, CancellationToken ct,
        KeyValueTransactionLocking locking = KeyValueTransactionLocking.Pessimistic)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = locking,
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

    /// <summary>Every key of the bucket, read under the transaction's lock through the bucket scan and through a
    /// point read, carries <paramref name="expected"/>.</summary>
    private static async Task AssertBucketReads(IKahuna kahuna, TransactionHandle tx, string bucket, string expected, CancellationToken ct)
    {
        KeyValueGetByBucketResult scan = await kahuna.LocateAndGetByBucket(
            tx.TransactionId, bucket, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            tx.CoordinatorKey, TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Get, scan.Type);

        Dictionary<string, string> seen = new(StringComparer.Ordinal);
        foreach ((string key, ReadOnlyKeyValueEntry entry) in scan.Items)
            seen[key] = entry.Value is null ? "" : Encoding.UTF8.GetString(entry.Value);

        for (int i = 0; i < KeyCount; i++)
        {
            string key = Key(bucket, i);
            Assert.True(seen.TryGetValue(key, out string? scanned), $"bucket scan under the lock did not return {key}");
            Assert.Equal(expected, scanned);
        }

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? read) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, Key(bucket, KeyCount - 1), -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal(expected, Encoding.UTF8.GetString(read!.Value!));
    }
}
