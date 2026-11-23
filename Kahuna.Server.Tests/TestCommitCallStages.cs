using System.Diagnostics;
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
/// Commit calls through the routed entry point of a node: each call is recorded once, under the path that served
/// it, and walks the stages of that path in order. The totals and the walks read here belong to one node, so they
/// are exact although other test classes commit in the same process.
/// </summary>
public sealed class TestCommitCallStages : BaseCluster
{
    private const string CallMetric = "kahuna.transactions.commit_call_ms";

    /// <summary>A delay long enough to be the largest part of any commit call of these tests.</summary>
    private const int HoldMs = 300;

    /// <summary>The least time a stage that contains the delay must show.</summary>
    private const double HeldMs = HoldMs * 0.8;

    private readonly ILoggerFactory loggerFactory;

    public TestCommitCallStages(ITestOutputHelper outputHelper)
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
        await node.WaitForLeaderForKeyAsync("cs0/k0", ct);

        return node;
    }

    private static CommitCallTelemetry CallsOf(IKahuna node) => ((KahunaManager)node).TransactionCoordinator.CommitCalls;

    private static string Bucket(string tag) => $"cs-{Guid.NewGuid().ToString("N")[..8]}-{tag}";

    private static long CallCount(CommitCallTelemetry calls)
    {
        long total = 0;

        foreach (CommitCallPath path in Enum.GetValues<CommitCallPath>())
            total += calls.Calls(path);

        return total;
    }

    private static double TotalMs(CommitCallTelemetry calls, CommitCallPath path)
    {
        double total = 0;

        foreach (CommitCallStage stage in Enum.GetValues<CommitCallStage>())
            total += calls.StageMilliseconds(path, stage);

        return total;
    }

    /// <summary>The one stage of <paramref name="path"/> that took the most time.</summary>
    private static CommitCallStage Largest(CommitCallTelemetry calls, CommitCallPath path) =>
        Enum.GetValues<CommitCallStage>().MaxBy(stage => calls.StageMilliseconds(path, stage));

    /// <summary>The stages the next calls of the node enter, in order.</summary>
    private static List<CommitCallStage> WalkOf(CommitCallTelemetry calls)
    {
        List<CommitCallStage> walk = [];
        calls.TestStageEntered = walk.Add;
        return walk;
    }

    private static void AssertUsed(CommitCallTelemetry calls, CommitCallPath path, params CommitCallStage[] stages)
    {
        foreach (CommitCallStage stage in stages)
            Assert.True(calls.StageMilliseconds(path, stage) > 0, $"stage {stage} of path {path} has no time");
    }

    private static void AssertUnused(CommitCallTelemetry calls, CommitCallPath path, params CommitCallStage[] stages)
    {
        foreach (CommitCallStage stage in stages)
            Assert.True(calls.StageMilliseconds(path, stage) == 0, $"stage {stage} of path {path} has time");
    }

    private static async Task<TransactionHandle> Start(IKahuna kahuna, string coordinatorKey, CancellationToken ct, int timeout = 60_000)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                Timeout = timeout
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static async Task Write(
        IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, "v"u8.ToArray(), null, -1, KeyValueFlags.None, 0, durability, ct,
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

    /// <summary>
    /// One-phase commits: one call each, every stage of the path has time, no stage of another path does, and the
    /// recorded time lies inside what the caller saw.
    /// </summary>
    [Fact]
    public async Task OnePhaseCommits_AreSplitIntoTheStagesOfThePath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        IKahuna kahuna = node.Kahuna;
        CommitCallTelemetry calls = CallsOf(kahuna);
        using MetricCapture capture = new("path", CallMetric);
        List<CommitCallStage> walk = WalkOf(calls);

        string bucket = Bucket("one");
        const int transactions = 20;
        double callerMs = 0;

        for (int i = 0; i < transactions; i++)
        {
            // Keys of their own: a key with an unsettled predecessor sends the next commit to the two-phase path.
            TransactionHandle tx = await Start(kahuna, bucket + "/tx", ct);
            await Write(kahuna, tx, $"{bucket}/a{i}", ct);
            await Write(kahuna, tx, $"{bucket}/b{i}", ct);

            walk.Clear();
            long started = Stopwatch.GetTimestamp();
            TransactionCommitAnswer answer = await kahuna.LocateAndCommitTransaction(tx, ct);
            callerMs += Stopwatch.GetElapsedTime(started).TotalMilliseconds;

            Assert.Equal(KeyValueResponseType.Committed, answer.Type);
            Assert.Equal(
                [
                    CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Preflight,
                    CommitCallStage.Encode, CommitCallStage.Attempt, CommitCallStage.Apply, CommitCallStage.Resolution,
                    CommitCallStage.Answer, CommitCallStage.Release, CommitCallStage.Answer
                ], walk);
        }

        Assert.Equal(transactions, calls.Calls(CommitCallPath.OnePhase));
        Assert.Equal(transactions, CallCount(calls));
        Assert.True(capture.Samples(CallMetric, "one_phase").Count >= transactions);

        AssertUsed(calls, CommitCallPath.OnePhase,
            CommitCallStage.Freeze, CommitCallStage.Preflight, CommitCallStage.Encode, CommitCallStage.Attempt,
            CommitCallStage.Apply, CommitCallStage.Resolution, CommitCallStage.Release, CommitCallStage.Answer);
        AssertUnused(calls, CommitCallPath.OnePhase,
            CommitCallStage.Prepare, CommitCallStage.Validate, CommitCallStage.Decision, CommitCallStage.Ephemeral);

        double recordedMs = TotalMs(calls, CommitCallPath.OnePhase);
        Assert.True(recordedMs <= callerMs, $"recorded {recordedMs} ms, the caller saw {callerMs} ms");
    }

    /// <summary>A transaction over two partitions has no one-phase attempt, and its prepare, validation and decision
    /// are stages of their own.</summary>
    [Fact]
    public async Task ATwoPartitionCommit_IsSplitIntoTheTwoPhaseStages()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct, partitions: 2);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        CommitCallTelemetry calls = CallsOf(manager);
        using MetricCapture capture = new("path", CallMetric);

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

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);
        Assert.Equal(
            [
                CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Preflight,
                CommitCallStage.Encode, CommitCallStage.Prepare, CommitCallStage.Validate, CommitCallStage.Decision,
                CommitCallStage.Resolution, CommitCallStage.Answer, CommitCallStage.Release, CommitCallStage.Answer
            ], walk);

        Assert.Equal(1, calls.Calls(CommitCallPath.TwoPhase));
        Assert.Equal(1, CallCount(calls));
        Assert.NotEmpty(capture.Samples(CallMetric, "two_phase"));

        AssertUsed(calls, CommitCallPath.TwoPhase,
            CommitCallStage.Freeze, CommitCallStage.Prepare, CommitCallStage.Validate, CommitCallStage.Decision,
            CommitCallStage.Release);
        AssertUnused(calls, CommitCallPath.TwoPhase,
            CommitCallStage.Attempt, CommitCallStage.Apply, CommitCallStage.Ephemeral);
    }

    /// <summary>A transaction that read and wrote nothing spends its commit in the read checks.</summary>
    [Fact]
    public async Task AReadOnlyCommit_HasItsOwnPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        IKahuna kahuna = node.Kahuna;
        CommitCallTelemetry calls = CallsOf(kahuna);
        using MetricCapture capture = new("path", CallMetric);

        string key = Bucket("read") + "/k";
        await Seed(kahuna, key, ct);

        TransactionHandle tx = await Start(kahuna, key, ct);
        await Read(kahuna, tx, key, ct);

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(tx, ct)).Type);
        Assert.Equal(
            [
                CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Validate,
                CommitCallStage.Release, CommitCallStage.Answer
            ], walk);

        Assert.Equal(1, calls.Calls(CommitCallPath.ReadOnly));
        Assert.Equal(1, CallCount(calls));
        AssertUsed(calls, CommitCallPath.ReadOnly, CommitCallStage.Validate, CommitCallStage.Release);
        AssertUnused(calls, CommitCallPath.ReadOnly,
            CommitCallStage.Preflight, CommitCallStage.Attempt, CommitCallStage.Prepare, CommitCallStage.Decision, CommitCallStage.Ephemeral);
    }

    /// <summary>A transaction of ephemeral keys commits in memory, in one stage of its own.</summary>
    [Fact]
    public async Task AnEphemeralCommit_HasItsOwnPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        IKahuna kahuna = node.Kahuna;
        CommitCallTelemetry calls = CallsOf(kahuna);
        using MetricCapture capture = new("path", CallMetric);

        string bucket = Bucket("ephemeral");
        TransactionHandle tx = await Start(kahuna, bucket + "/tx", ct);
        await Write(kahuna, tx, bucket + "/a", ct, KeyValueDurability.Ephemeral);
        await Write(kahuna, tx, bucket + "/b", ct, KeyValueDurability.Ephemeral);

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(tx, ct)).Type);
        Assert.Equal(
            [
                CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Validate,
                CommitCallStage.Ephemeral, CommitCallStage.Release, CommitCallStage.Answer
            ], walk);

        Assert.Equal(1, calls.Calls(CommitCallPath.Ephemeral));
        Assert.Equal(1, CallCount(calls));
        AssertUsed(calls, CommitCallPath.Ephemeral, CommitCallStage.Ephemeral, CommitCallStage.Release);
        AssertUnused(calls, CommitCallPath.Ephemeral,
            CommitCallStage.Preflight, CommitCallStage.Attempt, CommitCallStage.Prepare, CommitCallStage.Decision);
    }

    /// <summary>A transaction with both kinds of keys is recorded under the path of its durable decision, and its
    /// in-memory prepare and commit are a stage around it.</summary>
    [Fact]
    public async Task AMixedCommit_HasTheDurablePathAndAnEphemeralStage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        IKahuna kahuna = node.Kahuna;
        CommitCallTelemetry calls = CallsOf(kahuna);
        using MetricCapture capture = new("path", CallMetric);

        string bucket = Bucket("mixed");
        TransactionHandle tx = await Start(kahuna, bucket + "/tx", ct);
        await Write(kahuna, tx, bucket + "/persistent", ct);
        await Write(kahuna, tx, bucket + "/ephemeral", ct, KeyValueDurability.Ephemeral);

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(tx, ct)).Type);
        Assert.Equal(
            [
                CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Ephemeral,
                CommitCallStage.Freeze, CommitCallStage.Preflight, CommitCallStage.Encode, CommitCallStage.Attempt,
                CommitCallStage.Apply, CommitCallStage.Resolution, CommitCallStage.Answer, CommitCallStage.Ephemeral,
                CommitCallStage.Release, CommitCallStage.Answer
            ], walk);

        Assert.Equal(1, calls.Calls(CommitCallPath.OnePhase));
        Assert.Equal(1, CallCount(calls));
        AssertUsed(calls, CommitCallPath.OnePhase, CommitCallStage.Ephemeral, CommitCallStage.Attempt, CommitCallStage.Freeze);
    }

    /// <summary>A delay before the bundle is proposed lands on the attempt, and a delay after the staged-base
    /// validation lands on the preflight.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ADelayInsideTheFinalize_LandsOnItsStage(bool insideAttempt)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        CommitCallTelemetry calls = CallsOf(manager);
        using MetricCapture capture = new("path", CallMetric);

        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
        Func<CancellationToken, Task> hold = hookCt => Task.Delay(HoldMs, hookCt);

        if (insideAttempt)
            finalizer.TestAfterReadSetValidationHook = hold;
        else
            finalizer.TestAfterPreValidationHook = hold;

        string bucket = Bucket("held");
        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Write(manager, tx, bucket + "/k", ct);

        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        CommitCallStage expected = insideAttempt ? CommitCallStage.Attempt : CommitCallStage.Preflight;
        Assert.True(calls.StageMilliseconds(CommitCallPath.OnePhase, expected) >= HeldMs);
        Assert.Equal(expected, Largest(calls, CommitCallPath.OnePhase));
    }

    /// <summary>A second commit of a session whose finalize is running waits for its outcome: it is a call of its
    /// own, and its wait is not charged to the owner.</summary>
    [Fact]
    public async Task ACommitThatMirrorsTheOwner_IsRecordedApart()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        CommitCallTelemetry calls = CallsOf(manager);
        using MetricCapture capture = new("path", CallMetric);

        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.TransactionCoordinator.DurableFinalizerForTests.TestAfterReadSetValidationHook = hookCt =>
        {
            entered.TrySetResult();
            return release.Task.WaitAsync(hookCt);
        };

        string bucket = Bucket("mirror");
        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Write(manager, tx, bucket + "/k", ct);

        Task<TransactionCommitAnswer> owner = manager.LocateAndCommitTransaction(tx, ct);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

        // The owner holds the finalize slot, so this call is a mirror by the time it returns its task.
        Task<TransactionCommitAnswer> mirror = manager.LocateAndCommitTransaction(tx, ct);
        await Task.Delay(HoldMs, ct);
        release.TrySetResult();

        Assert.Equal(KeyValueResponseType.Committed, (await owner).Type);
        Assert.Equal(KeyValueResponseType.Committed, (await mirror).Type);

        Assert.Equal(1, calls.Calls(CommitCallPath.OnePhase));
        Assert.Equal(1, calls.Calls(CommitCallPath.Mirrored));
        Assert.Equal(2, CallCount(calls));
        Assert.True(calls.StageMilliseconds(CommitCallPath.Mirrored, CommitCallStage.Session) >= HeldMs);
        Assert.Equal(CommitCallStage.Session, Largest(calls, CommitCallPath.Mirrored));
        Assert.Equal(CommitCallStage.Attempt, Largest(calls, CommitCallPath.OnePhase));
    }

    /// <summary>A commit of a session that already ended answers from the retained outcome, on a path of its own.</summary>
    [Fact]
    public async Task ACommitOfAnEndedSession_IsRecordedAsReplayed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        IKahuna kahuna = node.Kahuna;
        CommitCallTelemetry calls = CallsOf(kahuna);
        using MetricCapture capture = new("path", CallMetric);

        string bucket = Bucket("replay");
        TransactionHandle tx = await Start(kahuna, bucket + "/tx", ct);
        await Write(kahuna, tx, bucket + "/k", ct);

        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(tx, ct)).Type);

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(tx, ct)).Type);
        Assert.Equal([CommitCallStage.Session], walk);

        Assert.Equal(1, calls.Calls(CommitCallPath.OnePhase));
        Assert.Equal(1, calls.Calls(CommitCallPath.Replayed));
        Assert.Equal(2, CallCount(calls));
    }

    /// <summary>A commit whose session has an operation in flight waits for it, and gives up as a retryable answer:
    /// the wait is the drain stage of a call that chose no path.</summary>
    [Fact]
    public async Task ACommitThatCannotDrain_SpendsItsTimeInTheDrain()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        CommitCallTelemetry calls = CallsOf(manager);
        using MetricCapture outcomes = new("outcome", CallMetric);

        string bucket = Bucket("drain");
        TransactionHandle tx = await Start(manager, bucket + "/tx", ct, timeout: HoldMs);
        await Write(manager, tx, bucket + "/k", ct);

        (OperationRegistrationOutcome registered, _, _, _, _, _) = await manager.LocateAndBeginOperation(
            tx.CoordinatorKey, tx.TransactionId, TransactionOperationId.NewRandom(), OperationKind.Set, null, ct);
        Assert.Equal(OperationRegistrationOutcome.New, registered);

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.MustRetry, (await manager.LocateAndCommitTransaction(tx, ct)).Type);
        Assert.Equal([CommitCallStage.Session, CommitCallStage.Drain], walk);

        Assert.Equal(1, calls.Calls(CommitCallPath.None));
        Assert.Equal(1, CallCount(calls));
        Assert.True(calls.StageMilliseconds(CommitCallPath.None, CommitCallStage.Drain) >= HeldMs);
        Assert.Equal(CommitCallStage.Drain, Largest(calls, CommitCallPath.None));
        Assert.NotEmpty(outcomes.Samples(CallMetric, "must_retry"));

        // The session lives on for a retry, and the next finalize of it must not charge its time to this call.
        Assert.Null(manager.TransactionCoordinator.sessions[tx.TransactionId].CommitTimeline);
    }

    /// <summary>A finalize that throws keeps its time in the stage it threw in, and holds the working set for the
    /// retry, which is a call of its own.</summary>
    [Fact]
    public async Task AFinalizeThatThrows_KeepsItsTimeInItsStage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        CommitCallTelemetry calls = CallsOf(manager);
        using MetricCapture outcomes = new("outcome", CallMetric);

        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
        finalizer.TestAfterReadSetValidationHook = async hookCt =>
        {
            await Task.Delay(HoldMs, hookCt);
            throw new InvalidOperationException("injected");
        };

        string bucket = Bucket("throw");
        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Write(manager, tx, bucket + "/k", ct);

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.MustRetry, (await manager.LocateAndCommitTransaction(tx, ct)).Type);
        Assert.Equal(
            [
                CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Preflight,
                CommitCallStage.Encode, CommitCallStage.Attempt, CommitCallStage.Answer, CommitCallStage.Answer
            ], walk);

        Assert.Equal(1, calls.Calls(CommitCallPath.OnePhase));
        Assert.True(calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Attempt) >= HeldMs);
        Assert.Equal(CommitCallStage.Attempt, Largest(calls, CommitCallPath.OnePhase));
        Assert.NotEmpty(outcomes.Samples(CallMetric, "must_retry"));
        Assert.Null(manager.TransactionCoordinator.sessions[tx.TransactionId].CommitTimeline);

        finalizer.TestAfterReadSetValidationHook = null;

        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);
        Assert.Equal(2, calls.Calls(CommitCallPath.OnePhase));
        Assert.Equal(2, CallCount(calls));
    }

    /// <summary>A commit that a read conflict refuses is one aborted call.</summary>
    [Fact]
    public async Task ARefusedCommit_IsRecordedWithItsOutcome()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        IKahuna kahuna = node.Kahuna;
        CommitCallTelemetry calls = CallsOf(kahuna);
        using MetricCapture outcomes = new("outcome", CallMetric);

        string bucket = Bucket("conflict");
        await Seed(kahuna, bucket + "/read", ct);

        TransactionHandle tx = await Start(kahuna, bucket + "/tx", ct);
        await Read(kahuna, tx, bucket + "/read", ct);
        await Seed(kahuna, bucket + "/read", ct);
        await Write(kahuna, tx, bucket + "/written", ct);

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.Aborted, (await kahuna.LocateAndCommitTransaction(tx, ct)).Type);

        // The one-phase attempt fails its validation and the two-phase flow records the abort.
        Assert.Equal(
            [
                CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Preflight,
                CommitCallStage.Encode, CommitCallStage.Attempt, CommitCallStage.Prepare, CommitCallStage.Validate,
                CommitCallStage.Decision, CommitCallStage.Resolution, CommitCallStage.Answer, CommitCallStage.Release,
                CommitCallStage.Answer
            ], walk);
        Assert.Equal(1, calls.Calls(CommitCallPath.TwoPhase));
        Assert.Equal(1, CallCount(calls));
        AssertUsed(calls, CommitCallPath.TwoPhase, CommitCallStage.Attempt, CommitCallStage.Prepare, CommitCallStage.Decision);
        Assert.NotEmpty(outcomes.Samples(CallMetric, "aborted"));
    }

    /// <summary>
    /// A snapshot read of a staged key inside the freeze pushes the commit above itself, so the attempt is refused and
    /// the input is frozen and finalized again: both passes belong to one call, and the second freeze is in its
    /// freeze stage.
    /// </summary>
    [Fact]
    public async Task ACommitFrozenAgain_WalksTheFinalizeTwiceInOneCall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        CommitCallTelemetry calls = CallsOf(manager);
        using MetricCapture capture = new("path", CallMetric);

        string bucket = Bucket("refreeze");
        string key = bucket + "/k";
        await Seed(manager, key, ct);

        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Write(manager, tx, key, ct);

        Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)>? snapshotRead = null;
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
        finalizer.TestAfterPreValidationHook = async hookCt =>
        {
            finalizer.TestAfterPreValidationHook = null;

            // Minted after the freeze, so the snapshot is above the frozen commit timestamp.
            HLCTimestamp readTimestamp = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
            snapshotRead = manager.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, readTimestamp, KeyValueDurability.Persistent, hookCt).AsTask();
            (KeyValueResponseType readType, _) = await snapshotRead;
            Assert.Equal(KeyValueResponseType.Get, readType);
        };

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.Committed, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        Assert.NotNull(snapshotRead);
        Assert.Equal(
            [
                CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Preflight,
                CommitCallStage.Encode, CommitCallStage.Attempt, CommitCallStage.Resolution, CommitCallStage.Answer,
                CommitCallStage.Freeze, CommitCallStage.Preflight, CommitCallStage.Encode, CommitCallStage.Attempt,
                CommitCallStage.Apply, CommitCallStage.Resolution, CommitCallStage.Answer, CommitCallStage.Release,
                CommitCallStage.Answer
            ], walk);
        Assert.Equal(1, calls.Calls(CommitCallPath.OnePhase));
        Assert.Equal(1, CallCount(calls));
    }

    /// <summary>A retry of an attempt whose deadline passed runs no finalize: the fence through the record is its
    /// decision, on a call that chose no path.</summary>
    [Fact]
    public async Task ARetryPastItsDeadline_IsChargedToTheDecision()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        CommitCallTelemetry calls = CallsOf(manager);
        using MetricCapture capture = new("path", CallMetric);

        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
        finalizer.TestAfterReadSetValidationHook = _ => throw new InvalidOperationException("injected");

        string bucket = Bucket("deadline");
        TransactionHandle tx = await Start(manager, bucket + "/tx", ct);
        await Write(manager, tx, bucket + "/k", ct);

        Assert.Equal(KeyValueResponseType.MustRetry, (await manager.LocateAndCommitTransaction(tx, ct)).Type);
        finalizer.TestAfterReadSetValidationHook = null;

        // The attempt proposed nothing, so moving its frozen deadline into the past stands for the time that passed.
        TransactionContext context = manager.TransactionCoordinator.sessions[tx.TransactionId];
        DurableFinalizeInput unresolved = Assert.IsType<DurableFinalizeInput>(context.UnresolvedDurableFinalize);
        context.UnresolvedDurableFinalize = unresolved with { DecisionDeadline = unresolved.CommitTimestamp };

        List<CommitCallStage> walk = WalkOf(calls);
        Assert.Equal(KeyValueResponseType.Aborted, (await manager.LocateAndCommitTransaction(tx, ct)).Type);

        Assert.Equal(
            [
                CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Decision,
                CommitCallStage.Release, CommitCallStage.Answer
            ], walk);
        Assert.Equal(1, calls.Calls(CommitCallPath.None));
        Assert.Equal(1, calls.Calls(CommitCallPath.OnePhase));
        Assert.Equal(2, CallCount(calls));
    }

    /// <summary>A rollback and a script are not commit calls.</summary>
    [Fact]
    public async Task RollbacksAndScripts_RecordNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        IKahuna kahuna = node.Kahuna;
        CommitCallTelemetry calls = CallsOf(kahuna);
        using MetricCapture capture = new("path", CallMetric);

        string bucket = Bucket("other");
        TransactionHandle tx = await Start(kahuna, bucket + "/tx", ct);
        await Write(kahuna, tx, bucket + "/k", ct);
        Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(tx, ct));

        KeyValueTransactionResult script = await kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes($"BEGIN SET `{bucket}/script` 'v' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, script.Type);

        Assert.Equal(0, CallCount(calls));
    }

    /// <summary>A commit that reaches a node other than the session's owner is one call on each of the two: the
    /// forwarding node spends all of it in its route, and the owner records the path that served it.</summary>
    [Fact]
    public async Task AForwardedCommit_IsRecordedOnBothNodes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", 4, loggerFactory.CreateLogger<IRaft>(), loggerFactory.CreateLogger<IKahuna>());

        try
        {
            KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];
            using MetricCapture capture = new("path", CallMetric);

            string bucket = Bucket("forward");
            TransactionHandle tx = await Start(managers[0], bucket + "/tx", ct);

            int owner = Array.FindIndex(managers, m => m.TransactionCoordinator.HasSession(tx.TransactionId));
            Assert.True(owner >= 0, "some node must hold the session");

            int other = (owner + 1) % managers.Length;

            await Write(managers[owner], tx, bucket + "/k", ct);

            Assert.Equal(KeyValueResponseType.Committed, (await RetryOnMustRetryAsync(
                () => managers[other].LocateAndCommitTransaction(tx, ct), r => r.Type)).Type);

            CommitCallTelemetry forwarding = CallsOf(managers[other]);
            CommitCallTelemetry serving = CallsOf(managers[owner]);

            Assert.True(forwarding.Calls(CommitCallPath.Forwarded) >= 1);
            Assert.Equal(forwarding.Calls(CommitCallPath.Forwarded), CallCount(forwarding));
            Assert.Equal(
                TotalMs(forwarding, CommitCallPath.Forwarded),
                forwarding.StageMilliseconds(CommitCallPath.Forwarded, CommitCallStage.Route), 9);

            Assert.True(serving.Calls(CommitCallPath.OnePhase) + serving.Calls(CommitCallPath.TwoPhase) >= 1);
            Assert.Equal(0, serving.Calls(CommitCallPath.Forwarded));
            Assert.True(capture.Samples(CallMetric, "forwarded").Count >= 1);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
