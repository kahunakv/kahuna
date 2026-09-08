using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The session-registration calls one node makes when a transaction executes away from the node that owns its
/// session: the idempotent begin that opens an operation, the completion that folds its effect, and the
/// working-set query. Read from the executing node's own registration counters, so the numbers are exact for
/// one node and one transaction rather than a process-wide rate.
///
/// <para>These counts are the registration cost a session-local transaction never pays. They exist so that a
/// decision about merging registrations across transactions is made on a measured hop count instead of an
/// assumption about how many small calls the transport sends.</para>
/// </summary>
public sealed class TestSessionRegistrationAttribution : BaseCluster
{
    private const int Partitions = 6;

    /// <summary>The duration histogram of a forwarded registration, as a consumer scrapes it.</summary>
    private const string ForwardMsInstrument = "kahuna.durable_tx.session_registration_forward_ms";

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSessionRegistrationAttribution(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<int> LeaderIndexOf(int partition, IRaft[] rafts, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return i;

            await Task.Delay(50, ct);
        }
    }

    /// <summary>
    /// A coordinator key whose partition is led by a known node, and the index of a node that does not lead it.
    /// The session lives on the coordinator key's leader, so driving the transaction from the other node is what
    /// makes every registration a forward. With one replica per partition the partitions are placed across the
    /// three nodes, so such a pair exists.
    /// </summary>
    private static async Task<(string CoordinatorKey, int SessionIndex, int DriverIndex)> FindRemoteSessionPair(
        IRaft[] rafts, KahunaManager[] managers, CancellationToken ct)
    {
        string random = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; i < 4_096; i++)
        {
            string candidate = $"xsr{i}/{random}";
            int partition = managers[0].KeyValues.LocateDurablePartition(candidate).PartitionId;
            int sessionIndex = await LeaderIndexOf(partition, rafts, ct);

            for (int driverIndex = 0; driverIndex < rafts.Length; driverIndex++)
                if (driverIndex != sessionIndex)
                    return (candidate, sessionIndex, driverIndex);
        }

        throw new InvalidOperationException("no coordinator key resolved to a partition with a known leader");
    }

    private static async Task<TransactionHandle> StartTransaction(KahunaManager node, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await node.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        Assert.False(handle.IsEmpty);
        return handle;
    }

    private static async Task WriteOneKey(KahunaManager node, TransactionHandle handle, string key, CancellationToken ct)
    {
        (KeyValueResponseType writeType, _, _) = await node.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "1"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
            KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    /// <summary>
    /// The session is on another node: every operation costs exactly one begin forward and one complete
    /// forward, a working-set query costs exactly one more, and the commit itself costs none — it is forwarded
    /// whole to the session node, which reads its own working set locally.
    /// </summary>
    [Fact]
    public async Task RemoteSession_OneBeginAndOneCompleteForwardPerOperation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (string coordinatorKey, int sessionIndex, int driverIndex) = await FindRemoteSessionPair(rafts, managers, ct);
            KahunaManager driver = managers[driverIndex];

            // The path under test only exists when the driving node does not hold the session.
            int coordinatorPartition = driver.KeyValues.LocateDurablePartition(coordinatorKey).PartitionId;
            Assert.False(await rafts[driverIndex].AmILeaderIfHosted(coordinatorPartition, ct));

            TransactionHandle handle = await StartTransaction(driver, coordinatorKey, ct);

            string key = $"xsrk{Guid.NewGuid():N}";

            SessionRegistrationCounts beforeWrite = driver.KeyValues.SessionRegistrationCounts;

            await WriteOneKey(driver, handle, key, ct);

            SessionRegistrationCounts afterWrite = driver.KeyValues.SessionRegistrationCounts;

            // Two hops per operation: the registration that opens it, and the completion that folds its effect.
            Assert.Equal(1, afterWrite.BeginForwarded - beforeWrite.BeginForwarded);
            Assert.Equal(1, afterWrite.CompleteForwarded - beforeWrite.CompleteForwarded);

            // None of them was served here, none was refused, none was lost.
            Assert.Equal(0, afterWrite.BeginLocal - beforeWrite.BeginLocal);
            Assert.Equal(0, afterWrite.CompleteLocal - beforeWrite.CompleteLocal);
            Assert.Equal(0, afterWrite.Refused - beforeWrite.Refused);
            Assert.Equal(0, afterWrite.Threw - beforeWrite.Threw);
            Assert.Equal(0, afterWrite.Unrouted - beforeWrite.Unrouted);

            // A working-set query made away from the session node is one further hop.
            TransactionWorkingSet? workingSet = await driver.LocateAndGetTransactionWorkingSet(coordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(workingSet);

            SessionRegistrationCounts afterWorkingSet = driver.KeyValues.SessionRegistrationCounts;
            Assert.Equal(1, afterWorkingSet.WorkingSetForwarded - afterWrite.WorkingSetForwarded);
            Assert.Equal(0, afterWorkingSet.WorkingSetLocal - afterWrite.WorkingSetLocal);

            // The commit is forwarded whole to the session node, so it costs this node no registration hop at
            // all: the working-set read behind the finalize happens on the session node's own session table.
            (KeyValueResponseType commitType, _) = await driver.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, commitType);

            SessionRegistrationCounts afterCommit = driver.KeyValues.SessionRegistrationCounts;
            Assert.Equal(0, afterCommit.BeginForwarded - afterWorkingSet.BeginForwarded);
            Assert.Equal(0, afterCommit.CompleteForwarded - afterWorkingSet.CompleteForwarded);
            Assert.Equal(0, afterCommit.WorkingSetForwarded - afterWorkingSet.WorkingSetForwarded);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The same transaction driven entirely on its session node: no registration leaves the node, and the local
    /// counters move instead. This is the deployment that has nothing to batch.
    /// </summary>
    [Fact]
    public async Task CoLocatedSession_NoForwards_LocalCountersMoveInstead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (string coordinatorKey, int sessionIndex, _) = await FindRemoteSessionPair(rafts, managers, ct);
            KahunaManager session = managers[sessionIndex];

            TransactionHandle handle = await StartTransaction(session, coordinatorKey, ct);

            string key = $"xsrk{Guid.NewGuid():N}";

            SessionRegistrationCounts before = session.KeyValues.SessionRegistrationCounts;

            await WriteOneKey(session, handle, key, ct);

            TransactionWorkingSet? workingSet = await session.LocateAndGetTransactionWorkingSet(coordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(workingSet);

            SessionRegistrationCounts after = session.KeyValues.SessionRegistrationCounts;

            Assert.Equal(1, after.BeginLocal - before.BeginLocal);
            Assert.Equal(1, after.CompleteLocal - before.CompleteLocal);
            Assert.Equal(1, after.WorkingSetLocal - before.WorkingSetLocal);

            Assert.Equal(0, after.BeginForwarded - before.BeginForwarded);
            Assert.Equal(0, after.CompleteForwarded - before.CompleteForwarded);
            Assert.Equal(0, after.WorkingSetForwarded - before.WorkingSetForwarded);
            Assert.Equal(0, after.Unrouted - before.Unrouted);

            (KeyValueResponseType commitType, _) = await session.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, commitType);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A refused registration and a completion whose transport throws are counted under the right result, and
    /// the caller sees exactly the behaviour it saw before the counters existed: the refusal is returned, the
    /// throw propagates.
    /// </summary>
    [Fact]
    public async Task RefusedAndThrownRegistrations_AreCountedAndLeaveBehaviourUnchanged()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (string coordinatorKey, _, int driverIndex) = await FindRemoteSessionPair(rafts, managers, ct);
            KahunaManager driver = managers[driverIndex];

            // A registration for a transaction no session holds: the forward happens, and the answer is a
            // rejection the caller still receives.
            SessionRegistrationCounts beforeRefusal = driver.KeyValues.SessionRegistrationCounts;

            (OperationRegistrationOutcome refusedOutcome, _, _, _, _) = await driver.LocateAndBeginOperation(
                coordinatorKey, new HLCTimestamp(1, 42, 0), TransactionOperationId.NewRandom(), OperationKind.Set, [1, 2, 3], ct);

            Assert.Equal(OperationRegistrationOutcome.RejectedSessionClosed, refusedOutcome);

            SessionRegistrationCounts afterRefusal = driver.KeyValues.SessionRegistrationCounts;
            Assert.Equal(1, afterRefusal.BeginForwarded - beforeRefusal.BeginForwarded);
            Assert.Equal(1, afterRefusal.Refused - beforeRefusal.Refused);
            Assert.Equal(0, afterRefusal.Threw - beforeRefusal.Threw);

            // A completion whose transport throws: counted as a hop that threw, and the exception still reaches
            // the caller.
            TransactionHandle handle = await StartTransaction(driver, coordinatorKey, ct);

            TransactionOperationId faultedOp = TransactionOperationId.NewRandom();

            MemoryInterNodeCommmunication transport = Assert.IsType<MemoryInterNodeCommmunication>(driver.KeyValues.InterNodeCommunication);
            transport.CompleteOperationFault = (txId, opId) => txId == handle.TransactionId && opId == faultedOp;

            try
            {
                (OperationRegistrationOutcome beginOutcome, _, _, _, _) = await driver.LocateAndBeginOperation(
                    handle.CoordinatorKey, handle.TransactionId, faultedOp, OperationKind.Set, [4, 5, 6], ct);

                Assert.Equal(OperationRegistrationOutcome.New, beginOutcome);

                SessionRegistrationCounts beforeThrow = driver.KeyValues.SessionRegistrationCounts;

                await Assert.ThrowsAnyAsync<Exception>(async () => await driver.LocateAndCompleteOperation(
                    handle.CoordinatorKey, handle.TransactionId, faultedOp, new OperationCompletionPayload(), ct));

                SessionRegistrationCounts afterThrow = driver.KeyValues.SessionRegistrationCounts;
                Assert.Equal(1, afterThrow.CompleteForwarded - beforeThrow.CompleteForwarded);
                Assert.Equal(1, afterThrow.Threw - beforeThrow.Threw);
                Assert.Equal(0, afterThrow.Refused - beforeThrow.Refused);
            }
            finally
            {
                transport.CompleteOperationFault = null;
            }

            // The faulted operation stays pending on the coordinator by construction, so the session is left for
            // the cluster teardown rather than rolled back: a rollback would sit out the whole drain deadline
            // waiting for a completion this test deliberately lost.
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A retried registration carrying the same operation id is a second call on the wire, so it counts a
    /// second forward without changing the registration outcome. The counters measure transport cost, not
    /// logical operations, and the difference matters when hops per commit are read off them.
    /// </summary>
    [Fact]
    public async Task IdempotentReRegistration_CountsASecondForward()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (string coordinatorKey, _, int driverIndex) = await FindRemoteSessionPair(rafts, managers, ct);
            KahunaManager driver = managers[driverIndex];

            TransactionHandle handle = await StartTransaction(driver, coordinatorKey, ct);

            TransactionOperationId op = TransactionOperationId.NewRandom();
            byte[] digest = [7, 7, 7];

            SessionRegistrationCounts before = driver.KeyValues.SessionRegistrationCounts;

            (OperationRegistrationOutcome first, _, _, _, _) = await driver.LocateAndBeginOperation(
                handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, digest, ct);
            Assert.Equal(OperationRegistrationOutcome.New, first);

            (OperationRegistrationOutcome second, _, _, _, _) = await driver.LocateAndBeginOperation(
                handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, digest, ct);
            Assert.Equal(OperationRegistrationOutcome.AlreadyPending, second);

            SessionRegistrationCounts after = driver.KeyValues.SessionRegistrationCounts;

            Assert.Equal(2, after.BeginForwarded - before.BeginForwarded);

            // An idempotent re-registration is a served call, not a refusal: only a rejection counts as refused.
            Assert.Equal(0, after.Refused - before.Refused);

            // The registered operation is never completed, so the session is left for the cluster teardown: a
            // rollback here would sit out the whole drain deadline waiting for that completion.
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The wall time a forwarded registration takes, which is the other half of its cost: a hop count alone
    /// cannot say what share of a commit the registrations explain. One sample per forwarded hop, tagged by the
    /// call it timed, and nothing at all for a registration served on the node that holds the session.
    /// </summary>
    [Fact]
    public async Task ForwardedRegistrations_AreTimedPerHop_AndLocalOnesAreNot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        // The clock is read only for a listened instrument, so the capture is what turns the timing on. It also
        // reads the histogram the way a consumer does, through a real listener rather than through the counters.
        using MetricCapture capture = new("op", ForwardMsInstrument);

        try
        {
            (string coordinatorKey, int sessionIndex, int driverIndex) = await FindRemoteSessionPair(rafts, managers, ct);
            KahunaManager driver = managers[driverIndex];
            KahunaManager session = managers[sessionIndex];

            TransactionHandle handle = await StartTransaction(driver, coordinatorKey, ct);

            string key = $"xsrk{Guid.NewGuid():N}";

            SessionRegistrationForwardDurations beforeWrite = driver.KeyValues.SessionRegistrationForwardDurations;
            SessionRegistrationCounts countsBeforeWrite = driver.KeyValues.SessionRegistrationCounts;

            SessionRegistrationForwardDurations sessionBefore = session.KeyValues.SessionRegistrationForwardDurations;

            await WriteOneKey(driver, handle, key, ct);

            SessionRegistrationForwardDurations afterWrite = driver.KeyValues.SessionRegistrationForwardDurations;

            // Two forwarded hops per operation, so two timed samples: one for the registration that opens it,
            // one for the completion that folds its effect.
            Assert.Equal(1, afterWrite.BeginSamples - beforeWrite.BeginSamples);
            Assert.Equal(1, afterWrite.CompleteSamples - beforeWrite.CompleteSamples);
            Assert.Equal(0, afterWrite.WorkingSetSamples - beforeWrite.WorkingSetSamples);

            // A call that crossed the transport consumed time; a sample stuck at zero would mean the clock was
            // read once, or on the wrong side of the call.
            Assert.True(afterWrite.BeginTotalMs > beforeWrite.BeginTotalMs,
                $"a forwarded begin recorded no elapsed time: {afterWrite.BeginTotalMs} ms");

            Assert.True(afterWrite.CompleteTotalMs > beforeWrite.CompleteTotalMs,
                $"a forwarded complete recorded no elapsed time: {afterWrite.CompleteTotalMs} ms");

            // A working-set query made away from the session node is one further forwarded hop, timed as well.
            TransactionWorkingSet? workingSet = await driver.LocateAndGetTransactionWorkingSet(coordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(workingSet);

            SessionRegistrationForwardDurations afterWorkingSet = driver.KeyValues.SessionRegistrationForwardDurations;
            Assert.Equal(1, afterWorkingSet.WorkingSetSamples - afterWrite.WorkingSetSamples);
            Assert.True(afterWorkingSet.WorkingSetTotalMs > afterWrite.WorkingSetTotalMs,
                $"a forwarded working-set query recorded no elapsed time: {afterWorkingSet.WorkingSetTotalMs} ms");

            // Every hop the counters attribute to this node has a duration behind it. The decision this feeds
            // multiplies one by the other, so a hop counted without a sample would silently understate the cost.
            SessionRegistrationCounts countsAfter = driver.KeyValues.SessionRegistrationCounts;

            Assert.Equal(countsAfter.BeginForwarded - countsBeforeWrite.BeginForwarded, afterWorkingSet.BeginSamples - beforeWrite.BeginSamples);
            Assert.Equal(countsAfter.CompleteForwarded - countsBeforeWrite.CompleteForwarded, afterWorkingSet.CompleteSamples - beforeWrite.CompleteSamples);
            Assert.Equal(countsAfter.WorkingSetForwarded - countsBeforeWrite.WorkingSetForwarded, afterWorkingSet.WorkingSetSamples - beforeWrite.WorkingSetSamples);

            // The node that holds the session answered all three from its own session table. A method call is
            // not a hop, and timing it would mix a number three orders of magnitude smaller into the remote
            // distribution.
            SessionRegistrationForwardDurations sessionAfter = session.KeyValues.SessionRegistrationForwardDurations;

            Assert.Equal(0, sessionAfter.BeginSamples - sessionBefore.BeginSamples);
            Assert.Equal(0, sessionAfter.CompleteSamples - sessionBefore.CompleteSamples);
            Assert.Equal(0, sessionAfter.WorkingSetSamples - sessionBefore.WorkingSetSamples);

            (KeyValueResponseType commitType, _) = await driver.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, commitType);

            // The same samples reach the histogram a consumer scrapes, under the op that names the call. The
            // meter is process-wide and this project runs classes in parallel, so this asserts presence rather
            // than an exact total; the exact counts are the per-node ones above.
            Assert.NotEmpty(capture.Samples(ForwardMsInstrument, "begin"));
            Assert.NotEmpty(capture.Samples(ForwardMsInstrument, "complete"));
            Assert.NotEmpty(capture.Samples(ForwardMsInstrument, "working_set"));

            foreach (double sample in capture.Samples(ForwardMsInstrument, "begin"))
                Assert.True(sample >= 0, $"a duration sample was negative: {sample} ms");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A forwarded registration whose transport throws is timed like any other. A forward that failed after
    /// waiting cost its transaction that wait, and dropping it would flatter the distribution the batching
    /// decision reads. The exception still reaches the caller unchanged.
    /// </summary>
    [Fact]
    public async Task ForwardedRegistrationThatThrows_IsTimedAndStillPropagates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        using MetricCapture capture = new("op", ForwardMsInstrument);

        try
        {
            (string coordinatorKey, _, int driverIndex) = await FindRemoteSessionPair(rafts, managers, ct);
            KahunaManager driver = managers[driverIndex];

            TransactionHandle handle = await StartTransaction(driver, coordinatorKey, ct);

            TransactionOperationId faultedOp = TransactionOperationId.NewRandom();

            MemoryInterNodeCommmunication transport = Assert.IsType<MemoryInterNodeCommmunication>(driver.KeyValues.InterNodeCommunication);
            transport.CompleteOperationFault = (txId, opId) => txId == handle.TransactionId && opId == faultedOp;

            try
            {
                (OperationRegistrationOutcome beginOutcome, _, _, _, _) = await driver.LocateAndBeginOperation(
                    handle.CoordinatorKey, handle.TransactionId, faultedOp, OperationKind.Set, [4, 5, 6], ct);

                Assert.Equal(OperationRegistrationOutcome.New, beginOutcome);

                SessionRegistrationForwardDurations before = driver.KeyValues.SessionRegistrationForwardDurations;
                SessionRegistrationCounts countsBefore = driver.KeyValues.SessionRegistrationCounts;

                await Assert.ThrowsAnyAsync<Exception>(async () => await driver.LocateAndCompleteOperation(
                    handle.CoordinatorKey, handle.TransactionId, faultedOp, new OperationCompletionPayload(), ct));

                SessionRegistrationForwardDurations after = driver.KeyValues.SessionRegistrationForwardDurations;
                SessionRegistrationCounts countsAfter = driver.KeyValues.SessionRegistrationCounts;

                // The failed call is one sample, exactly as the hop it is counted as.
                Assert.Equal(1, countsAfter.Threw - countsBefore.Threw);
                Assert.Equal(1, after.CompleteSamples - before.CompleteSamples);
                Assert.True(after.CompleteTotalMs > before.CompleteTotalMs,
                    $"a forwarded complete that threw recorded no elapsed time: {after.CompleteTotalMs} ms");
            }
            finally
            {
                transport.CompleteOperationFault = null;
            }

            // The faulted operation stays pending on the coordinator by construction, so the session is left for
            // the cluster teardown rather than rolled back: a rollback would sit out the whole drain deadline
            // waiting for a completion this test deliberately lost.
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// With nothing listening for the duration, a forwarded registration is still counted as a hop but reads no
    /// clock at all — the instrument costs one boolean read on a path that runs on every operation of every
    /// remote-session transaction. Attaching a listener turns the same calls into samples, which is what shows
    /// the guard is the listener rather than a dead recording path.
    /// </summary>
    [Fact]
    public void UnlistenedDurationHistogram_CountsTheHopAndReadsNoClock()
    {
        // The meter is process-wide, so another class holding a listener on this instrument makes the
        // unlistened path unobservable. Better to skip than to assert something the process cannot show.
        Assert.SkipWhen(
            DurableTransactionMetrics.SessionRegistrationForwardMs.Enabled,
            "another test holds a listener on the shared meter, so the unlistened path cannot be observed");

        SessionRegistrationTelemetry unlistened = new();

        unlistened.BeginForward(SessionRegistrationOp.Begin).Answered(true);
        unlistened.BeginForward(SessionRegistrationOp.Complete).Threw();

        SessionRegistrationCounts counts = unlistened.Snapshot;
        Assert.Equal(1, counts.BeginForwarded);
        Assert.Equal(1, counts.CompleteForwarded);
        Assert.Equal(1, counts.Threw);

        SessionRegistrationForwardDurations durations = unlistened.DurationSnapshot;
        Assert.Equal(0, durations.BeginSamples);
        Assert.Equal(0, durations.CompleteSamples);
        Assert.True(durations.BeginTotalMs == 0d, $"an unlistened begin read the clock: {durations.BeginTotalMs} ms");
        Assert.True(durations.CompleteTotalMs == 0d, $"an unlistened complete read the clock: {durations.CompleteTotalMs} ms");

        using MetricCapture capture = new("op", ForwardMsInstrument);

        SessionRegistrationTelemetry listened = new();
        listened.BeginForward(SessionRegistrationOp.WorkingSet).Answered(true);

        Assert.Equal(1, listened.DurationSnapshot.WorkingSetSamples);
        Assert.NotEmpty(capture.Samples(ForwardMsInstrument, "working_set"));
    }
}
