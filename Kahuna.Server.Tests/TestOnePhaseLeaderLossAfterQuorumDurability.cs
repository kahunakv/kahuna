using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The one-phase bundled commit when the leader is lost after quorum durability and before anyone is answered.
/// The bundle is durable, every replica has judged it, the client never heard — and then the only node that
/// knew it had committed is cut off the cluster.
///
/// <para>The claim under test is the design's determinism claim: a surviving replica reaches the same verdict
/// the lost leader's apply already reached, the values materialize exactly once when recovery re-drives the
/// settlement, and a client that retries the same transaction is answered from the recorded outcome instead of
/// executing anything a second time.</para>
///
/// <para><b>What this does not prove.</b> The reply hold models "durable but unanswered" without killing a
/// process, so a real signal-kill matrix — before the prepare, after the propose, at the decision, at the
/// acknowledgement, before settlement — is still fault-injection work outside this suite. What is closed here is
/// the deterministic half: the outcome does not depend on the leader surviving to announce it.</para>
/// </summary>
public sealed class TestOnePhaseLeaderLossAfterQuorumDurability : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    /// <summary>Long enough that no hold resolves on its own inside the fixture's window.</summary>
    private static readonly TimeSpan HoldBound = TimeSpan.FromMinutes(2);

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestOnePhaseLeaderLossAfterQuorumDurability(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

    private static async Task<int> LeaderIndexOf(int partition, IRaft[] rafts, CancellationToken ct)
    {
        int index = -1;
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (!await rafts[i].AmILeaderIfHosted(partition, ct))
                    continue;

                index = i;
                return true;
            }

            return false;
        }, timeoutMs: 30_000);

        return index;
    }

    /// <summary>
    /// The state the shared prefix leaves behind: the transaction whose bundle is durable on every survivor,
    /// the node that proposed it and was then cut off, the survivor that leads its partition now, and the
    /// finalize the cut-off node still owns and can never be answered from the cluster.
    /// </summary>
    private sealed record LostLeaderScenario(
        string Key,
        int Partition,
        TransactionHandle Handle,
        int Lost,
        int Successor,
        Task<(KeyValueResponseType, string?)> AbandonedCommit);

    /// <summary>
    /// Runs the shared prefix of both fixtures: begins the transaction on the leader of the key's partition,
    /// holds the reply of its one-phase bundle, proves the bundle is durable on every replica, drops the held
    /// reply, cuts the leader off the Raft transport, waits for a survivor to lead the partition and for
    /// recovery on the survivors to settle the intent with the recorded commit.
    /// </summary>
    private static async Task<LostLeaderScenario> CutOffLeaderAfterHeldOnePhaseBundleAsync(
        KahunaManager[] managers, IRaft[] rafts, InMemoryCommunication raftComm, CancellationToken ct)
    {
        string key = $"lost{Guid.NewGuid():N}/k";
        int partition = managers[0].KeyValues.LocateDurablePartition(key).PartitionId;
        int lost = await LeaderIndexOf(partition, rafts, ct);

        KahunaManager session = managers[lost];

        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = key,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType writeType, _, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "alpha"u8.ToArray(), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, writeType);

        using HeldCommitReplies holds = new(rafts[lost], partition);

        Task<(KeyValueResponseType, string?)> abandonedCommit = session.LocateAndCommitTransaction(handle, ct);

        // The record reaching Commit under the FIRST held reply is what proves the bundle: a two-phase
        // finalize's first proposal carries only the record initialize and the prepare.
        await WaitUntilAsync(() =>
            managers[lost].DurableTransactionRecordStore.Get(handle.TransactionId, 1)?.Decision == TransactionDecision.Commit,
            timeoutMs: 30_000);

        Assert.Equal(1, holds.Count);
        Assert.False(abandonedCommit.IsCompleted, "the finalize must still be waiting for the held reply");

        // Durable on a quorum: the replicas that will survive have judged the bundle themselves.
        await WaitUntilAsync(() =>
        {
            for (int i = 0; i < managers.Length; i++)
            {
                if (i == lost)
                    continue;

                if (managers[i].DurableTransactionRecordStore.Get(handle.TransactionId, 1)?.Decision != TransactionDecision.Commit)
                    return false;
            }

            return true;
        }, timeoutMs: 30_000);

        // Nobody is ever answered, and then the node that knew is gone.
        holds.DropAll();
        raftComm.PartitionNode(EndpointOf(lost));

        int successor = await WaitForSurvivingLeader(rafts, partition, lost, ct);
        Assert.NotEqual(lost, successor);

        // Recovery on the new leader reaches the verdict the lost leader's apply already reached, and its
        // settle is idempotent under re-drive.
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < managers.Length; i++)
            {
                if (i == lost)
                    continue;

                await managers[i].KeyValues.RecoverPreparedIntents(ct);
            }

            for (int i = 0; i < managers.Length; i++)
            {
                if (i == lost)
                    continue;

                if (managers[i].DurablePreparedIntentStore.Get(key) is not null)
                    return false;
            }

            return true;
        }, timeoutMs: 60_000);

        for (int i = 0; i < managers.Length; i++)
        {
            if (i == lost)
                continue;

            Assert.Equal(TransactionDecision.Commit,
                managers[i].DurableTransactionRecordStore.Get(handle.TransactionId, 1)?.Decision);
        }

        return new LostLeaderScenario(key, partition, handle, lost, successor, abandonedCommit);
    }

    /// <summary>
    /// Exactly once: a first write settles at revision 0, so a re-applied settle would read as 1. Checked on
    /// every survivor, whichever of them serves the read.
    /// </summary>
    private static async Task AssertSettledOnceOnSurvivorsAsync(KahunaManager[] managers, LostLeaderScenario scenario, CancellationToken ct)
    {
        for (int i = 0; i < managers.Length; i++)
        {
            if (i == scenario.Lost)
                continue;

            (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
                () => managers[i].LocateAndTryGetValue(HLCTimestamp.Zero, scenario.Key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1);

            Assert.Equal(KeyValueResponseType.Get, readType);
            Assert.Equal("alpha"u8.ToArray(), entry!.Value);
            Assert.Equal(0, entry.Revision);
        }
    }

    private static void ObserveAbandonedCommit(LostLeaderScenario? scenario)
    {
        // The abandoned finalize is never answered by the cluster; observe its eventual fault so it cannot
        // surface as an unobserved task exception in an unrelated test.
        if (scenario is not null)
            _ = scenario.AbandonedCommit.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);
    }

    [Fact]
    public async Task HeldOnePhaseBundle_ThenTheLeaderIsCutOff_ASurvivorAnswersTheSameCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, InMemoryCommunication raftComm, _) = await AssembleClusterWithTransports(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureRaft: config => config.ProposalTimeout = HoldBound,
            configureKahuna: config => config.OnePhaseApplyTimeValidation = true);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        LostLeaderScenario? scenario = null;

        try
        {
            scenario = await CutOffLeaderAfterHeldOnePhaseBundleAsync(managers, rafts, raftComm, ct);

            await AssertSettledOnceOnSurvivorsAsync(managers, scenario, ct);

            // The client retries the same transaction against a survivor. Its session died with the lost node,
            // so the answer can only come from the canonical record — and it must be the recorded commit, with
            // nothing executed a second time (the revision above is re-checked after the retry).
            //
            // The retried handle carries the record anchor, which is the transaction's only route to its
            // canonical record. A handle that never learned one is unconsultable by construction and is
            // answered as unknown; that is a different gap, and not what this fixture is about.
            TransactionHandle retriedHandle = scenario.Handle with { RecordAnchorKey = scenario.Key };

            (KeyValueResponseType retriedType, _) = await RetryOnMustRetryAsync(
                () => managers[scenario.Successor].LocateAndCommitTransaction(retriedHandle, ct), r => r.Item1);

            Assert.Equal(KeyValueResponseType.Committed, retriedType);

            await AssertSettledOnceOnSurvivorsAsync(managers, scenario, ct);
        }
        finally
        {
            ObserveAbandonedCommit(scenario);

            if (scenario is not null)
                raftComm.HealPartition(EndpointOf(scenario.Lost));

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The cut-off node still owns the session, and a survivor that receives the client's retry probes it for
    /// that session; the client can also retry against it directly. By then the frozen decision deadline has
    /// passed, so its re-driven finalize fences the attempt through the record CAS — replication work a node
    /// that cannot reach the anchor leader cannot do. That failure decides nothing: the bundle committed on a
    /// quorum before the cut, so the only answers the owner may give are the recorded Committed or a retryable
    /// MustRetry, never a fabricated Aborted that would be retained and replayed to every later retry.
    /// </summary>
    [Fact]
    public async Task HeldOnePhaseBundle_ThenTheLeaderIsCutOff_ARetryOnTheCutOffOwnerPastTheDecisionDeadline_AnswersTheSameCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, InMemoryCommunication raftComm, _) = await AssembleClusterWithTransports(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureRaft: config => config.ProposalTimeout = HoldBound,
            configureKahuna: config =>
            {
                config.OnePhaseApplyTimeValidation = true;

                // A short frozen decision deadline, so the retry below is provably past it without a long wait.
                config.DurableDecisionDeadlineFloorMs = 1_000;
                config.DurableDecisionDeadlineCeilingMs = 1_000;
            });

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        LostLeaderScenario? scenario = null;

        try
        {
            scenario = await CutOffLeaderAfterHeldOnePhaseBundleAsync(managers, rafts, raftComm, ct);

            await AssertSettledOnceOnSurvivorsAsync(managers, scenario, ct);

            // The owner's own record froze the deadline; wait until any attempt minted now is past it.
            HLCTimestamp deadline = managers[scenario.Lost].DurableTransactionRecordStore.Get(scenario.Handle.TransactionId, 1)!.DecisionDeadline;
            await WaitUntilAsync(() => rafts[scenario.Lost].HybridLogicalClock.TrySendOrLocalEvent(rafts[scenario.Lost].GetLocalNodeId()) > deadline, timeoutMs: 30_000);

            TransactionHandle retriedHandle = scenario.Handle with { RecordAnchorKey = scenario.Key };

            // Straight at the owner: it still holds the session, so this re-drives its finalize.
            (KeyValueResponseType ownerType, _) = await RetryOnMustRetryAsync(
                () => managers[scenario.Lost].LocateAndCommitTransaction(retriedHandle, ct), r => r.Item1, timeoutMs: 60_000);

            Assert.Equal(KeyValueResponseType.Committed, ownerType);

            // And through the survivor that leads the partition now, which probes the owner for the session.
            (KeyValueResponseType successorType, _) = await RetryOnMustRetryAsync(
                () => managers[scenario.Successor].LocateAndCommitTransaction(retriedHandle, ct), r => r.Item1, timeoutMs: 60_000);

            Assert.Equal(KeyValueResponseType.Committed, successorType);

            await AssertSettledOnceOnSurvivorsAsync(managers, scenario, ct);
        }
        finally
        {
            ObserveAbandonedCommit(scenario);

            if (scenario is not null)
                raftComm.HealPartition(EndpointOf(scenario.Lost));

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    private static async Task<int> WaitForSurvivingLeader(IRaft[] rafts, int partition, int lost, CancellationToken ct)
    {
        int index = -1;
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (i == lost || !await rafts[i].AmILeaderIfHosted(partition, ct))
                    continue;

                index = i;
                return true;
            }

            return false;
        }, timeoutMs: 60_000);

        return index;
    }
}
