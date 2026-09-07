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

    [Fact]
    public async Task HeldOnePhaseBundle_ThenTheLeaderIsCutOff_ASurvivorAnswersTheSameCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, InMemoryCommunication raftComm, _) = await AssembleClusterWithTransports(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureRaft: config => config.ProposalTimeout = HoldBound,
            configureKahuna: config => config.OnePhaseApplyTimeValidation = true);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        int lost = -1;
        Task<(KeyValueResponseType, string?)>? abandonedCommit = null;

        try
        {
            string key = $"lost{Guid.NewGuid():N}/k";
            int partition = managers[0].KeyValues.LocateDurablePartition(key).PartitionId;
            lost = await LeaderIndexOf(partition, rafts, ct);

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

            abandonedCommit = session.LocateAndCommitTransaction(handle, ct);

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

            // Exactly once: a first write settles at revision 0, so a re-applied settle would read as 1.
            for (int i = 0; i < managers.Length; i++)
            {
                if (i == lost)
                    continue;

                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
                    () => managers[i].LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                    r => r.Item1);

                Assert.Equal(KeyValueResponseType.Get, readType);
                Assert.Equal("alpha"u8.ToArray(), entry!.Value);
                Assert.Equal(0, entry.Revision);
            }

            // The client retries the same transaction against a survivor. Its session died with the lost node,
            // so the answer can only come from the canonical record — and it must be the recorded commit, with
            // nothing executed a second time (the revision above is re-checked after the retry).
            //
            // The retried handle carries the record anchor, which is the transaction's only route to its
            // canonical record. A handle that never learned one is unconsultable by construction and is
            // answered as unknown; that is a different gap, and not what this fixture is about.
            TransactionHandle retriedHandle = handle with { RecordAnchorKey = key };

            (KeyValueResponseType retriedType, _) = await RetryOnMustRetryAsync(
                () => managers[successor].LocateAndCommitTransaction(retriedHandle, ct), r => r.Item1);

            Assert.Equal(KeyValueResponseType.Committed, retriedType);

            (KeyValueResponseType afterRetryType, ReadOnlyKeyValueEntry? afterRetry) = await RetryOnMustRetryAsync(
                () => managers[successor].LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1);

            Assert.Equal(KeyValueResponseType.Get, afterRetryType);
            Assert.Equal("alpha"u8.ToArray(), afterRetry!.Value);
            Assert.Equal(0, afterRetry.Revision);
        }
        finally
        {
            // The abandoned finalize is never answered by the cluster; observe its eventual fault so it cannot
            // surface as an unobserved task exception in an unrelated test.
            if (abandonedCommit is not null)
                _ = abandonedCommit.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);

            if (lost >= 0)
                raftComm.HealPartition(EndpointOf(lost));

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
