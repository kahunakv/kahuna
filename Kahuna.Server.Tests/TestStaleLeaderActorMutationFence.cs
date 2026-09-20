using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A leader cut off from both of its voters keeps believing it leads until a higher-term message
/// reaches it. In that window the operations that mutate in-memory actor state without a Raft
/// proposal — a transactional set, an exclusive lock and its release, the write-intent probe a
/// commit runs — must not be answered from the cut-off node: a staged write or a released lock
/// that lives only in its memory is a write the cluster never had. The quorum leader elected in
/// the meantime serves the same transaction, commits it through its quorum, and the value is
/// what every replica reads afterwards.
///
/// <para>The first fixture runs with the check-quorum step-down off, which models the nightly's
/// shape: the isolated leader never stepped down on its own, so the receiving-node gate is the
/// only thing standing between the stale node and the client. The second fixture turns the
/// step-down on and bounds how long the belief-only window can last at all.</para>
/// </summary>
public sealed class TestStaleLeaderActorMutationFence : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestStaleLeaderActorMutationFence(ITestOutputHelper outputHelper)
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
        }, timeoutMs: 30_000);

        return index;
    }

    [Fact]
    public async Task IsolatedLeader_RefusesActorMutations_WhileTheQuorumLeaderCommitsTheWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, InMemoryCommunication raftComm, _) = await AssembleClusterWithTransports(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            // The nightly's shape: nothing makes the isolated leader step down on its own.
            configureRaft: config => config.EnableCheckQuorum = false);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        int isolated = -1;

        try
        {
            string key = $"fence{Guid.NewGuid():N}/k";
            int partition = managers[0].KeyValues.LocateDurablePartition(key).PartitionId;
            isolated = await LeaderIndexOf(partition, rafts, ct);

            raftComm.PartitionNode(EndpointOf(isolated));

            int successor = await WaitForSurvivingLeader(rafts, partition, isolated, ct);
            Assert.NotEqual(isolated, successor);

            KahunaManager stale = managers[isolated];
            KahunaManager live = managers[successor];

            // Two leaders of one partition: the cut-off node still holds the role by local belief.
            Assert.True(await rafts[isolated].AmILeaderQuick(partition), "the isolated leader must still believe it leads for the window to exist");

            (KeyValueResponseType startType, TransactionHandle handle) = await RetryOnMustRetryAsync(
                () => live.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        CoordinatorKey = key,
                        Locking = KeyValueTransactionLocking.Pessimistic,
                        AsyncRelease = true,
                        Timeout = 60_000
                    }, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, startType);

            // Every actor-only mutation the stale node receives is refused, because its quorum will
            // not confirm the leadership it believes in. None of these touches Raft, so nothing else
            // would have stopped them.
            (KeyValueResponseType staleSet, _, _) = await stale.LocateAndTrySetKeyValue(
                handle.TransactionId, key, "stale"u8.ToArray(), null, -1,
                KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, staleSet);

            (KeyValueResponseType staleLock, _, _, _) = await stale.LocateAndTryAcquireExclusiveLock(
                handle.TransactionId, key, 10_000, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, staleLock);

            (KeyValueResponseType staleRelease, _) = await stale.LocateAndTryReleaseExclusiveLock(
                handle.TransactionId, key, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, staleRelease);

            KeyValueResponseType staleProbe = await stale.LocateAndTryCheckWriteIntent(
                handle.TransactionId, key, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, staleProbe);

            // The stale node staged nothing: no write intent for the transaction lives in its actor.
            (KeyValueResponseType staleLocalProbe, _) = await stale.TryGetValue(
                handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.DoesNotExist, staleLocalProbe);

            // The quorum leader takes the same transaction's write and commits it through its quorum.
            (KeyValueResponseType liveSet, _, _) = await RetryOnMustRetryAsync(
                () => live.LocateAndTrySetKeyValue(
                    handle.TransactionId, key, "alpha"u8.ToArray(), null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, liveSet);

            (KeyValueResponseType commitType, _) = await RetryOnMustRetryAsync(
                () => live.LocateAndCommitTransaction(handle, ct), r => r.Item1);
            Assert.Equal(KeyValueResponseType.Committed, commitType);

            // The acknowledged write is on every reachable replica, not only on the node that answered.
            for (int i = 0; i < managers.Length; i++)
            {
                if (i == isolated)
                    continue;

                int node = i;
                await WaitUntilAsync(async () =>
                {
                    (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await managers[node].TryGetValue(
                        HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                    return type == KeyValueResponseType.Get && entry?.Value is not null && entry.Value.AsSpan().SequenceEqual("alpha"u8);
                }, timeoutMs: 30_000);
            }

            // Once the network heals the stale node learns the newer term, follows, and converges on the
            // committed value — nothing it held in the window survives to contradict it.
            raftComm.HealPartition(EndpointOf(isolated));
            isolated = -1;

            await WaitUntilAsync(async () =>
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await stale.TryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                return type == KeyValueResponseType.Get && entry?.Value is not null && entry.Value.AsSpan().SequenceEqual("alpha"u8);
            }, timeoutMs: 60_000);
        }
        finally
        {
            if (isolated >= 0)
                raftComm.HealPartition(EndpointOf(isolated));

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    [Fact]
    public async Task CheckQuorum_StepsDownAnIsolatedLeader_InsideTheWindow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        TimeSpan window = TimeSpan.Zero;

        (IRaft[] rafts, IKahuna[] kahunas, InMemoryCommunication raftComm, _) = await AssembleClusterWithTransports(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureRaft: config =>
            {
                // The derived window: the largest one that still steps an isolated leader down before
                // any follower can start an election.
                config.EnableCheckQuorum = true;
                config.CheckQuorumIntervalMultiplier = 0;
                window = config.CheckQuorumWindow;
            });

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        int isolated = -1;

        try
        {
            string key = $"stepdown{Guid.NewGuid():N}/k";
            int partition = managers[0].KeyValues.LocateDurablePartition(key).PartitionId;
            isolated = await LeaderIndexOf(partition, rafts, ct);

            KahunaManager leader = managers[isolated];

            // Belief-only state admitted under the current leadership: a staged transactional write with
            // its write intent, and an exclusive lock on a second key.
            (KeyValueResponseType startType, TransactionHandle handle) = await RetryOnMustRetryAsync(
                () => leader.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        CoordinatorKey = key,
                        Locking = KeyValueTransactionLocking.Pessimistic,
                        AsyncRelease = true,
                        Timeout = 60_000
                    }, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, startType);

            (KeyValueResponseType stagedType, _, _) = await RetryOnMustRetryAsync(
                () => leader.LocateAndTrySetKeyValue(
                    handle.TransactionId, key, "staged"u8.ToArray(), null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, stagedType);

            (KeyValueResponseType stagedRead, ReadOnlyKeyValueEntry? stagedEntry) = await leader.TryGetValue(
                handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Get, stagedRead);
            Assert.Equal("staged"u8.ToArray(), stagedEntry!.Value);

            HLCTimestamp probe = rafts[isolated].HybridLogicalClock.TrySendOrLocalEvent(rafts[isolated].GetLocalNodeId());
            Assert.Equal(KeyValueResponseType.Aborted, await leader.TryCheckWriteIntentValue(probe, key, KeyValueDurability.Persistent));

            raftComm.PartitionNode(EndpointOf(isolated));

            // The step-down bound: the check-quorum window plus generous slack for the leader-check tick,
            // scaled like every other timer in the fixture.
            int budgetMs = (int)Math.Max(2_000, window.TotalMilliseconds * 4 + 1_000 * TimingScale);

            await WaitUntilAsync(async () => !await rafts[isolated].AmILeaderQuick(partition), timeoutMs: budgetMs);

            // The survivors elect among themselves; the stepped-down node is not the leader they name.
            int successor = await WaitForSurvivingLeader(rafts, partition, isolated, ct);
            Assert.NotEqual(isolated, successor);

            // Leadership lost: the staged write and its write intent are gone from the old leader's actor.
            // Nothing admitted under the lost term survives to be prepared under a later one.
            await WaitUntilAsync(async () =>
            {
                (KeyValueResponseType type, _) = await leader.TryGetValue(
                    handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                return type == KeyValueResponseType.DoesNotExist;
            }, timeoutMs: 10_000);

            Assert.Equal(KeyValueResponseType.DoesNotExist, await leader.TryCheckWriteIntentValue(probe, key, KeyValueDurability.Persistent));
        }
        finally
        {
            if (isolated >= 0)
                raftComm.HealPartition(EndpointOf(isolated));

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
