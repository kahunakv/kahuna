using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The one-phase bundle validates on the leader of its anchor partition before it is proposed, and once appended it
/// cannot be refused: its decision shares the durable batch with its prepare. The staged write intents that the
/// validation confirmed live in that leader's memory only, and until the bundle applies they are what makes a
/// snapshot read wait for the staged write. If the partition changes leader between the validation and the
/// propose, the new leader holds no intent, answers a snapshot read without the staged write, and would then apply
/// the bundle inside that read's snapshot. The bundle is therefore fenced to the term the validation ran under: the
/// new leader refuses it before anything is appended, and the retried commit finds the staged intent gone and
/// refuses the transaction. Two reads at one snapshot agree either way.
/// </summary>
public sealed class TestSnapshotReadStagedWriteWindowCluster : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSnapshotReadStagedWriteWindowCluster(ITestOutputHelper outputHelper)
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

    private static async Task MoveLeadership(IRaft[] rafts, int partition, int from, int to, CancellationToken ct)
    {
        RaftOperationStatus status = await rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
        await WaitUntilAsync(async () => !await rafts[from].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
    }

    /// <summary>
    /// A snapshot read at <paramref name="readTimestamp"/> (or the current state for Zero), retried while the
    /// key's leader asks it to wait or to retry. Answers the response type and, for a hit, the value.
    /// </summary>
    private static async Task<(KeyValueResponseType Type, string? Value)> ReadAt(
        KahunaManager manager, string key, HLCTimestamp readTimestamp, CancellationToken ct)
    {
        while (true)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await manager.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, readTimestamp, KeyValueDurability.Persistent, ct);

            if (type is KeyValueResponseType.WaitingForReplication or KeyValueResponseType.MustRetry)
            {
                await Task.Delay(10, ct);
                continue;
            }

            return (type, type == KeyValueResponseType.Get ? Encoding.UTF8.GetString(entry!.Value!) : null);
        }
    }

    /// <summary>
    /// The anchor partition changes leader after the commit probe passed and before the bundle is proposed. A
    /// snapshot read served by the new leader in that window answers the old value, so the bundle must not commit
    /// inside that snapshot: the first commit attempt is refused before anything durable (the bundle is fenced to
    /// the term the probe ran under), and the retried commit refuses the transaction because its staged intent is
    /// gone. The read asked again at the same snapshot agrees with the first.
    /// </summary>
    [Fact]
    public async Task OnePhaseBundle_AnchorLeaderChangesBetweenProbeAndPropose_IsRefused_AndReadsAgree()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "srw-cluster-" + Guid.NewGuid().ToString("N")[..8] + "/k";
            int partition = managers[0].LocateRange(key).PartitionId;
            int leader = await LeaderIndexOf(partition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            // The session is coordinated on the written key, so its owner is the key's leader: the staged write,
            // the probe and the finalizer all run on this node until the leadership moves.
            KahunaManager coordinator = managers[leader];

            (KeyValueResponseType seedType, _, _) = await RetryOnMustRetryAsync(
                () => coordinator.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "V1"u8.ToArray(), null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, seedType);

            (KeyValueResponseType startType, TransactionHandle tx) = await coordinator.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey = key,
                    Locking = KeyValueTransactionLocking.Optimistic,
                    AsyncRelease = true,
                    Timeout = 60_000
                }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            Assert.True(coordinator.TransactionCoordinator.HasSession(tx.TransactionId), "the key's leader must own the session");

            (KeyValueResponseType writeType, _, _) = await coordinator.LocateAndTrySetKeyValue(
                tx.TransactionId, key, "V2"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
                KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, writeType);

            DurableTransactionFinalizer finalizer = coordinator.TransactionCoordinator.DurableFinalizerForTests;

            Task<(KeyValueResponseType Type, string? Value)>? firstRead = null;
            HLCTimestamp readTimestamp = HLCTimestamp.Zero;

            // Runs after the commit probe passed on the old leader and before the bundle is proposed: the
            // interleaving no external caller can time.
            finalizer.TestAfterReadSetValidationHook = async hookCt =>
            {
                finalizer.TestAfterReadSetValidationHook = null;

                await MoveLeadership(rafts, partition, leader, successor, ct);

                readTimestamp = coordinator.Raft.HybridLogicalClock.TrySendOrLocalEvent(coordinator.Raft.GetLocalNodeId());
                firstRead = ReadAt(coordinator, key, readTimestamp, ct);

                await Task.WhenAny(firstRead, Task.Delay(2_000, hookCt));
            };

            KeyValueResponseType firstAttempt;
            try
            {
                (firstAttempt, _) = await coordinator.LocateAndCommitTransaction(tx, ct);
            }
            finally
            {
                finalizer.TestAfterReadSetValidationHook = null;
            }

            Assert.NotNull(firstRead);

            // The new leader refused the bundle before anything was appended: a clean retry, never a commit.
            Assert.Equal(KeyValueResponseType.MustRetry, firstAttempt);

            (KeyValueResponseType finalType, _) = await RetryOnMustRetryAsync(
                () => coordinator.LocateAndCommitTransaction(tx, ct), r => r.Item1, timeoutMs: 30_000);

            // The retried commit's probe finds the staged intent gone with the old leadership.
            Assert.Equal(KeyValueResponseType.Aborted, finalType);

            (KeyValueResponseType Type, string? Value) first = await firstRead;
            (KeyValueResponseType Type, string? Value) second = await ReadAt(coordinator, key, readTimestamp, ct);

            Assert.Equal((KeyValueResponseType.Get, "V1"), first);
            Assert.True(first == second,
                $"two reads at {readTimestamp} disagree: first {first.Type}/{first.Value}, then {second.Type}/{second.Value}");

            foreach (KahunaManager reader in managers)
                Assert.Equal((KeyValueResponseType.Get, "V1"), await ReadAt(reader, key, HLCTimestamp.Zero, ct));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
