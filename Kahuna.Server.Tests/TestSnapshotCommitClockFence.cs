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
/// A snapshot read at T that found no writer to wait for promises that nothing commits at or below T for what it
/// read. A transaction that stages and commits the key afterwards must therefore be stamped above T, even when T was
/// minted by a clock ahead of every node in the cluster and the transaction's coordinator is a different node from
/// the key's leader. Two fences carry the promise: the read advances the key leader's clock past T, and the
/// coordinator mints the durable commit timestamp above the participant's stamp on the staged write.
/// </summary>
public sealed class TestSnapshotCommitClockFence : BaseCluster
{
    private const int Partitions = 6;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSnapshotCommitClockFence(ITestOutputHelper outputHelper)
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

    /// <summary>A key led by one node and a coordinator key led by a different node.</summary>
    private static async Task<(string Key, int KeyLeader, string CoordinatorKey, int CoordinatorLeader)> FindSplitPair(
        IRaft[] rafts, KahunaManager[] managers, CancellationToken ct)
    {
        string random = Guid.NewGuid().ToString("N")[..8];

        string key = $"fence{random}/k";
        int keyLeader = await LeaderIndexOf(managers[0].KeyValues.LocateDurablePartition(key).PartitionId, rafts, ct);

        for (int i = 0; i < 4_096; i++)
        {
            string candidate = $"fence{i}/{random}/coord";
            int coordinatorLeader = await LeaderIndexOf(managers[0].KeyValues.LocateDurablePartition(candidate).PartitionId, rafts, ct);
            if (coordinatorLeader != keyLeader)
                return (key, keyLeader, candidate, coordinatorLeader);
        }

        throw new InvalidOperationException("no coordinator key resolved to a partition led by another node");
    }

    /// <summary>
    /// The coordinator's clock can lag the participant that staged a write (it has not heard from that node since
    /// the participant fenced its clock past a snapshot). The durable commit timestamp must still land above every
    /// staged stamp, not only above the transaction id.
    /// </summary>
    [Fact]
    public void DurableCommitTimestamp_IsMintedAboveEveryStagedStamp()
    {
        HybridLogicalClock lagging = new(() => 1_000);
        HLCTimestamp transactionId = new(1, 900, 0);
        HLCTimestamp ahead = new(2, 50_000, 3);

        Dictionary<string, StagedValue> staged = new(StringComparer.Ordinal)
        {
            ["a"] = new StagedValue("x"u8.ToArray(), KeyValueState.Set, 1, 0, false, new HLCTimestamp(2, 950, 0)),
            ["b"] = new StagedValue(null, KeyValueState.Deleted, 4, 0, false, ahead),
            ["c"] = new StagedValue("y"u8.ToArray(), KeyValueState.Set, 2, 0, false) // stamp unknown
        };

        HLCTimestamp commit = TransactionCoordinator.MintDurableCommitTimestamp(lagging, 1, transactionId, staged);

        Assert.True(commit > ahead, $"commit {commit} is not above the staged stamp {ahead}");
        Assert.True(commit > transactionId);
    }

    /// <summary>Restaging a key within one transaction keeps the highest participant stamp the key took.</summary>
    [Fact]
    public void StageMutation_KeepsTheHighestStampForAKey()
    {
        TransactionContext context = new() { CoordinatorKey = "c", TransactionId = new HLCTimestamp(1, 100, 0) };
        HLCTimestamp high = new(2, 500, 0);

        context.StageMutation("k", "a"u8.ToArray(), KeyValueState.Set, 1, 0, false, high);
        context.StageMutation("k", "b"u8.ToArray(), KeyValueState.Set, 1, 0, false, new HLCTimestamp(2, 400, 0));

        Assert.Equal(high, context.StagedMutations!["k"].StagedAt);
        Assert.Equal("b"u8.ToArray(), context.StagedMutations["k"].Value);
    }

    [Fact]
    public async Task CommitAfterSnapshotRead_IsStampedAboveTheSnapshot_WithARemoteCoordinator()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (string key, int keyLeader, string coordinatorKey, int coordinatorLeader) = await FindSplitPair(rafts, managers, ct);
            KahunaManager reader = managers[keyLeader];
            KahunaManager coordinator = managers[coordinatorLeader];

            (KeyValueResponseType seedType, _, _) = await reader.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v0"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, seedType);

            // A snapshot minted by a clock ahead of every node in the cluster (within the lead a node will fold).
            HLCTimestamp snapshot = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2_000, 0);

            (KeyValueResponseType firstType, ReadOnlyKeyValueEntry? first) = await reader.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, firstType);
            Assert.Equal("v0", Encoding.UTF8.GetString(first!.Value!));

            // A transaction begun and committed after the read, driven through a coordinator on another node.
            (KeyValueResponseType startType, TransactionHandle handle) = await coordinator.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey = coordinatorKey,
                    Locking = KeyValueTransactionLocking.Optimistic,
                    Timeout = 60_000
                }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            Assert.True(handle.TransactionId < snapshot, "the transaction id comes from a clock behind the snapshot");

            (KeyValueResponseType setType, _, _) = await coordinator.LocateAndTrySetKeyValue(
                handle.TransactionId, key, "v1"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, setType);

            (KeyValueResponseType commitType, _) = await coordinator.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, commitType);

            (KeyValueResponseType latestType, ReadOnlyKeyValueEntry? latest) = await reader.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, latestType);
            Assert.Equal("v1", Encoding.UTF8.GetString(latest!.Value!));
            Assert.True(latest.LastModified > snapshot,
                $"commit stamped at {latest.LastModified}, at or below the snapshot {snapshot} a prior read was served at");

            (KeyValueResponseType againType, ReadOnlyKeyValueEntry? again) = await reader.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, againType);
            Assert.Equal("v0", Encoding.UTF8.GetString(again!.Value!));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
