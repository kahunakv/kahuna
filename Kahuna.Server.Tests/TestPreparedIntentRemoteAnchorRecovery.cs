using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Recovery of a prepared intent whose record anchor lives on a partition led by another node. The
/// recovery sweep runs on the intent's key-partition leader; the presumed-abort it drives must route
/// to the anchor partition's leader rather than require this node to lead both. With the drive gated
/// on local anchor leadership, an abandoned cross-partition transaction whose two partitions have
/// different leaders could never be resolved by any node: the intent stayed undecided forever, every
/// scan of its key space answered MustRetry indefinitely, and once the intent's age crossed the
/// record-retention horizon the recordless hold made the wedge permanent.
/// </summary>
public sealed class TestPreparedIntentRemoteAnchorRecovery : BaseCluster
{
    private const int Partitions = 6;

    private const string Space = "xw:s";

    /// <summary>The key that carries the orphan intent; scans of the space must reach it.</summary>
    private const string WedgedKey = Space + "/z0";

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestPreparedIntentRemoteAnchorRecovery(ITestOutputHelper outputHelper)
    {
        Microsoft.Extensions.Logging.ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IRaft>(loggerFactory);
        kahunaLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IKahuna>(loggerFactory);
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(
        int partition, IRaft[] rafts, KahunaManager[] kahunas, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return (rafts[i], kahunas[i]);

            await Task.Delay(50, ct);
        }
    }

    [Fact]
    public async Task OrphanIntent_AnchorLedElsewhere_SweepDrivesAbortAndUnblocksScans()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 0);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        foreach (KahunaManager kahuna in managers)
            kahuna.RegisterKeyRange(Space);

        (_, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, managers, ct);

        // The anchor key is hash-routed; its partition's leader is the node the abort drive must reach.
        const string anchorKey = "xw-tx/anchor-remote";
        int anchorPartition = metaLeader.KeyValues.LocateDurablePartition(anchorKey).PartitionId;
        (IRaft anchorRaft, _) = await LeaderOf(anchorPartition, rafts, managers, ct);

        // The intent's key space lives on a partition led by a DIFFERENT node, so the sweeping node
        // (the key partition's leader) does not lead the anchor partition — the incident topology.
        int sourcePartition = 0;
        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
        {
            if (partitionId == anchorPartition)
                continue;
            if (!await anchorRaft.AmILeaderIfHosted(partitionId, ct))
            {
                sourcePartition = partitionId;
                break;
            }
        }

        Assert.NotEqual(0, sourcePartition);

        bool seeded = await metaLeader.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor { KeySpace = Space, PartitionId = sourcePartition, Generation = 1 }], ct);
        Assert.True(seeded);

        foreach (KahunaManager kahuna in managers)
            await WaitUntilAsync(
                () => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1, timeoutMs: 30_000);

        foreach (string key in new[] { Space + "/a0", WedgedKey })
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => metaLeader.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V("base"), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        (IRaft sourceRaft, KahunaManager sourceLeader) = await LeaderOf(sourcePartition, rafts, managers, ct);
        Assert.False(await sourceRaft.AmILeaderIfHosted(anchorPartition, ct));

        // The orphan: a pending intent past its recovery deadline with no record anywhere — the
        // abandoned transaction. Its age stays inside the retention horizon, so the recordless hold
        // does not apply and the presumed-abort protocol owns it.
        HLCTimestamp now = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp txId = new(now.N, now.L - 40_000, now.C);

        PreparedIntent intent = new(
            TransactionId: txId, Epoch: 1, Key: WedgedKey, ManifestHash: 42, RecordAnchorKey: anchorKey,
            CommitTimestamp: new HLCTimestamp(now.N, now.L - 40_000, now.C),
            State: KeyValueState.Set, Value: V("orphaned"), Bucket: Space,
            Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 0, BaseState: KeyValueState.Set,
            RecoveryDeadline: new HLCTimestamp(now.N, now.L - 30_000, now.C),
            Resolution: PreparedIntentResolution.Pending);

        Assert.True(await metaLeader.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(
            sourcePartition, Array.Empty<TransactionRecord>(), new List<PreparedIntent> { intent }, ct));

        await WaitUntilAsync(
            () => managers.Any(m => m.DurablePreparedIntentStore.Get(WedgedKey) is { } i && i.TransactionId == txId),
            timeoutMs: 30_000);

        // The wedge symptom: a scan whose page reaches the undecided intent answers MustRetry.
        KeyValueGetByRangeResult wedged = await metaLeader.LocateAndGetByRange(
            HLCTimestamp.Zero, Space, null, true, null, false, 10,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, wedged.Type);

        // The production resolver: the periodic recovery sweep on the key partition's leader. It
        // must drive the presumed abort to the anchor partition's leader over the wire and settle
        // the intent — with the drive gated on local anchor leadership it answered null forever.
        await WaitUntilAsync(async () =>
        {
            await sourceLeader.KeyValues.RecoverPreparedIntents(ct);
            return managers.All(m => m.DurablePreparedIntentStore.Get(WedgedKey) is null);
        }, timeoutMs: 30_000);

        // The abort tombstone is the canonical outcome at the anchor.
        await WaitUntilAsync(
            () => managers.Any(m => m.DurableTransactionRecordStore.Get(txId, 1) is { Decision: TransactionDecision.Abort }),
            timeoutMs: 30_000);

        // And the key space scans again, serving the base rows the aborted intent sat over.
        KeyValueGetByRangeResult unblocked = await RetryOnMustRetryAsync(
            () => metaLeader.LocateAndGetByRange(
                HLCTimestamp.Zero, Space, null, true, null, false, 10,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Type, timeoutMs: 30_000);

        Assert.Equal(KeyValueResponseType.Get, unblocked.Type);
        Assert.Equal(2, unblocked.Items.Count);
        Assert.Equal("base", Encoding.UTF8.GetString(unblocked.Items.First(i => i.Item1 == WedgedKey).Item2.Value!));
    }
}
