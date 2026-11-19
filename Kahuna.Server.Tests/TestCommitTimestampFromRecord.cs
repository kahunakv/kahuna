using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A commit retried after the coordinator lost the session and its retained outcome is answered from the
/// transaction's canonical record. The answer carries the commit timestamp frozen in that record, which is the
/// same timestamp the first commit answered, so a caller that reports commit timestamps reports one value for
/// the transaction whichever answer it got.
///
/// <para>The retained outcome is turned off with <c>TransactionOutcomeRetentionMax = 0</c>. Then the finalize
/// removes the session and keeps no outcome, and the second commit reaches the record consult directly.</para>
/// </summary>
public sealed class TestCommitTimestampFromRecord : BaseCluster
{
    private const int Partitions = 4;

    private readonly ILoggerFactory loggerFactory;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestCommitTimestampFromRecord(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// One node: the second commit finds no session and no retained outcome, reads the committed record, and
    /// answers the first commit's timestamp.
    /// </summary>
    [Fact]
    public async Task Recommit_WithoutSessionOrRetainedOutcome_AnswersTheRecordCommitTimestamp()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = Partitions,
            TransactionOutcomeRetentionMax = 0
        }, loggerFactory);

        await node.StartAsync(ct);

        string prefix = "record-ts-" + Guid.NewGuid().ToString("N")[..8];
        await node.WaitForLeaderForKeyAsync(prefix + "/a", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        TransactionHandle handle = await StartTransaction(manager, null, ct);
        await Write(manager, handle, prefix + "/a", "va", ct);
        await Write(manager, handle, prefix + "/b", "vb", ct);

        TransactionCommitAnswer first = await manager.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, first.Type);
        Assert.NotNull(first.RecordAnchorKey);
        Assert.NotEqual(HLCTimestamp.Zero, first.CommitTimestamp);

        TransactionCommitAnswer again = await manager.LocateAndCommitTransaction(handle with { RecordAnchorKey = first.RecordAnchorKey }, ct);

        Assert.Equal(KeyValueResponseType.Committed, again.Type);
        Assert.Equal(first.RecordAnchorKey, again.RecordAnchorKey);
        Assert.Equal(first.CommitTimestamp, again.CommitTimestamp);
    }

    /// <summary>
    /// Three nodes: the coordinator partition and the record anchor's partition have different leaders, and the
    /// second commit is sent from the third node. The commit is forwarded to the coordinator node, which has no
    /// session and forwards the record lookup to the anchor leader. Both replies keep the commit timestamp.
    /// </summary>
    [Fact]
    public async Task Recommit_ForwardedToACoordinatorThatLostTheSession_AnswersTheRecordCommitTimestamp()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger,
            configureKahuna: configuration => configuration.TransactionOutcomeRetentionMax = 0);

        try
        {
            KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "record-ts-cluster");
            Assert.True(keys.Count >= 2, "the test needs keys on two different partitions");

            int[] partitions = [.. keys.Keys.Order()];
            int coordinatorPartition = partitions[0];
            int anchorPartition = partitions[1];
            string anchorKey = keys[anchorPartition];

            // Node 0 leads the coordinator partition and node 1 leads the anchor partition, so node 2 leads neither.
            await MoveLeadershipTo(rafts, coordinatorPartition, 0, ct);
            await MoveLeadershipTo(rafts, anchorPartition, 1, ct);

            TransactionHandle handle = await StartTransaction(managers[2], coordinatorPartition, ct);
            await Write(managers[2], handle, anchorKey, "va", ct);

            TransactionCommitAnswer first = await managers[2].LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, first.Type);
            Assert.Equal(anchorKey, first.RecordAnchorKey);
            Assert.NotEqual(HLCTimestamp.Zero, first.CommitTimestamp);

            TransactionCommitAnswer again = await managers[2].LocateAndCommitTransaction(handle with { RecordAnchorKey = first.RecordAnchorKey }, ct);

            Assert.Equal(KeyValueResponseType.Committed, again.Type);
            Assert.Equal(first.CommitTimestamp, again.CommitTimestamp);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    private static async Task<TransactionHandle> StartTransaction(KahunaManager manager, int? coordinatorPartition, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await manager.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorPartitionId = coordinatorPartition,
                Locking = KeyValueTransactionLocking.Optimistic,
                Timeout = 30_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static async Task Write(KahunaManager manager, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, _, _) = await manager.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    /// <summary>A fresh key per partition. Hash routing hashes the prefix before the last '/', so the candidates
    /// vary that prefix.</summary>
    private static Dictionary<int, string> FreshKeyPerPartition(KahunaManager probe, string tag)
    {
        string random = Guid.NewGuid().ToString("N")[..8];
        Dictionary<int, string> byPartition = [];

        for (int i = 0; i < 4_096 && byPartition.Count < Partitions; i++)
        {
            string candidate = $"{tag}{i}/{random}";
            int partition = probe.KeyValues.LocateDurablePartition(candidate).PartitionId;
            byPartition.TryAdd(partition, candidate);
        }

        return byPartition;
    }

    /// <summary>Moves the leadership of <paramref name="partition"/> to the node at <paramref name="target"/> and
    /// waits until that node answers as its leader.</summary>
    private static async Task MoveLeadershipTo(IRaft[] rafts, int partition, int target, CancellationToken ct)
    {
        if (await rafts[target].AmILeaderIfHosted(partition, ct))
            return;

        int current = -1;
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (!await rafts[i].AmILeaderIfHosted(partition, ct))
                    continue;

                current = i;
                return true;
            }

            return false;
        }, timeoutMs: 30_000);

        RaftOperationStatus status = await rafts[current].TransferLeadershipAsync(partition, $"localhost:{8001 + target}", ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[target].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
    }
}
