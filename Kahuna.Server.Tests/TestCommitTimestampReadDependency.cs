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
/// A transaction that reads a value and then writes commits at a later timestamp than the commit of the value it
/// read, when the read and the write are on different partitions with different leaders.
///
/// <para>T1 commits key A at c1 on the clock of its coordinator, which runs ahead of every other node. No Raft
/// message carried that lead yet, so the clocks of A's leader, of B's leader and of T2's coordinator are all below
/// c1. The test installs T1's committed write on A's leader with c1 three seconds ahead of every clock, which is the
/// state such a lead leaves. Three seconds stays inside the commit admission bound, so a cluster that turns the
/// admission window on can also produce it. T2 reads A, sees T1's value, writes key B on another partition and
/// commits. Its commit timestamp must be above c1. Otherwise a snapshot read between the two commits sees T2's
/// write and not the value T2 read.</para>
///
/// <para>The read shapes are the ones a transaction issues before it writes: a latest read of a transaction that
/// validates its reads, a latest read of one that does not, a statement read at a timestamp at or above c1, a range
/// scan, a bucket scan, a batch get, a batched probe and a batched range probe. T2 is started before T1 commits
/// and after it.</para>
/// </summary>
public sealed class TestCommitTimestampReadDependency : BaseCluster
{
    private const int Partitions = 4;

    /// <summary>How far ahead of every clock T1's commit timestamp is. Below the 5 s commit admission bound and the
    /// 5 s lead a snapshot read may have over the serving node's clock.</summary>
    private const long LeadMs = 3_000;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestCommitTimestampReadDependency(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    public enum ReadShape
    {
        /// <summary>An optimistic transaction that validates its reads: a latest read, folded into the read set.</summary>
        ValidatedLatest,

        /// <summary>A pessimistic transaction that does not validate its reads: a latest read.</summary>
        Latest,

        /// <summary>A pessimistic transaction that reads at a statement timestamp at or above c1.</summary>
        Statement,

        /// <summary>A pessimistic transaction that scans A's key space: a latest range scan.</summary>
        RangeScan,

        /// <summary>A pessimistic transaction that reads A in a batch get.</summary>
        GetMany,

        /// <summary>A pessimistic transaction that reads A in a batched probe.</summary>
        Probe,

        /// <summary>A pessimistic transaction that scans A's bucket.</summary>
        BucketScan,

        /// <summary>A pessimistic transaction that reads A in a batched range probe over A's key space.</summary>
        RangeProbe
    }

    [Theory]
    [InlineData(ReadShape.ValidatedLatest, true, false)]
    [InlineData(ReadShape.ValidatedLatest, false, false)]
    [InlineData(ReadShape.Latest, true, false)]
    [InlineData(ReadShape.Latest, false, false)]
    [InlineData(ReadShape.Statement, true, false)]
    [InlineData(ReadShape.Statement, false, false)]
    [InlineData(ReadShape.ValidatedLatest, true, true)]
    [InlineData(ReadShape.Latest, true, true)]
    [InlineData(ReadShape.Statement, true, true)]
    [InlineData(ReadShape.RangeScan, true, false)]
    [InlineData(ReadShape.GetMany, true, false)]
    [InlineData(ReadShape.Probe, true, false)]
    [InlineData(ReadShape.BucketScan, true, false)]
    [InlineData(ReadShape.RangeProbe, true, false)]
    public async Task ReadThenWriteOnAnotherPartition_CommitsAboveTheCommitItRead(ReadShape shape, bool startedBeforeT1Committed, bool coordinatorOnWriteLeader)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(3, "memory", Partitions, raftLogger, kahunaLogger);

        try
        {
            KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "read-dep");
            Assert.True(keys.Count >= 3, "the test needs keys on three different partitions");

            int[] partitions = [.. keys.Keys.Order()];
            (int readPartition, int writePartition, int coordinatorPartition) = (partitions[0], partitions[1], partitions[2]);
            (string keyA, string keyB) = (keys[readPartition], keys[writePartition]);

            // A's leader is node 0 and B's leader is node 1. T2's coordinator is node 2, or B's leader.
            const int readLeader = 0, writeLeader = 1;
            int coordinatorLeader = coordinatorOnWriteLeader ? writeLeader : 2;

            await MoveLeadershipTo(rafts, readPartition, readLeader, ct);
            await MoveLeadershipTo(rafts, writePartition, writeLeader, ct);
            await MoveLeadershipTo(rafts, coordinatorPartition, coordinatorLeader, ct);

            KahunaManager session = managers[coordinatorLeader];

            TransactionHandle t2 = default;
            if (startedBeforeT1Committed)
                t2 = await StartTransaction(session, coordinatorPartition, shape, ct);

            HLCTimestamp c1 = InstallCommittedWrite(managers[readLeader], rafts, keyA, "t1-value");

            if (!startedBeforeT1Committed)
                t2 = await StartTransaction(session, coordinatorPartition, shape, ct);

            ReadOnlyKeyValueEntry read = await Read(session, t2, keyA, shape, c1, ct);

            Assert.Equal("t1-value", Encoding.UTF8.GetString(read.Value!));
            Assert.Equal(c1, read.LastModified);

            await Write(session, t2, keyB, "t2-value", shape, ct);

            TransactionCommitAnswer commit = await session.LocateAndCommitTransaction(t2, ct);
            Assert.Equal(KeyValueResponseType.Committed, commit.Type);

            HLCTimestamp c2 = commit.CommitTimestamp;
            Assert.NotEqual(HLCTimestamp.Zero, c2);
            Assert.True(c2 > c1, $"T2 read T1's write committed at {c1} but committed at {c2}, at or below it");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A script transaction reads A on the coordinator node and writes B. A script returns no commit timestamp, so
    /// the order shows in B's <c>LastModified</c>, which is the commit timestamp B was written at.
    /// </summary>
    [Fact]
    public async Task ScriptReadThenWriteOnAnotherPartition_WritesAboveTheCommitItRead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(3, "memory", Partitions, raftLogger, kahunaLogger);

        try
        {
            KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "read-dep-script");
            Assert.True(keys.Count >= 2, "the test needs keys on two different partitions");

            int[] partitions = [.. keys.Keys.Order()];
            (int readPartition, int writePartition) = (partitions[0], partitions[1]);
            (string keyA, string keyB) = (keys[readPartition], keys[writePartition]);

            await MoveLeadershipTo(rafts, readPartition, 0, ct);
            await MoveLeadershipTo(rafts, writePartition, 1, ct);

            HLCTimestamp c1 = InstallCommittedWrite(managers[0], rafts, keyA, "t1-value");

            const string script = """
            BEGIN (locking=pessimistic, timeout=20000)
              LET a = GET @a
              SET @b "t2-value"
              COMMIT
            END
            """;

            KeyValueTransactionResult result = await RetryOnMustRetry(
                managers[2], Encoding.UTF8.GetBytes(script), null,
                [new() { Key = "@a", Value = keyA }, new() { Key = "@b", Value = keyB }]);

            Assert.Equal(KeyValueResponseType.Set, result.Type);

            (KeyValueResponseType readType, ReadOnlyKeyValueEntry? written) = await managers[2].LocateAndTryGetValue(
                HLCTimestamp.Zero, keyB, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, readType);
            Assert.NotNull(written);
            Assert.Equal("t2-value", Encoding.UTF8.GetString(written.Value!));
            Assert.True(written.LastModified > c1, $"the script read A committed at {c1} but wrote B at {written.LastModified}, at or below it");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Installs T1's committed write of <paramref name="key"/> on the key's leader, with a commit timestamp
    /// <see cref="LeadMs"/> ahead of the highest clock in the cluster. Only the stores change: no clock is moved.
    /// </summary>
    private static HLCTimestamp InstallCommittedWrite(KahunaManager leader, IRaft[] rafts, string key, string value)
    {
        HLCTimestamp highest = HLCTimestamp.Zero;
        foreach (IRaft raft in rafts)
        {
            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            if (now > highest)
                highest = now;
        }

        HLCTimestamp transactionId = new(1, highest.L + LeadMs - 1, 0);
        HLCTimestamp commitTimestamp = new(1, highest.L + LeadMs, 0);

        PreparedIntent intent = new(
            TransactionId: transactionId, Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
            CommitTimestamp: commitTimestamp, State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes(value), Bucket: null,
            Revision: 0, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: -1, BaseState: KeyValueState.Undefined,
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending);

        PreparedIntentStore store = leader.DurablePreparedIntentStore;
        store.Apply(new PrepareIntentCommand(intent));
        store.Apply(new ResolveIntentCommand(transactionId, intent.Epoch, key, Commit: true));

        return commitTimestamp;
    }

    /// <summary>Reads <paramref name="key"/> inside <paramref name="handle"/> with the read shape under test, and
    /// returns the entry it served.</summary>
    private static async Task<ReadOnlyKeyValueEntry> Read(
        KahunaManager session, TransactionHandle handle, string key, ReadShape shape, HLCTimestamp c1, CancellationToken ct)
    {
        switch (shape)
        {
            case ReadShape.RangeScan:
            {
                string keySpace = key[..key.LastIndexOf('/')];

                KeyValueGetByRangeResult page = await session.LocateAndGetByRange(
                    handle.TransactionId, keySpace, null, true, null, true, 100, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

                Assert.Equal(KeyValueResponseType.Get, page.Type);
                return Assert.Single(page.Items, item => item.Item1 == key).Item2;
            }

            case ReadShape.BucketScan:
            {
                string bucket = key[..key.LastIndexOf('/')];

                KeyValueGetByBucketResult scan = await session.LocateAndGetByBucket(
                    handle.TransactionId, bucket, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

                Assert.Equal(KeyValueResponseType.Get, scan.Type);
                return Assert.Single(scan.Items, item => item.Item1 == key).Item2;
            }

            case ReadShape.RangeProbe:
            {
                string keySpace = key[..key.LastIndexOf('/')];

                KeyValueProbeRangeResult[] results = await session.LocateAndTryProbeManyRanges(
                    handle.TransactionId, HLCTimestamp.Zero, keySpace, [new KeyValueProbeRange(null, true, null, true, 100)],
                    KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

                KeyValueProbeRangeResult result = Assert.Single(results);
                Assert.Equal(KeyValueResponseType.Get, result.Type);
                return Assert.Single(result.Items, item => item.Item1 == key).Item2;
            }

            case ReadShape.GetMany:
            {
                List<(KeyValueResponseType Type, string Key, KeyValueDurability Durability, ReadOnlyKeyValueEntry? Entry)> results = await session.LocateAndTryGetManyValues(
                    handle.TransactionId, HLCTimestamp.Zero, [(key, -1, KeyValueDurability.Persistent)], ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

                (KeyValueResponseType type, _, _, ReadOnlyKeyValueEntry? entry) = Assert.Single(results);
                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.NotNull(entry);
                return entry;
            }

            case ReadShape.Probe:
            {
                KeyValueProbeResult[] results = await session.LocateAndTryProbeMany(
                    handle.TransactionId, HLCTimestamp.Zero, [key], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

                KeyValueProbeResult result = Assert.Single(results);
                Assert.Equal(KeyValueResponseType.Get, result.Type);
                Assert.NotNull(result.Entry);
                return result.Entry;
            }

            default:
            {
                HLCTimestamp readTimestamp = shape == ReadShape.Statement ? c1 : HLCTimestamp.Zero;
                KeyValueReadMode readMode = shape == ReadShape.Statement ? KeyValueReadMode.StatementSnapshot : KeyValueReadMode.Default;

                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await session.LocateAndTryGetValue(
                    handle.TransactionId, key, -1, readTimestamp, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom(), readMode: readMode);

                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.NotNull(entry);
                return entry;
            }
        }
    }

    private static async Task<TransactionHandle> StartTransaction(KahunaManager session, int coordinatorPartition, ReadShape shape, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorPartitionId = coordinatorPartition,
                Locking = shape == ReadShape.ValidatedLatest ? KeyValueTransactionLocking.Optimistic : KeyValueTransactionLocking.Pessimistic,
                ReadValidation = shape == ReadShape.ValidatedLatest ? ReadValidation.TrackAndValidate : ReadValidation.None,
                Timeout = 30_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static async Task Write(KahunaManager session, TransactionHandle handle, string key, string value, ReadShape shape, CancellationToken ct)
    {
        if (shape != ReadShape.ValidatedLatest)
        {
            (KeyValueResponseType lockType, _, _, _) = await session.LocateAndTryAcquireExclusiveLock(
                handle.TransactionId, key, 30_000, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, lockType);
        }

        (KeyValueResponseType writeType, _, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
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
