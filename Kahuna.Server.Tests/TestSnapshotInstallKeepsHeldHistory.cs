using System.Security.Cryptography;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A replica seeded by a whole-partition snapshot, then promoted, must answer a read at a held timestamp exactly as
/// the replicas that never left answer it. A key the partition changed after the hold has no revision at or before
/// the hold unless the snapshot carried one, and an as-of read reports a missing revision as "did not exist": the
/// shape is a branch of a database that silently reads almost no rows while its hold stays registered.
///
/// <para>The install is driven through the node as a leader's transfer drives it (the leader's own export delivered
/// to the follower's <c>ReceiveInstallSnapshot</c>), and every read goes through the public read entry points on the
/// promoted node.</para>
/// </summary>
public sealed class TestSnapshotInstallKeepsHeldHistory : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int Partition = 1;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSnapshotInstallKeepsHeldHistory(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

    private static byte[] Value(string text) => Encoding.UTF8.GetBytes(text);

    private static async Task<int> LeaderIndexOf(IRaft[] rafts, CancellationToken ct)
    {
        int index = -1;
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (!await rafts[i].AmILeaderIfHosted(Partition, ct))
                    continue;

                index = i;
                return true;
            }

            return false;
        }, timeoutMs: 30_000);

        return index;
    }

    /// <summary>A key space that hashes onto the partition, so its keys can also be read with one bucket scan.</summary>
    private static string KeySpaceOnPartition(KahunaManager probe)
    {
        string random = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; i < 65_536; i++)
        {
            string candidate = $"held{i}{random}";
            if (probe.KeyValues.LocateDurablePartition($"{candidate}/k").PartitionId == Partition)
                return candidate;
        }

        throw new InvalidOperationException("no key space hashes onto the partition");
    }

    private static async Task<HLCTimestamp> Set(KahunaManager session, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, HLCTimestamp committed) = await session.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Value(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return committed;
    }

    private static async Task<long> ReadCurrentTerm(IRaft raft, KahunaManager session, string key, CancellationToken ct)
    {
        TaskCompletionSource<long> term = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using IDisposable registration = raft.HoldCommittedProposalRepliesForTesting(Partition, reply =>
        {
            term.TrySetResult(reply.Term);
            reply.Release();
        });

        int round = 0;
        while (!term.Task.IsCompleted)
        {
            await Set(session, key, $"term-{round++}", ct);
            await Task.Delay(20, ct);
        }

        return await term.Task;
    }

    private static async Task WaitForStableCommitIndex(IRaft raft, CancellationToken ct)
    {
        long previous = -1;
        await WaitUntilAsync(async () =>
        {
            long current = raft.GetCommitIndex(Partition);
            await Task.Delay(200, ct);
            bool stable = current == previous && current == raft.GetCommitIndex(Partition);
            previous = current;
            return stable;
        }, timeoutMs: 30_000);
    }

    private static async Task Install(IRaft[] rafts, KahunaManager leaderManager, int leader, int installee, long term, CancellationToken ct)
    {
        await WaitForStableCommitIndex(rafts[leader], ct);
        long snapshotIndex = rafts[leader].GetCommitIndex(Partition);

        byte[] payload;
        await using (Stream stream = await leaderManager.KeyValues.PartitionStateTransfer.ExportPartitionState(Partition, snapshotIndex, ct))
        {
            using MemoryStream buffer = new();
            await stream.CopyToAsync(buffer, ct);
            payload = buffer.ToArray();
        }

        SnapshotResponse response = await ((RaftManager)rafts[installee]).ReceiveInstallSnapshot(new SnapshotRequest
        {
            SessionId = Guid.NewGuid().ToString("N"),
            PartitionId = Partition,
            SnapshotIndex = snapshotIndex,
            LeaderTerm = term,
            LastIncludedTerm = term,
            LeaderEndpoint = EndpointOf(leader),
            FollowerEndpoint = EndpointOf(installee),
            ChunkIndex = 0,
            IsLast = true,
            Data = payload,
            Kind = SnapshotKind.PartitionState,
            SnapshotChecksum = Convert.ToHexString(SHA256.HashData(payload))
        }, ct);

        Assert.True(response.Success, "the follower refused the snapshot install");
    }

    private static async Task MoveLeadership(IRaft[] rafts, int from, int to, CancellationToken ct)
    {
        Assert.Equal(RaftOperationStatus.Success, await rafts[from].TransferLeadershipAsync(Partition, EndpointOf(to), ct));
        await WaitUntilAsync(() => rafts[to].AmILeaderIfHosted(Partition, ct).AsTask(), timeoutMs: 30_000);
    }

    private static async Task AssertValueAt(KahunaManager node, string key, HLCTimestamp at, string expected, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, at, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal(expected, Encoding.UTF8.GetString(entry!.Value!));
    }

    [Fact]
    public async Task ThePromotedReplicaAnswersHeldTimestampsAndRefusesTheOthers()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        int installee = -1;

        try
        {
            string space = KeySpaceOnPartition(managers[0]);
            string untouched = $"{space}/untouched";
            string updated = $"{space}/updated";
            string deleted = $"{space}/deleted";
            string inserted = $"{space}/inserted";
            string warm = $"{space}/warm";

            int leader = await LeaderIndexOf(rafts, ct);
            installee = (leader + 1) % Nodes;
            KahunaManager session = managers[leader];

            long term = await ReadCurrentTerm(rafts[leader], session, warm, ct);

            await Set(session, untouched, "held", ct);
            await Set(session, updated, "held", ct);
            await Set(session, deleted, "held", ct);

            (_, ReadOnlyKeyValueEntry? lastHeld) = await session.LocateAndTryGetValue(
                HLCTimestamp.Zero, deleted, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            HLCTimestamp held = lastHeld!.LastModified;

            (KeyValueResponseType holdType, _, _) = await session.LocateAndAcquireSnapshotHold("install-test", held, leaseMs: 600_000, ct);
            Assert.Equal(KeyValueResponseType.Set, holdType);

            await WaitUntilAsync(
                () => managers.All(m => m.KeyValues.SnapshotFloorStore.Holds.Values.Any(h => h.Timestamp == held)),
                timeoutMs: 30_000);

            // Moved past the hold: more revisions than the resident archive keeps, then a delete and a new key.
            await Set(session, updated, "between", ct);
            (_, ReadOnlyKeyValueEntry? between) = await session.LocateAndTryGetValue(
                HLCTimestamp.Zero, updated, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            HLCTimestamp notHeld = between!.LastModified;

            for (int i = 0; i < 8; i++)
                await Set(session, updated, $"after-{i}", ct);

            (KeyValueResponseType deleteType, _, _) = await session.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, deleted, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Deleted, deleteType);

            await Set(session, inserted, "new", ct);

            // Age the writes above out of the window the export keeps whole, so only the hold can carry them.
            await Task.Delay(TimeSpan.FromMilliseconds(CommitAdmission.MaxClockLeadMs + 1_000), ct);

            await Install(rafts, session, leader, installee, term, ct);
            await MoveLeadership(rafts, leader, installee, ct);

            KahunaManager promoted = managers[installee];

            // Every shape answers the held timestamp exactly.
            await AssertValueAt(promoted, untouched, held, "held", ct);
            await AssertValueAt(promoted, updated, held, "held", ct);
            await AssertValueAt(promoted, deleted, held, "held", ct);

            (KeyValueResponseType insertedType, _) = await promoted.LocateAndTryGetValue(
                HLCTimestamp.Zero, inserted, -1, held, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, insertedType);

            (KeyValueResponseType existsType, _) = await promoted.LocateAndTryExistsValue(
                HLCTimestamp.Zero, updated, -1, held, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Exists, existsType);

            KeyValueGetByBucketResult bucket = await promoted.ScanAllByPrefix($"{space}/", held, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, bucket.Type);
            Assert.Contains(untouched, bucket.Items.Select(i => i.Item1));
            Assert.Contains(updated, bucket.Items.Select(i => i.Item1));
            Assert.Contains(deleted, bucket.Items.Select(i => i.Item1));
            Assert.DoesNotContain(inserted, bucket.Items.Select(i => i.Item1));

            KeyValueGetByRangeResult range = await promoted.LocateAndGetByRange(
                HLCTimestamp.Zero, $"{space}/", null, true, null, false, 100, held, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, range.Type);
            Assert.Equal(
                new[] { deleted, untouched, updated }.Order(StringComparer.Ordinal),
                range.Items.Select(i => i.Item1).Where(key => key != warm).Order(StringComparer.Ordinal));
            Assert.Equal("held", Encoding.UTF8.GetString(range.Items.Single(i => i.Item1 == updated).Item2.Value!));

            // Current state is untouched by any of this.
            await AssertValueAt(promoted, updated, HLCTimestamp.Zero, "after-7", ct);

            (KeyValueResponseType deletedNow, _) = await promoted.LocateAndTryGetValue(
                HLCTimestamp.Zero, deleted, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, deletedNow);

            // A timestamp nothing held is one this replica cannot answer: it refuses instead of reading the older pin.
            (KeyValueResponseType refused, _) = await promoted.LocateAndTryGetValue(
                HLCTimestamp.Zero, updated, -1, notHeld, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, refused);

            KeyValueGetByRangeResult refusedRange = await promoted.LocateAndGetByRange(
                HLCTimestamp.Zero, $"{space}/", null, true, null, false, 100, notHeld, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, refusedRange.Type);
            Assert.Empty(refusedRange.Items);

            // The replica that kept its history still answers it.
            await MoveLeadership(rafts, installee, leader, ct);
            await AssertValueAt(managers[leader], updated, notHeld, "between", ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
