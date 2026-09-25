using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The per-partition apply fingerprint: the applied kv log id and the committed heads a node holds
/// for a partition. Replicas at the same applied log id must hold the same committed heads, and a
/// replica that does not is a divergence the cluster must surface by itself — at a promotion, as
/// an error-level log line plus a counter — and must refuse to spread: a split copies the moving
/// range from the source leader, so a leader holding fewer heads than a replica at the same log id
/// is an incomplete source and the split is refused before its copy.
/// </summary>
public sealed class TestPartitionApplyFingerprint : BaseCluster
{
    private const string Space = "t:f";

    private const int SystemPartition = 0;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestPartitionApplyFingerprint(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(int partition, (IRaft, KahunaManager)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeaderIfHosted(partition, ct))
                    return (raft, kahuna);
            await Task.Delay(50, ct);
        }
    }

    private static async Task<SplitOutcome> SplitViaLeaders(string space, string splitKey, (IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 5; attempt++)
        {
            (IRaft leaderRaft, KahunaManager leader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            int newPartitionId = RangeSplitter.ComputeNextPartitionId(leaderRaft, leader.RangeMapStore.Current);

            RaftPartitionLifecycleResult createResult =
                await leaderRaft.CreatePartitionAsync(newPartitionId, RaftRoutingMode.Unrouted, null, ct);

            if (!createResult.Success)
                return SplitOutcome.PartitionCreationFailed;

            SplitOutcome outcome = await leader.RangeSplitter.SplitAsync(space, splitKey, newPartitionId, ct);

            if (outcome.Status != SplitStatus.CutoverFailed)
                return outcome;

            await Task.Delay(100, ct);
        }

        return SplitOutcome.CutoverFailed;
    }

    /// <summary>
    /// Three nodes, the ranged space seeded on one data partition, and a set of keys committed through
    /// durable interactive transactions so the partition's committed-head ledger is populated.
    /// </summary>
    private async Task<((IRaft, KahunaManager)[] Nodes, int Partition, string[] Keys)> Setup()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(Space);

        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

        int partition = RangeMapStore.FirstDataPartitionId;

        bool committed = await metaLeader.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = partition, Generation = 1 }],
            ct);
        Assert.True(committed);

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1);

        (IRaft _, KahunaManager dataLeader) = await LeaderOf(partition, nodes);

        string[] keys = [.. Enumerable.Range(0, 8).Select(i => $"{Space}/{(char)('a' + i)}")];

        foreach (string key in keys)
        {
            string k = key;

            (KeyValueResponseType startType, TransactionHandle handle) = await RetryOnMustRetryAsync(
                () => dataLeader.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        CoordinatorKey = k,
                        Locking = KeyValueTransactionLocking.Pessimistic,
                        AsyncRelease = true,
                        Timeout = 30_000
                    }, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, startType);

            (KeyValueResponseType setType, _, _) = await RetryOnMustRetryAsync(
                () => dataLeader.LocateAndTrySetKeyValue(
                    handle.TransactionId, k, V("v"), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, setType);

            (KeyValueResponseType commitType, _) = await RetryOnMustRetryAsync(
                () => dataLeader.LocateAndCommitTransaction(handle, ct), r => r.Item1);
            Assert.Equal(KeyValueResponseType.Committed, commitType);
        }

        foreach (string key in keys)
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                string k = key;
                await WaitUntilAsync(async () =>
                {
                    (KeyValueResponseType rt, _) = await kahuna.TryGetValue(
                        HLCTimestamp.Zero, k, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                    return rt == KeyValueResponseType.Get;
                });
            }
        }

        return (nodes, partition, keys);
    }

    [Fact]
    public async Task Fingerprints_ConvergeAcrossReplicas_AfterDurableCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ((IRaft, KahunaManager)[] nodes, int partition, string[] keys) = await Setup();

        try
        {
            (IRaft _, KahunaManager leader) = await LeaderOf(partition, nodes);

            (KeyValueResponseType leaderType, _) = await leader.GetPartitionApplyFingerprint(partition, ct);
            Assert.Equal(KeyValueResponseType.Get, leaderType);

            // A committed head is recorded when the commit settles, after the client was answered.
            await WaitUntilAsync(async () =>
            {
                (_, KeyValueApplyFingerprint fingerprint) = await leader.GetPartitionApplyFingerprint(partition, ct);
                return fingerprint.CommittedHeads >= keys.Length;
            }, timeoutMs: 30_000);

            // Every replica reaches the leader's applied log id, and at that id holds the same heads: the
            // fingerprint is a pure function of the log.
            foreach ((IRaft _, KahunaManager replica) in nodes)
            {
                KahunaManager node = replica;
                await WaitUntilAsync(async () =>
                {
                    (KeyValueResponseType type, KeyValueApplyFingerprint fingerprint) = await node.GetPartitionApplyFingerprint(partition, ct);
                    (_, KeyValueApplyFingerprint current) = await leader.GetPartitionApplyFingerprint(partition, ct);
                    return type == KeyValueResponseType.Get
                        && fingerprint.AppliedLogId == current.AppliedLogId
                        && fingerprint.CommittedHeads == current.CommittedHeads;
                }, timeoutMs: 30_000);
            }

            // A partition this node does not host answers DoesNotExist, never a fingerprint of zeros.
            (KeyValueResponseType unknownType, _) = await leader.GetPartitionApplyFingerprint(9_999, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, unknownType);

            // The cluster-wide comparison from any node sees no divergence.
            ApplyFingerprintComparison comparison = await nodes[0].Item2.KeyValues.CompareApplyFingerprintWithReplicasAsync(partition, ct);
            Assert.True(comparison.IsDeterminate);
            Assert.False(comparison.HasDivergence);
            Assert.Equal(2, comparison.PeersAsked);
            Assert.Equal(2, comparison.PeersAnswered);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    [Fact]
    public async Task ShortLeader_IsReportedAtPromotion_RelinquishesLeadership_AndTheSplitRefusesAnIncompleteSource()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ((IRaft, KahunaManager)[] nodes, int partition, string[] keys) = await Setup();

        using MetricCapture metrics = new("partition",
            "kahuna.keyvalues.apply_divergence_detected",
            "kahuna.range.split.incomplete_source_refusals");

        KahunaManager? masked = null;

        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await LeaderOf(partition, nodes);
            masked = leader;

            // The leader reports one committed head LESS than it really holds, at its real applied log id: the
            // shape of a leader whose install marked entries applied without delivering them, seen against the
            // replicas that did apply them. The applied ids match because every node applies the same log.
            int maskedPartition = partition;
            leader.KeyValues.ApplyFingerprintOverrideForTesting = (id, real) =>
                id == maskedPartition && real is not null ? real.Value with { CommittedHeads = real.Value.CommittedHeads - 1 } : real;

            // Every replica must sit at the same applied log id for the counts to be comparable: the divergence
            // rule deliberately ignores a replica that is merely behind.
            KahunaManager leaderNode = leader;
            await WaitUntilAsync(async () =>
            {
                (_, KeyValueApplyFingerprint a) = await leaderNode.GetPartitionApplyFingerprint(partition, ct);
                foreach ((IRaft _, KahunaManager node) in nodes)
                {
                    (_, KeyValueApplyFingerprint b) = await node.GetPartitionApplyFingerprint(partition, ct);
                    if (a.AppliedLogId != b.AppliedLogId)
                        return false;
                }
                return true;
            }, timeoutMs: 30_000);

            // Promotion of the leader: the comparison runs off the notification, reports the divergence, and the
            // short leader hands the partition to a fuller peer and gates itself.
            double divergencesBefore = metrics.Total("kahuna.keyvalues.apply_divergence_detected");
            Assert.True(await leader.OnLeaderChanged(partition, leaderRaft.GetLocalEndpoint()));
            await WaitUntilAsync(() => metrics.Total("kahuna.keyvalues.apply_divergence_detected") > divergencesBefore, timeoutMs: 15_000);
            await WaitUntilAsync(() => leader.KeyValues.DivergenceContainment.IsGated(partition), timeoutMs: 15_000);
            await WaitUntilAsync(async () => !await leaderRaft.AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);

            (IRaft _, KahunaManager successor) = await LeaderOf(partition, nodes);
            Assert.NotSame(leader, successor);

            // Back to a truthful report and an open gate: the mask moves to the successor so the split's source
            // completeness gate is exercised on the leader it copies from.
            leader.KeyValues.ApplyFingerprintOverrideForTesting = null;
            await leader.KeyValues.DivergenceContainment.ClearAsync(partition);

            masked = successor;
            successor.KeyValues.ApplyFingerprintOverrideForTesting = (id, real) =>
                id == maskedPartition && real is not null ? real.Value with { CommittedHeads = real.Value.CommittedHeads - 1 } : real;

            KahunaManager successorNode = successor;
            await WaitUntilAsync(async () =>
            {
                (_, KeyValueApplyFingerprint a) = await successorNode.GetPartitionApplyFingerprint(partition, ct);
                foreach ((IRaft _, KahunaManager node) in nodes)
                {
                    (_, KeyValueApplyFingerprint b) = await node.GetPartitionApplyFingerprint(partition, ct);
                    if (a.AppliedLogId != b.AppliedLogId)
                        return false;
                }
                return true;
            }, timeoutMs: 30_000);

            // The split refuses to copy from the source leader while a replica holds more at the same id.
            double refusalsBefore = metrics.Total("kahuna.range.split.incomplete_source_refusals");
            SplitOutcome refused = await SplitViaLeaders(Space, keys[keys.Length / 2], nodes, ct);
            Assert.Equal(SplitStatus.SourceStateIncomplete, refused.Status);
            Assert.True(metrics.Total("kahuna.range.split.incomplete_source_refusals") > refusalsBefore);

            // The refusal left the map untouched: one descriptor, generation 1.
            Assert.Equal(1, successor.RangeMapStore.Current.Find(Space, keys[0])?.Generation);

            // With the leader reporting its real state again the same split goes through.
            successor.KeyValues.ApplyFingerprintOverrideForTesting = null;

            SplitOutcome accepted = await SplitViaLeaders(Space, keys[keys.Length / 2], nodes, ct);
            Assert.True(accepted.IsSuccess, $"expected the split to succeed once the replicas agree, got {accepted.Status} {accepted.Detail}");
        }
        finally
        {
            if (masked is not null)
                masked.KeyValues.ApplyFingerprintOverrideForTesting = null;

            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    /// <summary>
    /// Two replicas at the same applied log id with equal committed heads but different live-intent counts are
    /// divergent and reported as such. The counts do not say which side is incomplete (a settlement missed on
    /// one side reads the same as a prepare lost on the other), so the comparison names no side and containment
    /// is left to the heads and to recovery's peer cross-check.
    /// </summary>
    [Fact]
    public async Task ReplicaHoldingMoreLiveIntents_AtTheSameAppliedId_IsReportedDivergent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ((IRaft, KahunaManager)[] nodes, int partition, _) = await Setup();

        KahunaManager? masked = null;
        KahunaManager? leader = null;

        try
        {
            int maskedPartition = partition;
            Func<int, KeyValueApplyFingerprint?, KeyValueApplyFingerprint?> mask = (id, real) =>
                id == maskedPartition && real is not null ? real.Value with { LiveIntents = real.Value.LiveIntents + 2 } : real;

            // The comparison is a single shot by contract: it is indeterminate while the partition has no
            // settled leader and inconclusive while a replica is still catching up, and every production
            // caller retries it. Drive it the same way here, and follow the leader if an election moves it
            // (a loaded host trips check-quorum) — the mask always sits on a replica that does not lead.
            ApplyFingerprintComparison? comparison = null;

            await WaitUntilAsync(async () =>
            {
                (IRaft _, KahunaManager current) = await LeaderOf(partition, nodes);

                if (!ReferenceEquals(current, leader) || masked is null || ReferenceEquals(masked, current))
                {
                    leader = current;

                    if (masked is not null)
                        masked.KeyValues.ApplyFingerprintOverrideForTesting = null;

                    masked = null;
                    foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    {
                        if (!ReferenceEquals(kahuna, leader))
                        {
                            masked = kahuna;
                            break;
                        }
                    }

                    masked!.KeyValues.ApplyFingerprintOverrideForTesting = mask;
                }

                comparison = await current.KeyValues.CompareApplyFingerprintWithReplicasAsync(partition, ct);

                return comparison.IsDeterminate
                    && comparison.IsConclusive
                    && comparison.Leader == current.KeyValues.Raft.GetLocalEndpoint();
            }, timeoutMs: 30_000);

            Assert.NotNull(comparison);
            Assert.NotNull(masked);
            Assert.NotNull(leader);
            KahunaManager maskedNode = masked;

            Assert.True(comparison.IsDeterminate);
            Assert.True(comparison.HasDivergence);

            ApplyDivergentPeer divergent = Assert.Single(comparison.Divergent);
            Assert.Equal(maskedNode.KeyValues.Raft.GetLocalEndpoint(), divergent.Peer);
            Assert.False(divergent.HeadsDiffer);
            Assert.True(divergent.IntentsDiffer);
            Assert.Equal(ApplyDivergenceSide.Undetermined, divergent.Side);
            Assert.False(comparison.IsBehind(divergent.Peer));
            Assert.False(comparison.IsBehind(comparison.Leader!));
            Assert.Null(comparison.FullerPeerThanLeader());
            Assert.False(maskedNode.KeyValues.DivergenceContainment.IsGated(partition));
            Assert.False(leader.KeyValues.DivergenceContainment.IsGated(partition));
        }
        finally
        {
            if (masked is not null)
                masked.KeyValues.ApplyFingerprintOverrideForTesting = null;

            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }
}
