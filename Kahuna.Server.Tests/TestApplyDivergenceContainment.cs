using System.Security.Cryptography;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A replica whose apply stream dropped committed entries — the shape of a snapshot install that raised the
/// apply cursor without delivering the entries below it — must not keep serving the partition once the
/// fingerprint comparison indicts it. Detection alone left the short replica leading in every fault soak that
/// found this shape; these tests pin the containment that follows it: a short leader hands leadership to the
/// fuller peer and refuses to serve from its own projection, a short follower gates itself at the leader change
/// and relinquishes at once if elected, a whole-partition install lifts the gate, and a record-less prepared
/// intent the peers have settled is recognised as this replica's divergence instead of a permanent hold.
/// </summary>
public sealed class TestApplyDivergenceContainment : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int Partition = 1;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly ITestOutputHelper output;

    public TestApplyDivergenceContainment(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
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

    private static string[] KeysOnPartition(KahunaManager probe, int partition, string tag, int count)
    {
        List<string> keys = new(count);
        string random = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; keys.Count < count && i < 65_536; i++)
        {
            string candidate = $"{tag}{i}{random}/k";
            if (probe.KeyValues.LocateDurablePartition(candidate).PartitionId == partition)
                keys.Add(candidate);
        }

        Assert.Equal(count, keys.Count);
        return [.. keys];
    }

    /// <summary>Commits one durable interactive transaction writing <paramref name="key"/>; returns its transaction id.</summary>
    private static async Task<HLCTimestamp> CommitKey(KahunaManager session, string key, string value, CancellationToken ct, int timeoutMs = 30_000)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await RetryOnMustRetryAsync(
            () => session.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey = key,
                    Locking = KeyValueTransactionLocking.Pessimistic,
                    AsyncRelease = true,
                    Timeout = timeoutMs
                }, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType setType, _, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTrySetKeyValue(
                handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Set, setType);

        (KeyValueResponseType commitType, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndCommitTransaction(handle, ct), r => r.Item1);
        Assert.Equal(KeyValueResponseType.Committed, commitType);

        return handle.TransactionId;
    }

    /// <summary>
    /// The node's fingerprint for the partition, or null when the consistent read kept racing applies (the
    /// node answers MustRetry): a "not yet" for the polling callers, never a failure.
    /// </summary>
    private static async Task<KeyValueApplyFingerprint?> FingerprintOf(KahunaManager node, CancellationToken ct)
    {
        (KeyValueResponseType type, KeyValueApplyFingerprint fingerprint) = await node.GetPartitionApplyFingerprint(Partition, ct);
        Assert.NotEqual(KeyValueResponseType.DoesNotExist, type);
        return type == KeyValueResponseType.Get ? fingerprint : null;
    }

    /// <summary>Waits until every replica reports the same fingerprint for the partition and at least <paramref name="minHeads"/> heads.</summary>
    private static async Task WaitForAgreement(KahunaManager[] managers, long minHeads, CancellationToken ct)
    {
        await WaitUntilAsync(async () =>
        {
            KeyValueApplyFingerprint? first = await FingerprintOf(managers[0], ct);
            if (first is null || first.Value.CommittedHeads < minHeads)
                return false;

            for (int i = 1; i < managers.Length; i++)
            {
                KeyValueApplyFingerprint? other = await FingerprintOf(managers[i], ct);
                if (other != first)
                    return false;
            }

            return true;
        }, timeoutMs: 30_000);
    }

    /// <summary>Waits until the partition's commit index stops moving on <paramref name="raft"/>.</summary>
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

    private static async Task<byte[]> ExportPartitionState(KahunaManager leader, long upToIndex, CancellationToken ct)
    {
        await using Stream stream = await leader.KeyValues.PartitionStateTransfer.ExportPartitionState(Partition, upToIndex, ct);
        using MemoryStream buffer = new();
        await stream.CopyToAsync(buffer, ct);
        return buffer.ToArray();
    }

    /// <summary>
    /// Makes replica <paramref name="short"/> drop every committed entry of the partition while the log grows by
    /// <paramref name="count"/> committed keys, then lifts the drop. Afterwards the replica reports the same applied
    /// kv log id as its peers with fewer committed heads: the apply-cursor-raised-without-delivery shape.
    /// </summary>
    private static async Task<string[]> DivergeReplica(KahunaManager[] managers, int leader, int @short, int count, CancellationToken ct)
    {
        string[] keys = KeysOnPartition(managers[0], Partition, "div", count);

        managers[@short].KeyValues.ReplicationDispatcher.ApplySkipForTesting = (partition, _) => partition == Partition;

        try
        {
            foreach (string key in keys)
                await CommitKey(managers[leader], key, "v", ct);

            // The short replica must sit at the peers' applied log id with fewer heads before the drop is lifted,
            // or the entries applied after the lift would close part of the gap.
            KahunaManager shortNode = managers[@short];
            KahunaManager leaderNode = managers[leader];
            await WaitUntilAsync(async () =>
            {
                KeyValueApplyFingerprint? a = await FingerprintOf(leaderNode, ct);
                KeyValueApplyFingerprint? b = await FingerprintOf(shortNode, ct);
                return a is not null && b is not null
                    && a.Value.AppliedLogId == b.Value.AppliedLogId && b.Value.CommittedHeads < a.Value.CommittedHeads
                    && a.Value.LiveIntents == 0 && b.Value.LiveIntents == 0;
            }, timeoutMs: 30_000);
        }
        finally
        {
            managers[@short].KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;
        }

        return keys;
    }

    private static async Task WaitUntilNotLeading(IRaft raft, CancellationToken ct) =>
        await WaitUntilAsync(async () => !await raft.AmILeaderIfHosted(Partition, ct), timeoutMs: 30_000);

    [Fact]
    public async Task ShortLeader_RelinquishesToTheFullerPeer_RefusesToServe_AndIsRepairedByAnInstall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        using MetricCapture metrics = new("action",
            "kahuna.keyvalues.apply_divergence_detected",
            "kahuna.keyvalues.apply_divergence_contained",
            "kahuna.keyvalues.apply_divergence_repaired");

        try
        {
            int leader = await LeaderIndexOf(Partition, rafts, ct);
            int @short = (leader + 1) % Nodes;

            string[] warm = KeysOnPartition(managers[0], Partition, "warm", 4);
            foreach (string key in warm)
                await CommitKey(managers[leader], key, "w", ct);
            await WaitForAgreement(managers, warm.Length, ct);

            string[] dropped = await DivergeReplica(managers, leader, @short, 4, ct);

            KeyValueApplyFingerprint? before = await FingerprintOf(managers[@short], ct);
            output.WriteLine($"short replica {EndpointOf(@short)} at applied {before?.AppliedLogId} holds {before?.CommittedHeads} heads");

            // Promote the short replica: the leader-change comparison indicts it as leader.
            double detectedBefore = metrics.Total("kahuna.keyvalues.apply_divergence_detected");
            RaftOperationStatus transfer = await rafts[leader].TransferLeadershipAsync(Partition, EndpointOf(@short), ct);
            Assert.True(transfer is RaftOperationStatus.Success or RaftOperationStatus.Pending, $"transfer to the short replica: {transfer}");

            // (a) It relinquishes within a bounded time, to a peer, and stays gated.
            await WaitUntilAsync(() => managers[@short].KeyValues.DivergenceContainment.IsGated(Partition), timeoutMs: 40_000);
            await WaitUntilNotLeading(rafts[@short], ct);
            int successor = await LeaderIndexOf(Partition, rafts, ct);
            Assert.NotEqual(@short, successor);

            await WaitUntilAsync(() => metrics.Total("kahuna.keyvalues.apply_divergence_detected") > detectedBefore, timeoutMs: 15_000);
            Assert.True(metrics.Total("kahuna.keyvalues.apply_divergence_contained", "gated") >= 1);
            await WaitUntilAsync(() =>
                metrics.Total("kahuna.keyvalues.apply_divergence_contained", "transferred")
                + metrics.Total("kahuna.keyvalues.apply_divergence_contained", "stepped_down") >= 1, timeoutMs: 15_000);

            // (d) No read is served from the short projection: the non-locating local reads (the inter-node batch
            // path lands there without passing the locator) refuse the keys, and through the gated node's locating
            // path the dropped keys are answered by the successor with the value it holds.
            foreach (string key in dropped)
            {
                (KeyValueResponseType localType, ReadOnlyKeyValueEntry? local) = await managers[@short].TryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.MustRetry, localType);
                Assert.Null(local);

                List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> batch = await managers[@short].TryGetManyValues(
                    HLCTimestamp.Zero, HLCTimestamp.Zero, [(key, -1L, KeyValueDurability.Persistent)]);
                (KeyValueResponseType batchType, _, _, _) = Assert.Single(batch);
                Assert.Equal(KeyValueResponseType.MustRetry, batchType);

                (KeyValueResponseType routedType, ReadOnlyKeyValueEntry? routed) = await RetryOnMustRetryAsync(
                    () => managers[@short].LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                    r => r.Item1);
                Assert.Equal(KeyValueResponseType.Get, routedType);
                Assert.Equal("v", Encoding.UTF8.GetString(routed!.Value!));
            }

            // While gated the replica withholds its candidacy, so it cannot be handed the partition or win a term
            // from the incomplete projection; a transfer aimed at it is refused on its side.
            Assert.True(rafts[@short].IsCandidacyWithheld(Partition), "the gated replica did not withhold its candidacy");

            // (b) It asks the leader to re-seed it and the install lifts the gate: Kommander holds its applies,
            // the leader takes a fresh checkpoint, the forced snapshot installs over the log it already holds.
            double repairedBefore = metrics.Total("kahuna.keyvalues.apply_divergence_repaired");
            await WaitUntilAsync(() => !managers[@short].KeyValues.DivergenceContainment.IsGated(Partition), timeoutMs: 60_000);
            Assert.True(metrics.Total("kahuna.keyvalues.apply_divergence_repaired") > repairedBefore);
            Assert.True(metrics.Total("kahuna.keyvalues.apply_divergence_contained", "reseed_requested") >= 1);
            Assert.False(rafts[@short].IsCandidacyWithheld(Partition), "the repaired replica did not release its candidacy");

            // (c) ... and the re-seeded replica's fingerprint equals its peers'. The install marks the replica
            // applied through the checkpoint entry, which peers never see delivered, so the applied ids line up
            // at the next committed entry.
            successor = await LeaderIndexOf(Partition, rafts, ct);
            string[] after = KeysOnPartition(managers[0], Partition, "after", 1);
            await CommitKey(managers[successor], after[0], "a", ct);
            await WaitForAgreement(managers, warm.Length + dropped.Length + after.Length, ct);

            // The repaired replica can lead again and serve the keys it once dropped.
            transfer = await rafts[successor].TransferLeadershipAsync(Partition, EndpointOf(@short), ct);
            Assert.True(transfer is RaftOperationStatus.Success or RaftOperationStatus.Pending, $"transfer to the repaired replica: {transfer}");
            await WaitUntilAsync(async () => await rafts[@short].AmILeaderIfHosted(Partition, ct), timeoutMs: 30_000);

            await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
            Assert.False(managers[@short].KeyValues.DivergenceContainment.IsGated(Partition));
            Assert.True(await rafts[@short].AmILeaderIfHosted(Partition, ct), "the repaired replica did not keep the leadership it was handed");

            foreach (string key in dropped)
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
                    () => managers[@short].LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                    r => r.Item1);
                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.Equal("v", Encoding.UTF8.GetString(entry!.Value!));
            }
        }
        finally
        {
            foreach (KahunaManager manager in managers)
                manager.KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    [Fact]
    public async Task ShortFollower_GatesItselfAtTheLeaderChange_AndRelinquishesIfElected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        using MetricCapture metrics = new("action",
            "kahuna.keyvalues.apply_divergence_detected",
            "kahuna.keyvalues.apply_divergence_contained");

        try
        {
            int leader = await LeaderIndexOf(Partition, rafts, ct);
            int @short = (leader + 1) % Nodes;
            int other = (leader + 2) % Nodes;

            string[] warm = KeysOnPartition(managers[0], Partition, "warm", 4);
            foreach (string key in warm)
                await CommitKey(managers[leader], key, "w", ct);
            await WaitForAgreement(managers, warm.Length, ct);

            await DivergeReplica(managers, leader, @short, 4, ct);

            // A leader change with the short replica still a follower: the leader reports it, and the replica's own
            // comparison gates it. Handing leadership to the third node keeps the short replica a follower for the
            // change itself.
            double detectedBefore = metrics.Total("kahuna.keyvalues.apply_divergence_detected");
            RaftOperationStatus transfer = await rafts[leader].TransferLeadershipAsync(Partition, EndpointOf(other), ct);
            Assert.True(transfer is RaftOperationStatus.Success or RaftOperationStatus.Pending, $"transfer to the complete peer: {transfer}");

            await WaitUntilAsync(() => managers[@short].KeyValues.DivergenceContainment.IsGated(Partition), timeoutMs: 40_000);
            await WaitUntilAsync(() => metrics.Total("kahuna.keyvalues.apply_divergence_detected") > detectedBefore, timeoutMs: 15_000);
            Assert.False(managers[leader].KeyValues.DivergenceContainment.IsGated(Partition));
            Assert.False(managers[other].KeyValues.DivergenceContainment.IsGated(Partition));

            // Not electable meanwhile: its candidacy is withheld and it does not lead.
            Assert.True(rafts[@short].IsCandidacyWithheld(Partition) || !managers[@short].KeyValues.DivergenceContainment.IsGated(Partition));
            Assert.False(await rafts[@short].AmILeaderIfHosted(Partition, ct));

            // Re-seeded: the gate lifts on the install, the candidacy is released, and the fingerprint agrees
            // at the next committed entry (the install marks the replica applied through the checkpoint entry).
            await WaitUntilAsync(() => !managers[@short].KeyValues.DivergenceContainment.IsGated(Partition), timeoutMs: 60_000);
            Assert.False(rafts[@short].IsCandidacyWithheld(Partition));
            int repairedLeader = await LeaderIndexOf(Partition, rafts, ct);
            string[] after = KeysOnPartition(managers[0], Partition, "after", 1);
            await CommitKey(managers[repairedLeader], after[0], "a", ct);
            await WaitForAgreement(managers, warm.Length + 4 + after.Length, ct);

            // Electable again: a transfer to it lands and it keeps the partition.
            int current = await LeaderIndexOf(Partition, rafts, ct);
            transfer = await rafts[current].TransferLeadershipAsync(Partition, EndpointOf(@short), ct);
            Assert.True(transfer is RaftOperationStatus.Success or RaftOperationStatus.Pending, $"transfer to the repaired replica: {transfer}");
            await WaitUntilAsync(async () => await rafts[@short].AmILeaderIfHosted(Partition, ct), timeoutMs: 30_000);
            await Task.Delay(500, ct);
            Assert.True(await rafts[@short].AmILeaderIfHosted(Partition, ct), "the repaired replica did not keep the leadership it was handed");
        }
        finally
        {
            foreach (KahunaManager manager in managers)
                manager.KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A replica that applied the prepares of committed transactions but dropped their settlements holds their
    /// intents forever once the records age out: record absence cannot be told from a reclaimed commit. Asking the
    /// partition's other replicas resolves it — a majority that settled the intent at or past this node's applied id
    /// proves the settlement is in the log and this replica missed it — and the answer routes to containment
    /// instead of a permanent read-only key.
    /// </summary>
    [Fact]
    public async Task StaleRecordlessHold_IsProvenByThePeers_AndContained()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureKahuna: config =>
            {
                config.TransactionOutcomeRetentionTtl = TimeSpan.FromSeconds(2);
                config.CompletionReceiptRetentionTtl = TimeSpan.FromSeconds(2);
                config.DurableMaintenanceInterval = TimeSpan.FromSeconds(1);
                config.CollectionInterval = TimeSpan.FromSeconds(1);
                config.DurableDecisionDeadlineFloorMs = 1_000;
                config.DurableDecisionDeadlineCeilingMs = 1_000;
            });
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        using MetricCapture metrics = new("action",
            "kahuna.transactions.recordless_intents_stale_detected",
            "kahuna.keyvalues.apply_divergence_contained");

        try
        {
            int leader = await LeaderIndexOf(Partition, rafts, ct);
            int @short = (leader + 1) % Nodes;

            string[] warm = KeysOnPartition(managers[0], Partition, "warm", 2);
            foreach (string key in warm)
                await CommitKey(managers[leader], key, "w", ct);
            await WaitForAgreement(managers, warm.Length, ct);

            // Drop only the settlements (intent deltas without a prepare) and the completion receipts on the short
            // replica: the prepares apply, the commits never settle there, and no receipt can later vouch for them.
            string[] keys = KeysOnPartition(managers[0], Partition, "hold", 3);
            managers[@short].KeyValues.ReplicationDispatcher.ApplySkipForTesting = (partition, log) =>
            {
                if (partition != Partition)
                    return false;

                if (log.LogType == ReplicationTypes.CompletionReceipt)
                    return true;

                if (log.LogType != ReplicationTypes.PreparedIntent)
                    return false;

                foreach (PreparedIntentCommand command in PreparedIntentStore.DecodeDelta(log.LogData!))
                    if (command is PrepareIntentCommand)
                        return false;

                return true;
            };

            List<HLCTimestamp> transactions = [];
            foreach (string key in keys)
                transactions.Add(await CommitKey(managers[leader], key, "h", ct));

            KahunaManager shortNode = managers[@short];
            KahunaManager leaderNode = managers[leader];
            await WaitUntilAsync(async () =>
            {
                KeyValueApplyFingerprint? a = await FingerprintOf(leaderNode, ct);
                KeyValueApplyFingerprint? b = await FingerprintOf(shortNode, ct);
                return a is not null && b is not null
                    && a.Value.AppliedLogId == b.Value.AppliedLogId && a.Value.LiveIntents == 0 && b.Value.LiveIntents == keys.Length;
            }, timeoutMs: 30_000);

            managers[@short].KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;

            // The anchor leader reclaims the records once they age out, and the purge replicates: the short
            // replica's intents become record-less.
            await WaitUntilAsync(() =>
            {
                foreach (HLCTimestamp transactionId in transactions)
                    if (managers[@short].KeyValues.DurableTransactionRecordStore.Get(transactionId, 1) is not null)
                        return false;

                return true;
            }, timeoutMs: 60_000);

            // Promote the short replica with its fingerprint masked, so the leader-change comparison does not catch
            // it first: this test is about the recovery sweep's own cross-check, the second line of defence.
            int held = keys.Length;
            managers[@short].KeyValues.ApplyFingerprintOverrideForTesting = (partition, real) =>
                partition == Partition && real is not null
                    ? real.Value with { CommittedHeads = real.Value.CommittedHeads + held, LiveIntents = real.Value.LiveIntents - held }
                    : real;

            double staleBefore = metrics.Total("kahuna.transactions.recordless_intents_stale_detected");

            RaftOperationStatus transfer = await rafts[leader].TransferLeadershipAsync(Partition, EndpointOf(@short), ct);
            Assert.True(transfer is RaftOperationStatus.Success or RaftOperationStatus.Pending, $"transfer to the short replica: {transfer}");
            await WaitUntilAsync(async () => await rafts[@short].AmILeaderIfHosted(Partition, ct), timeoutMs: 30_000);

            // The sweep holds the record-less intents, asks the peers, and is told they settled them.
            await WaitUntilAsync(() => metrics.Total("kahuna.transactions.recordless_intents_stale_detected") > staleBefore, timeoutMs: 60_000);

            await WaitUntilAsync(() => managers[@short].KeyValues.DivergenceContainment.IsGated(Partition), timeoutMs: 15_000);
            await WaitUntilNotLeading(rafts[@short], ct);
            Assert.NotEqual(@short, await LeaderIndexOf(Partition, rafts, ct));
            Assert.True(metrics.Total("kahuna.keyvalues.apply_divergence_contained", "gated") >= 1);

            // Nothing was presumed locally: the re-seed replaces the projection, and the install carries the
            // peers' settled state, so the holds drop to zero once the gate lifts.
            await WaitUntilAsync(() => !managers[@short].KeyValues.DivergenceContainment.IsGated(Partition), timeoutMs: 60_000);
            await WaitUntilAsync(() =>
            {
                foreach (string key in keys)
                    if (managers[@short].KeyValues.DurablePreparedIntentStore.Get(key) is not null)
                        return false;

                return true;
            }, timeoutMs: 15_000);
            Assert.False(rafts[@short].IsCandidacyWithheld(Partition));

            // The keys are readable through the repaired node with the values the peers settled.
            foreach (string key in keys)
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
                    () => managers[@short].LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                    r => r.Item1);
                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.Equal("h", Encoding.UTF8.GetString(entry!.Value!));
            }
        }
        finally
        {
            foreach (KahunaManager manager in managers)
            {
                manager.KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;
                manager.KeyValues.ApplyFingerprintOverrideForTesting = null;
            }

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
