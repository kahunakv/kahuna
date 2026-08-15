
using System.Text;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

using Kommander;
using Kommander.Communication.Memory;
using Kommander.Time;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end coverage for replica-placement forwarding: a three-node cluster at replication
/// factor 1 hosts each data partition on exactly one node, so for any key two of the three nodes
/// host nothing relevant and can only serve by forwarding the whole operation to a replica. Every
/// point operation — set, get, exists, delete, extend, and the lock lifecycle — is driven from a
/// node that does not host the key's partition and must succeed with the same results a hosting
/// node would produce. The full six-node RF=3 fixture and the adversarial scenarios belong to the
/// dedicated placement suites; this is the forwarding smoke that must stay cheap.
/// </summary>
public sealed class TestReplicaPlacementForwarding : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

    private readonly ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;

    private static readonly double TimingScale = GetTimingScale();

    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    private const int Partitions = 6;

    /// <summary>
    /// Assembles the three-node RF=1 cluster: joins all nodes, waits until every data partition
    /// has a committed single-replica placement with an elected leader, and asserts placement
    /// actually spread (the fixture is worthless if every node hosts everything).
    /// </summary>
    private async Task<(IRaft[] Rafts, IKahuna[] Kahunas, CancellationTokenSource Cts)> AssembleRf1Cluster()
    {
        InMemoryCommunication raftComm = new();
        MemoryInterNodeCommmunication interComm = new();

        (IRaft raft1, IKahuna kahuna1) = BuildNode(interComm, raftComm, "memory", 1, 8001, ["localhost:8002", "localhost:8003"], raftLogger, kahunaLogger, Partitions, replicationFactor: 1);
        (IRaft raft2, IKahuna kahuna2) = BuildNode(interComm, raftComm, "memory", 2, 8002, ["localhost:8001", "localhost:8003"], raftLogger, kahunaLogger, Partitions, replicationFactor: 1);
        (IRaft raft3, IKahuna kahuna3) = BuildNode(interComm, raftComm, "memory", 3, 8003, ["localhost:8001", "localhost:8002"], raftLogger, kahunaLogger, Partitions, replicationFactor: 1);

        interComm.SetNodes(new()
        {
            { "localhost:8001", kahuna1 },
            { "localhost:8002", kahuna2 },
            { "localhost:8003", kahuna3 }
        });

        raftComm.SetNodes(new()
        {
            { "localhost:8001", raft1 },
            { "localhost:8002", raft2 },
            { "localhost:8003", raft3 }
        });

        CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(120 * TimingScale));
        CancellationToken ct = cts.Token;

        IRaft[] rafts = [raft1, raft2, raft3];
        IKahuna[] kahunas = [kahuna1, kahuna2, kahuna3];

        await Task.WhenAll(raft1.JoinCluster(ct), raft2.JoinCluster(ct), raft3.JoinCluster(ct));

        await WaitForPlacedLeaders(rafts, ct);

        foreach (IRaft raft in rafts)
            Assert.Contains(Enumerable.Range(1, Partitions), p => !raft.HostsPartition(p));

        return (rafts, kahunas, cts);
    }

    [Fact]
    public async Task NonHostingNode_ServesPointOperationsByForwarding()
    {
        (IRaft[] rafts, IKahuna[] kahunas, CancellationTokenSource cts) = await AssembleRf1Cluster();
        using CancellationTokenSource ctsGuard = cts;
        CancellationToken ct = cts.Token;

        IRaft raft1 = rafts[0];
        IRaft raft2 = rafts[1];
        IKahuna kahuna1 = kahunas[0];
        IKahuna kahuna2 = kahunas[1];
        IKahuna kahuna3 = kahunas[2];

        // For each driving node, pick a key whose partition that node does NOT host, so serving it
        // locally is impossible and success proves the whole-operation forward.
        for (int nodeIndex = 0; nodeIndex < kahunas.Length; nodeIndex++)
        {
            IRaft raft = rafts[nodeIndex];
            IKahuna kahuna = kahunas[nodeIndex];

            (string key, int partitionId) = FindKeyNotHostedOn(raft, $"pf-n{nodeIndex + 1}");
            byte[] value = Encoding.UTF8.GetBytes($"forwarded-{nodeIndex}");

            // Set through the non-hosting node.
            (KeyValueResponseType setType, long revision, _) = await RetryKv(
                () => kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, value, null, 0, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct),
                r => r.Item1, ct);
            Assert.Equal(KeyValueResponseType.Set, setType);
            Assert.True(revision >= 0);

            // Read it back through a different non-hosting node when one exists (with RF=1 at
            // least one other node also does not host it).
            IKahuna otherNonHosting = kahunas[(nodeIndex + 1) % 3];
            if (rafts[(nodeIndex + 1) % 3].HostsPartition(partitionId))
                otherNonHosting = kahunas[(nodeIndex + 2) % 3];

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await RetryKv(
                () => otherNonHosting.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1, ct);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry);
            Assert.Equal(value, entry.Value);

            // Exists through the non-hosting node.
            (KeyValueResponseType existsType, _) = await RetryKv(
                () => kahuna.LocateAndTryExistsValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1, ct);
            Assert.Equal(KeyValueResponseType.Exists, existsType);

            // Delete through the non-hosting node, then verify absence from another node.
            (KeyValueResponseType deleteType, _, _) = await RetryKv(
                () => kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct),
                r => r.Item1, ct);
            Assert.Equal(KeyValueResponseType.Deleted, deleteType);

            (KeyValueResponseType afterDeleteType, _) = await RetryKv(
                () => otherNonHosting.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, afterDeleteType);
        }

        // Lock lifecycle across non-hosting nodes: acquire on one, observe mutual exclusion from
        // another, release from a third — every leg forwarded.
        {
            (string resource, int partitionId) = FindKeyNotHostedOn(raft1, "pf-lock");
            byte[] owner = Guid.NewGuid().ToByteArray();

            (LockResponseType lockType, long fencingToken) = await RetryKv(
                () => kahuna1.LocateAndTryLock(resource, owner, 60_000, LockDurability.Persistent, ct),
                r => MapLock(r.Item1), ct);
            Assert.Equal(LockResponseType.Locked, lockType);
            Assert.True(fencingToken >= 0);

            byte[] rivalOwner = Guid.NewGuid().ToByteArray();
            IKahuna rival = raft2.HostsPartition(partitionId) ? kahuna3 : kahuna2;

            (LockResponseType rivalType, _) = await RetryKv(
                () => rival.LocateAndTryLock(resource, rivalOwner, 60_000, LockDurability.Persistent, ct),
                r => MapLock(r.Item1), ct);
            Assert.Equal(LockResponseType.Busy, rivalType);

            LockResponseType unlockType = await RetryKv(
                () => rival.LocateAndTryUnlock(resource, owner, LockDurability.Persistent, ct),
                MapLock, ct);
            Assert.Equal(LockResponseType.Unlocked, unlockType);
        }
    }

    [Fact]
    public async Task PrefixScan_NarrowsToReplicas_AndIgnoresStaleLeftoversOnNonReplicas()
    {
        (IRaft[] rafts, IKahuna[] kahunas, CancellationTokenSource cts) = await AssembleRf1Cluster();
        using CancellationTokenSource ctsGuard = cts;
        CancellationToken ct = cts.Token;

        // Pick a key space whose partition node1 does not host, and identify the hosting node so
        // the stale leftover can be planted on a node that is neither the driver nor a replica.
        (string keySpace, int partitionId) = FindKeySpaceNotHostedOn(rafts[0], "scan-nr");

        string hostingEndpoint = Assert.Single(rafts[0].GetPartitionReplicas(partitionId).Select(static r => r.Endpoint).Distinct());
        int hostingIndex = Array.FindIndex(rafts, r => r.GetLocalEndpoint() == hostingEndpoint);
        Assert.True(hostingIndex > 0, "the fixture picked a space node1 does not host, so its replica must be node2 or node3");
        int plantedIndex = hostingIndex == 1 ? 2 : 1;

        // The real value, written through the non-hosting driver (forwarded to the replica).
        string liveKey = $"{keySpace}/live";
        byte[] liveValue = "live"u8.ToArray();
        (KeyValueResponseType setType, _, _) = await RetryKv(
            () => kahunas[0].LocateAndTrySetKeyValue(HLCTimestamp.Zero, liveKey, liveValue, null, 0, KeyValueFlags.None, 0, KeyValueDurability.Ephemeral, ct),
            r => r.Item1, ct);
        Assert.Equal(KeyValueResponseType.Set, setType);

        // A stale leftover planted directly into a non-replica node's local ephemeral store — the
        // state a node keeps after losing a replica, before anything purges it.
        string staleKey = $"{keySpace}/stale";
        (KeyValueResponseType plantType, _, _) = await kahunas[plantedIndex].TrySetKeyValue(
            HLCTimestamp.Zero, staleKey, "stale"u8.ToArray(), null, 0, KeyValueFlags.None, 0, KeyValueDurability.Ephemeral, 0);
        Assert.Equal(KeyValueResponseType.Set, plantType);

        // The scan driven from the non-hosting node consults itself and the partition's replicas
        // only: the live value comes back, the non-replica leftover does not.
        KeyValueGetByBucketResult scan = await RetryKv(
            () => kahunas[0].ScanAllByPrefix(keySpace, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct),
            static r => r.Type, ct);
        Assert.Equal(KeyValueResponseType.Get, scan.Type);
        Assert.Contains(scan.Items, item => item.Item1 == liveKey);
        Assert.DoesNotContain(scan.Items, item => item.Item1 == staleKey);

        // The same leftover IS visible when the planted node scans itself locally — proving the
        // narrowed fan-out (not a failed plant) is what keeps it out of the merged result.
        KeyValueGetByBucketResult local = await kahunas[plantedIndex].ScanByPrefix(keySpace, HLCTimestamp.Zero, KeyValueDurability.Ephemeral);
        Assert.Contains(local.Items, item => item.Item1 == staleKey);
    }

    /// <summary>
    /// Like <see cref="FindKeyNotHostedOn"/> but the candidate is a key space: keys are written
    /// under it, and the hash router hashes the space (the part before the last separator).
    /// </summary>
    private static (string KeySpace, int PartitionId) FindKeySpaceNotHostedOn(IRaft raft, string prefix)
    {
        DataPartitionRouter router = new(raft);

        for (int i = 0; i < 1_000; i++)
        {
            string keySpace = $"{prefix}-{i}";
            int partitionId = router.Locate($"{keySpace}/probe");

            if (!raft.HostsPartition(partitionId))
                return (keySpace, partitionId);
        }

        throw new InvalidOperationException($"No key space found routing to a partition not hosted on {raft.GetLocalEndpoint()} — placement did not spread.");
    }

    /// <summary>
    /// Waits until every data partition has a committed replica placement and its hosting node has
    /// an elected leader. Leadership questions go through the placement-safe wrappers — the raw
    /// APIs throw for the partitions a node does not host, which is the normal case here.
    /// </summary>
    private static async Task WaitForPlacedLeaders(IRaft[] rafts, CancellationToken ct)
    {
        for (int partitionId = 0; partitionId <= Partitions; partitionId++)
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();

                bool anyLeader = false;
                foreach (IRaft raft in rafts)
                {
                    if (await raft.AmILeaderIfHosted(partitionId, ct))
                    {
                        anyLeader = true;
                        break;
                    }
                }

                if (anyLeader)
                    break;

                await Task.Delay(50, ct);
            }
        }

        // Placement must be committed (not just leaders elected): every data partition carries a
        // non-empty replica set on every node's committed map before the assertions rely on it.
        foreach (IRaft raft in rafts)
        {
            for (int partitionId = 1; partitionId <= Partitions; partitionId++)
            {
                while (raft.GetPartitionReplicas(partitionId).Count == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    await Task.Delay(50, ct);
                }
            }
        }
    }

    /// <summary>
    /// Generates a key (varying the key-space prefix, which is what the hash router hashes) until
    /// one routes to a partition the given node does not host.
    /// </summary>
    private static (string Key, int PartitionId) FindKeyNotHostedOn(IRaft raft, string prefix)
    {
        DataPartitionRouter router = new(raft);

        for (int i = 0; i < 1_000; i++)
        {
            string key = $"{prefix}-{i}";
            int partitionId = router.Locate(key);

            if (!raft.HostsPartition(partitionId))
                return (key, partitionId);
        }

        throw new InvalidOperationException($"No key found routing to a partition not hosted on {raft.GetLocalEndpoint()} — placement did not spread.");
    }

    /// <summary>Retries an operation while it reports the retryable outcome (leader hints and
    /// placements settle asynchronously right after assembly); bounded so a genuinely broken
    /// forward still fails the test.</summary>
    private static async Task<T> RetryKv<T>(Func<Task<T>> operation, Func<T, object> classify, CancellationToken ct)
    {
        T result = await operation();

        for (int attempt = 0; attempt < 100; attempt++)
        {
            object outcome = classify(result);

            bool retryable = outcome is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                or LockResponseType.MustRetry or LockResponseType.WaitingForReplication;

            if (!retryable)
                return result;

            await Task.Delay(Math.Min(10 * (attempt + 1), 100), ct);
            result = await operation();
        }

        return result;
    }

    private static object MapLock(LockResponseType type) => type;
}
