using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Runs a representative operation subset against a 6-node cluster under both full replication
/// (replication factor 0) and per-partition placement (replication factor 3, rebalancer on):
/// KV set/get/CAS/delete, prefix and range scans, locks with fencing tokens, script transactions,
/// and interactive durable-2PC transactions whose coordinator, participants, and driver live on
/// different node sets. Operations are deliberately driven from nodes that do NOT host the touched
/// partition, so every case exercises the forwarding path placement depends on.
/// </summary>
public sealed class TestReplicatedPlacementOperations : BaseCluster
{
    private const int Nodes = 6;
    private const int Partitions = 8;
    private const int PlacedRf = 3;

    private static Task<(IRaft[] Rafts, IKahuna[] Kahunas)> AssembleAsync(int replicationFactor) =>
        AssembleCluster(
            Nodes, "memory", Partitions,
            NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance,
            replicationFactor,
            enablePlacementRebalancer: replicationFactor > 0);

    private static async Task TearDownAsync(IRaft[] rafts)
    {
        foreach (IRaft raft in rafts)
            await raft.LeaveCluster(true);
    }

    private static int PartitionOf(IRaft raft, string key) => new DataPartitionRouter(raft).Locate(key);

    /// <summary>
    /// Picks a node that does not host <paramref name="key"/>'s partition — the driver that forces
    /// the operation through forwarding. Under full replication every node hosts everything, so an
    /// arbitrary node is returned and the operation runs the legacy local path.
    /// </summary>
    private static IKahuna DriverFor(IRaft[] rafts, IKahuna[] kahunas, string key)
    {
        int partitionId = PartitionOf(rafts[0], key);
        for (int i = 0; i < rafts.Length; i++)
            if (!rafts[i].HostsPartition(partitionId))
                return kahunas[i];
        return kahunas[0];
    }

    /// <summary>
    /// Retries an operation while it answers a transient routing/replication outcome. A freshly
    /// assembled placed cluster forwards to leaders that may still be warming up, so MustRetry and
    /// WaitingForReplication are expected on first contact and guaranteed effect-free.
    /// </summary>
    private static async Task<T> RetryTransient<T>(Func<Task<T>> operation, Func<T, object> classify)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        T result = await operation();

        for (int attempt = 0; attempt < 100; attempt++)
        {
            object outcome = classify(result);
            bool transient = outcome switch
            {
                KeyValueResponseType kv => kv is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication,
                LockResponseType l => l is LockResponseType.MustRetry or LockResponseType.WaitingForReplication,
                _ => false
            };

            if (!transient)
                return result;

            await Task.Delay(50, ct);
            result = await operation();
        }

        return result;
    }

    // ── fixture invariants ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Rf3Fixture_EveryPartitionOnExactlyThreeDistinctNodes_AndRestrictedHosting()
    {
        (IRaft[] rafts, _) = await AssembleAsync(PlacedRf);

        int hostedTotal = 0;

        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
        {
            // The committed replica set names exactly RF distinct endpoints.
            List<string> endpoints = [.. rafts[0].GetPartitionReplicas(partitionId).Select(r => r.Endpoint)];
            Assert.Equal(PlacedRf, endpoints.Count);
            Assert.Equal(PlacedRf, endpoints.Distinct(StringComparer.Ordinal).Count());

            // Local materialization agrees with the map: exactly RF nodes host the partition.
            int hosting = rafts.Count(raft => raft.HostsPartition(partitionId));
            Assert.Equal(PlacedRf, hosting);
            hostedTotal += hosting;
        }

        Assert.Equal(Partitions * PlacedRf, hostedTotal);

        // The fixture is only meaningful if placement is genuinely restricted: for any given key
        // some nodes must NOT host its partition (they serve it by forwarding).
        int probePartition = PartitionOf(rafts[0], "inv/probe");
        Assert.Contains(rafts, raft => !raft.HostsPartition(probePartition));

        await TearDownAsync(rafts);
    }

    // ── KV point operations ──────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(0)]
    [InlineData(PlacedRf)]
    public async Task KeyValue_SetGetCasDelete_DrivenFromNonHostingNodes(int replicationFactor)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleAsync(replicationFactor);
        CancellationToken ct = TestContext.Current.CancellationToken;

        const string key = "rfkv/point";
        byte[] v1 = "v1"u8.ToArray();
        byte[] v2 = "v2"u8.ToArray();

        IKahuna writer = DriverFor(rafts, kahunas, key);

        (KeyValueResponseType setType, long setRevision, _) = await RetryTransient(
            () => writer.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, v1, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Set, setType);
        Assert.Equal(0, setRevision);

        // Read from a different non-hosting node than the writer when one exists.
        IKahuna reader = kahunas.First(k => !ReferenceEquals(k, writer));
        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await RetryTransient(
            () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Get, getType);
        Assert.Equal(v1, entry!.Value);

        // CAS with the matching expected value succeeds and advances the revision.
        (KeyValueResponseType casType, long casRevision, _) = await RetryTransient(
            () => writer.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, v2, v1, -1, KeyValueFlags.SetIfEqualToValue, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Set, casType);
        Assert.Equal(1, casRevision);

        // CAS with a stale expected value refuses and leaves the value untouched.
        (KeyValueResponseType staleType, _, _) = await RetryTransient(
            () => writer.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "v3"u8.ToArray(), v1, -1, KeyValueFlags.SetIfEqualToValue, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.NotSet, staleType);

        (getType, entry) = await RetryTransient(
            () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Get, getType);
        Assert.Equal(v2, entry!.Value);

        (KeyValueResponseType deleteType, _, _) = await RetryTransient(
            () => writer.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Deleted, deleteType);

        (getType, _) = await RetryTransient(
            () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.DoesNotExist, getType);

        await TearDownAsync(rafts);
    }

    // ── scans ────────────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(0)]
    [InlineData(PlacedRf)]
    public async Task Scans_PrefixAndRange_SeeEveryCommittedRow(int replicationFactor)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleAsync(replicationFactor);
        CancellationToken ct = TestContext.Current.CancellationToken;

        const string space = "rfscan";
        const int rows = 12;

        for (int i = 0; i < rows; i++)
        {
            string key = $"{space}/k{i:D2}";
            IKahuna writer = DriverFor(rafts, kahunas, key);
            (KeyValueResponseType setType, _, _) = await RetryTransient(
                () => writer.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes($"v{i}"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, setType);
        }

        // Prefix scan fans out; every node must see all rows regardless of what it hosts.
        foreach (IKahuna kahuna in kahunas)
        {
            KeyValueGetByBucketResult prefixResult = await RetryTransient(
                () => kahuna.ScanAllByPrefix($"{space}/", HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Type);
            Assert.Equal(rows, prefixResult.Items.Count);
        }

        // Range scan over the key space, driven from a node that does not host it.
        IKahuna rangeDriver = DriverFor(rafts, kahunas, $"{space}/k00");
        KeyValueGetByRangeResult rangeResult = await RetryTransient(
            () => rangeDriver.LocateAndGetByRange(HLCTimestamp.Zero, space, null, true, null, true, rows * 2, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Type);
        Assert.Equal(rows, rangeResult.Items.Count);

        await TearDownAsync(rafts);
    }

    // ── locks + fencing tokens ───────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(0)]
    [InlineData(PlacedRf)]
    public async Task Locks_MutualExclusionAndMonotonicFencingTokens(int replicationFactor)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleAsync(replicationFactor);
        CancellationToken ct = TestContext.Current.CancellationToken;

        const string resource = "rflock/r1";
        byte[] ownerA = Guid.NewGuid().ToByteArray();
        byte[] ownerB = Guid.NewGuid().ToByteArray();

        IKahuna nodeA = DriverFor(rafts, kahunas, resource);
        IKahuna nodeB = kahunas.First(k => !ReferenceEquals(k, nodeA));

        (LockResponseType lockType, long fencingToken1) = await RetryTransient(
            () => nodeA.LocateAndTryLock(resource, ownerA, 30_000, LockDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(LockResponseType.Locked, lockType);

        // A second owner is excluded while the lease is held — from any node.
        (LockResponseType busyType, _) = await RetryTransient(
            () => nodeB.LocateAndTryLock(resource, ownerB, 30_000, LockDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(LockResponseType.Busy, busyType);

        (LockResponseType extendType, _) = await RetryTransient(
            () => nodeB.LocateAndTryExtendLock(resource, ownerA, 30_000, LockDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(LockResponseType.Extended, extendType);

        LockResponseType unlockType = await RetryTransient(
            () => nodeA.LocateAndTryUnlock(resource, ownerA, LockDurability.Persistent, ct),
            r => r);
        Assert.Equal(LockResponseType.Unlocked, unlockType);

        // Re-acquisition mints a strictly greater fencing token — the split-brain guard.
        (LockResponseType relockType, long fencingToken2) = await RetryTransient(
            () => nodeB.LocateAndTryLock(resource, ownerB, 30_000, LockDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(LockResponseType.Locked, relockType);
        Assert.True(fencingToken2 > fencingToken1,
            $"fencing token must be monotonic: relock produced {fencingToken2} after {fencingToken1}");

        await TearDownAsync(rafts);
    }

    // ── script transactions (durable 2PC) ────────────────────────────────────────────────────

    [Theory]
    [InlineData(0)]
    [InlineData(PlacedRf)]
    public async Task ScriptTransaction_MultiPartitionCommit_IsReadableEverywhere(int replicationFactor)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleAsync(replicationFactor);
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Two key spaces that land on different partitions, so the script's 2PC spans partitions.
        (string keyA, string keyB) = PickKeysOnDifferentPartitions(rafts[0], "rfsxa", "rfsxb");

        IKahuna driver = DriverFor(rafts, kahunas, keyA);
        KeyValueTransactionResult result = await RetryOnMustRetry(
            driver,
            Encoding.UTF8.GetBytes($"BEGIN SET `{keyA}` 'a1' SET `{keyB}` 'b1' COMMIT END"),
            null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        foreach (string key in (string[])[keyA, keyB])
        {
            IKahuna reader = DriverFor(rafts, kahunas, key);
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await RetryTransient(
                () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.Equal(key == keyA ? "a1"u8.ToArray() : "b1"u8.ToArray(), entry!.Value);
        }

        await TearDownAsync(rafts);
    }

    // ── interactive transactions (durable decision, disjoint replica sets) ───────────────────

    [Theory]
    [InlineData(0)]
    [InlineData(PlacedRf)]
    public async Task InteractiveTransaction_ParticipantsOnDisjointReplicaSets_CommitsAtomically(int replicationFactor)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleAsync(replicationFactor);
        CancellationToken ct = TestContext.Current.CancellationToken;

        // The adversarial shape: two participant partitions whose replica sets share no node
        // (only possible under placement; under full replication settle for distinct partitions),
        // a coordinator key on a third partition, and a driver that hosts neither participant.
        (string keyA, string keyB) = PickParticipantKeys(rafts[0], replicationFactor);
        IKahuna driver = DriverFor(rafts, kahunas, keyA);

        byte[] valueA = "ia"u8.ToArray();
        byte[] valueB = "ib"u8.ToArray();

        KeyValueResponseType commitType = KeyValueResponseType.MustRetry;
        for (int attempt = 0; attempt < 10 && commitType != KeyValueResponseType.Committed; attempt++)
        {
            (KeyValueResponseType startType, TransactionHandle handle) = await driver.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey = $"rfitx/coord-{attempt}",
                    Locking = KeyValueTransactionLocking.Pessimistic,
                    DecisionDurability = DecisionDurability.Durable,
                    Timeout = 10_000
                }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);

            (KeyValueResponseType writeA, _, _) = await RetryTransient(
                () => driver.LocateAndTrySetKeyValue(
                    handle.TransactionId, keyA, valueA, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
                r => r.Item1);
            (KeyValueResponseType writeB, _, _) = await RetryTransient(
                () => driver.LocateAndTrySetKeyValue(
                    handle.TransactionId, keyB, valueB, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
                r => r.Item1);

            if (writeA != KeyValueResponseType.Set || writeB != KeyValueResponseType.Set)
            {
                await driver.LocateAndRollbackTransaction(handle, ct);
                await Task.Delay(100, ct);
                continue;
            }

            (commitType, _) = await driver.LocateAndCommitTransaction(handle, ct);
            if (commitType != KeyValueResponseType.Committed)
                await Task.Delay(100, ct);
        }

        Assert.Equal(KeyValueResponseType.Committed, commitType);

        // Both writes are visible from non-hosting readers — the transaction was not torn.
        foreach ((string key, byte[] expected) in ((string, byte[])[])[(keyA, valueA), (keyB, valueB)])
        {
            IKahuna reader = DriverFor(rafts, kahunas, key);
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await RetryTransient(
                () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.Equal(expected, entry!.Value);
        }

        await TearDownAsync(rafts);
    }

    /// <summary>
    /// Finds two keys in differently-named key spaces that hash to different partitions.
    /// </summary>
    private static (string KeyA, string KeyB) PickKeysOnDifferentPartitions(IRaft raft, string spaceA, string spaceB)
    {
        string keyA = $"{spaceA}/1";
        int partitionA = PartitionOf(raft, keyA);

        for (int i = 0; i < 64; i++)
        {
            string keyB = $"{spaceB}{i}/1";
            if (PartitionOf(raft, keyB) != partitionA)
                return (keyA, keyB);
        }

        throw new InvalidOperationException("No key space hashing to a second partition was found.");
    }

    /// <summary>
    /// Picks two participant keys for the cross-partition transaction. Under placement it insists
    /// on partitions whose replica sets are fully disjoint — the coordinator, each participant,
    /// and the driver then live on different node sets, which is the case most likely to break.
    /// Under full replication (empty replica sets) distinct partitions are the strongest available
    /// separation.
    /// </summary>
    private static (string KeyA, string KeyB) PickParticipantKeys(IRaft raft, int replicationFactor)
    {
        for (int a = 0; a < 32; a++)
        {
            string keyA = $"rfpa{a}/1";
            int partitionA = PartitionOf(raft, keyA);
            HashSet<string> replicasA = [.. raft.GetPartitionReplicas(partitionA).Select(r => r.Endpoint)];

            for (int b = 0; b < 32; b++)
            {
                string keyB = $"rfpb{b}/1";
                int partitionB = PartitionOf(raft, keyB);
                if (partitionB == partitionA)
                    continue;

                if (replicationFactor == 0)
                    return (keyA, keyB);

                HashSet<string> replicasB = [.. raft.GetPartitionReplicas(partitionB).Select(r => r.Endpoint)];
                if (!replicasA.Overlaps(replicasB))
                    return (keyA, keyB);
            }
        }

        // 8 partitions × RF3 over 6 nodes leaves plenty of disjoint pairs; failing to find one
        // means the fixture is not what this suite assumes, which is itself worth failing on.
        throw new InvalidOperationException("No pair of partitions with disjoint replica sets was found.");
    }
}
