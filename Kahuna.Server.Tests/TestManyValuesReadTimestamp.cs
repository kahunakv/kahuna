
using Kommander;
using Kommander.Time;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that LocateAndTryGetManyValues / LocateAndTryExistsManyValues honour a caller-supplied
/// readTimestamp, and that the batch result at a given snapshot is byte-identical to N separate
/// single-key LocateAndTryGetValue / LocateAndTryExistsValue calls at the same snapshot.
/// </summary>
[Collection("ClusterTests")]
public class TestManyValuesReadTimestamp : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestManyValuesReadTimestamp(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger   = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // ── GetMany snapshot ────────────────────────────────────────────────────────

    /// <summary>
    /// Batch get at snapshotT returns the version that existed at T, not the later version.
    /// Batch get at Zero returns the latest-committed version.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task GetManyValues_AtSnapshot_SeesPreUpdateValue(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key  = "batch/snap/get/" + Guid.NewGuid().ToString("N")[..10];
            byte[] valA = Encoding.UTF8.GetBytes("before");
            byte[] valB = Encoding.UTF8.GetBytes("after");

            // Write version A.
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Read A back to get its exact LastModified (= snapshotT).
            // HLC monotonicity guarantees valB's LastModified > snapshotT.
            (_, ReadOnlyKeyValueEntry? entryA) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(entryA);
            HLCTimestamp snapshotT = entryA.LastModified;

            // Write version B.
            (setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valB, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            List<(string key, long revision, KeyValueDurability durability)> keys =
                [(key, -1, KeyValueDurability.Persistent)];

            // Batch at snapshotT → valA.
            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> snapResults =
                await kahuna2.LocateAndTryGetManyValues(
                    HLCTimestamp.Zero, snapshotT, keys, TestContext.Current.CancellationToken);

            Assert.Single(snapResults);
            Assert.Equal(KeyValueResponseType.Get, snapResults[0].type);
            Assert.NotNull(snapResults[0].entry);
            Assert.Equal("before", Encoding.UTF8.GetString(snapResults[0].entry!.Value!));

            // Batch at Zero → latest → valB.
            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> latestResults =
                await kahuna2.LocateAndTryGetManyValues(
                    HLCTimestamp.Zero, HLCTimestamp.Zero, keys, TestContext.Current.CancellationToken);

            Assert.Single(latestResults);
            Assert.Equal(KeyValueResponseType.Get, latestResults[0].type);
            Assert.NotNull(latestResults[0].entry);
            Assert.Equal("after", Encoding.UTF8.GetString(latestResults[0].entry!.Value!));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Batch get at a snapshot taken before any write returns DoesNotExist;
    /// batch get at Zero returns the value written after the snapshot.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task GetManyValues_AtSnapshot_KeyNotYetWritten_DoesNotExist(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Capture snapshotT before any write. Task.Delay ensures the node HLC advances
            // past snapshotT before the write is stamped.
            HLCTimestamp snapshotT = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());
            await Task.Delay(10, TestContext.Current.CancellationToken);

            string key = "batch/snap/notyet/" + Guid.NewGuid().ToString("N")[..10];

            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("v"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            List<(string key, long revision, KeyValueDurability durability)> keys =
                [(key, -1, KeyValueDurability.Persistent)];

            // At snapshotT → key did not exist yet.
            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> snapResults =
                await kahuna2.LocateAndTryGetManyValues(
                    HLCTimestamp.Zero, snapshotT, keys, TestContext.Current.CancellationToken);

            Assert.Single(snapResults);
            Assert.Equal(KeyValueResponseType.DoesNotExist, snapResults[0].type);

            // At Zero → latest → found.
            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> latestResults =
                await kahuna2.LocateAndTryGetManyValues(
                    HLCTimestamp.Zero, HLCTimestamp.Zero, keys, TestContext.Current.CancellationToken);

            Assert.Single(latestResults);
            Assert.Equal(KeyValueResponseType.Get, latestResults[0].type);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A mixed batch — one key written before T, one after, one never — at snapshotT sees only
    /// the key that existed at T. At Zero it sees both written keys and not the missing one.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task GetManyValues_AtSnapshot_MixedPresenceKeys(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string prefix = "batch/snap/mixed/" + Guid.NewGuid().ToString("N")[..8];
            string keyBefore  = $"{prefix}/a";   // written before snapshotT
            string keyAfter   = $"{prefix}/b";   // written after  snapshotT
            string keyMissing = $"{prefix}/c";   // never written

            // Write keyBefore.
            await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, keyBefore, Encoding.UTF8.GetBytes("early"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            // Capture snapshotT from keyBefore's LastModified.
            (_, ReadOnlyKeyValueEntry? before) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, keyBefore, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(before);
            HLCTimestamp snapshotT = before.LastModified;

            // Write keyAfter — its LastModified > snapshotT (HLC monotonicity).
            await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, keyAfter, Encoding.UTF8.GetBytes("late"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            List<(string key, long revision, KeyValueDurability durability)> keys =
            [
                (keyBefore,  -1, KeyValueDurability.Persistent),
                (keyAfter,   -1, KeyValueDurability.Persistent),
                (keyMissing, -1, KeyValueDurability.Persistent)
            ];

            // At snapshotT: keyBefore=Get, keyAfter=DoesNotExist, keyMissing=DoesNotExist.
            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> snapResults =
                await kahuna2.LocateAndTryGetManyValues(
                    HLCTimestamp.Zero, snapshotT, keys, TestContext.Current.CancellationToken);

            Assert.Equal(3, snapResults.Count);
            (KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry) beforeSnap =
                snapResults.Single(r => r.k == keyBefore);
            Assert.Equal(KeyValueResponseType.Get, beforeSnap.type);
            Assert.Equal("early", Encoding.UTF8.GetString(beforeSnap.entry!.Value!));

            Assert.Equal(KeyValueResponseType.DoesNotExist, snapResults.Single(r => r.k == keyAfter).type);
            Assert.Equal(KeyValueResponseType.DoesNotExist, snapResults.Single(r => r.k == keyMissing).type);

            // At Zero: keyBefore=Get, keyAfter=Get, keyMissing=DoesNotExist.
            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> latestResults =
                await kahuna2.LocateAndTryGetManyValues(
                    HLCTimestamp.Zero, HLCTimestamp.Zero, keys, TestContext.Current.CancellationToken);

            Assert.Equal(3, latestResults.Count);
            Assert.Equal(KeyValueResponseType.Get,          latestResults.Single(r => r.k == keyBefore).type);
            Assert.Equal(KeyValueResponseType.Get,          latestResults.Single(r => r.k == keyAfter).type);
            Assert.Equal(KeyValueResponseType.DoesNotExist, latestResults.Single(r => r.k == keyMissing).type);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Parity: LocateAndTryGetManyValues at snapshotT returns the same (type, value, revision) as
    /// N individual LocateAndTryGetValue calls at the same snapshotT, across both local and
    /// remote-leader routing paths.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task GetManyValues_AtSnapshot_ParityWithSingleKeyReads(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Write several keys — using different Guid prefixes so they scatter across partitions.
            string prefix = "batch/snap/parity/" + Guid.NewGuid().ToString("N")[..6];
            string[] keyNames = Enumerable.Range(0, 6).Select(i => $"{prefix}/{i}").ToArray();
            byte[][] valuesA  = keyNames.Select((k, i) => Encoding.UTF8.GetBytes($"v1-{i}")).ToArray();
            byte[][] valuesB  = keyNames.Select((k, i) => Encoding.UTF8.GetBytes($"v2-{i}")).ToArray();

            for (int i = 0; i < keyNames.Length; i++)
                await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, keyNames[i], valuesA[i], null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            // Read one key back to get a reliable snapshotT.
            (_, ReadOnlyKeyValueEntry? anchor) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, keyNames[0], -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(anchor);
            HLCTimestamp snapshotT = anchor.LastModified;

            // Overwrite all keys so the latest versions are v2.
            for (int i = 0; i < keyNames.Length; i++)
                await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, keyNames[i], valuesB[i], null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            List<(string key, long revision, KeyValueDurability durability)> keys =
                keyNames.Select(k => (k, -1L, KeyValueDurability.Persistent)).ToList();

            // Batch read at snapshotT.
            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> batchResults =
                await kahuna2.LocateAndTryGetManyValues(
                    HLCTimestamp.Zero, snapshotT, keys, TestContext.Current.CancellationToken);

            Assert.Equal(keyNames.Length, batchResults.Count);

            // Parity: each single-key read at snapshotT must match the batch result.
            for (int i = 0; i < keyNames.Length; i++)
            {
                string k = keyNames[i];
                var batchItem = batchResults.Single(r => r.k == k);

                (KeyValueResponseType singleType, ReadOnlyKeyValueEntry? singleEntry) =
                    await kahuna2.LocateAndTryGetValue(
                        HLCTimestamp.Zero, k, -1, snapshotT,
                        KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

                Assert.Equal(singleType,  batchItem.type);
                Assert.Equal(singleEntry?.Revision, batchItem.entry?.Revision);
                if (singleEntry?.Value is not null)
                    Assert.Equal(singleEntry.Value, batchItem.entry!.Value);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    // ── ExistsMany snapshot ─────────────────────────────────────────────────────

    /// <summary>
    /// Batch existence check at snapshotT: a key written before T exists; a key written after T
    /// does not; at Zero both written keys exist.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task ExistsManyValues_AtSnapshot_ReflectsSnapshotState(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string prefix     = "batch/snap/exists/" + Guid.NewGuid().ToString("N")[..8];
            string keyBefore  = $"{prefix}/a";
            string keyAfter   = $"{prefix}/b";
            string keyMissing = $"{prefix}/c";

            await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, keyBefore, Encoding.UTF8.GetBytes("x"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            (_, ReadOnlyKeyValueEntry? anchor) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, keyBefore, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(anchor);
            HLCTimestamp snapshotT = anchor.LastModified;

            await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, keyAfter, Encoding.UTF8.GetBytes("y"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            List<(string key, long revision, KeyValueDurability durability)> keys =
            [
                (keyBefore,  -1, KeyValueDurability.Persistent),
                (keyAfter,   -1, KeyValueDurability.Persistent),
                (keyMissing, -1, KeyValueDurability.Persistent)
            ];

            // At snapshotT: keyBefore exists; keyAfter and keyMissing do not.
            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> snapResults =
                await kahuna2.LocateAndTryExistsManyValues(
                    HLCTimestamp.Zero, snapshotT, keys, TestContext.Current.CancellationToken);

            Assert.Equal(3, snapResults.Count);
            Assert.Equal(KeyValueResponseType.Exists,        snapResults.Single(r => r.k == keyBefore).type);
            Assert.Equal(KeyValueResponseType.DoesNotExist,  snapResults.Single(r => r.k == keyAfter).type);
            Assert.Equal(KeyValueResponseType.DoesNotExist,  snapResults.Single(r => r.k == keyMissing).type);

            // At Zero: keyBefore and keyAfter exist; keyMissing does not.
            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> latestResults =
                await kahuna2.LocateAndTryExistsManyValues(
                    HLCTimestamp.Zero, HLCTimestamp.Zero, keys, TestContext.Current.CancellationToken);

            Assert.Equal(3, latestResults.Count);
            Assert.Equal(KeyValueResponseType.Exists,        latestResults.Single(r => r.k == keyBefore).type);
            Assert.Equal(KeyValueResponseType.Exists,        latestResults.Single(r => r.k == keyAfter).type);
            Assert.Equal(KeyValueResponseType.DoesNotExist,  latestResults.Single(r => r.k == keyMissing).type);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Parity: LocateAndTryExistsManyValues at snapshotT returns the same type as N individual
    /// LocateAndTryExistsValue calls at the same snapshot, across both local and remote routing.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task ExistsManyValues_AtSnapshot_ParityWithSingleKeyReads(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string prefix = "batch/snap/exparity/" + Guid.NewGuid().ToString("N")[..6];
            string[] existingKeys = Enumerable.Range(0, 4).Select(i => $"{prefix}/e{i}").ToArray();
            string[] lateKeys     = Enumerable.Range(0, 3).Select(i => $"{prefix}/l{i}").ToArray();

            foreach (string k in existingKeys)
                await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, k, Encoding.UTF8.GetBytes("v"), null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            (_, ReadOnlyKeyValueEntry? anchor) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, existingKeys[0], -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(anchor);
            HLCTimestamp snapshotT = anchor.LastModified;

            foreach (string k in lateKeys)
                await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, k, Encoding.UTF8.GetBytes("w"), null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            List<(string key, long revision, KeyValueDurability durability)> keys =
                existingKeys.Concat(lateKeys)
                    .Select(k => (k, -1L, KeyValueDurability.Persistent))
                    .ToList();

            List<(KeyValueResponseType type, string k, KeyValueDurability dur, ReadOnlyKeyValueEntry? entry)> batchResults =
                await kahuna2.LocateAndTryExistsManyValues(
                    HLCTimestamp.Zero, snapshotT, keys, TestContext.Current.CancellationToken);

            Assert.Equal(keys.Count, batchResults.Count);

            foreach ((string k, long _, KeyValueDurability dur) in keys)
            {
                (KeyValueResponseType batchType, string _, KeyValueDurability _, ReadOnlyKeyValueEntry? _) =
                    batchResults.Single(r => r.k == k);

                (KeyValueResponseType singleType, _) = await kahuna2.LocateAndTryExistsValue(
                    HLCTimestamp.Zero, k, -1, snapshotT,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

                Assert.Equal(singleType, batchType);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
