
using System.Text;
using Kahuna;
using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// When the committed map stops listing this node as a replica of a partition, everything the node
/// retains for it must go: the backend rows, the durable stores' slices, the persisted durability
/// floor, any half-install marker, and the actor-resident entries and lock leases. The purge must
/// act only on committed absence, abort when the partition is re-gained, and be idempotent so a
/// crash mid-purge is repaired by re-deriving the intent from the committed map at startup.
/// </summary>
public sealed class TestUnhostedPartitionPurge : IDisposable
{
    private const int HashPoolSize = 3;

    private readonly string tempDir = Path.Combine(Path.GetTempPath(), "kahuna-purge-" + Guid.NewGuid().ToString("N"));

    private readonly ILoggerFactory loggerFactory;

    public TestUnhostedPartitionPurge(ITestOutputHelper outputHelper)
    {
        Directory.CreateDirectory(tempDir);
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    public void Dispose()
    {
        try { Directory.Delete(tempDir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static RangeMap BuildMap() => new([
        new RangeDescriptor { KeySpace = "ranged1", PartitionId = 2, Generation = 1 }
    ]);

    private sealed class Node
    {
        public required IPersistenceBackend Backend { get; init; }
        public required CompletionReceiptStore Receipts { get; init; }
        public required TransactionRecordStore Records { get; init; }
        public required PreparedIntentStore Intents { get; init; }
        public required PartitionStateTransfer Transfer { get; init; }
    }

    private Node MakeNode()
    {
        MemoryPersistenceBackend backend = new();
        RangeMap map = BuildMap();

        CompletionReceiptStore receipts = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();

        PartitionDataEnumerator enumerator = new(backend, () => map, HashPoolSize);

        PartitionStateTransfer transfer = new(
            enumerator, backend, receipts, records, intents,
            () => map, HashPoolSize,
            () => Task.CompletedTask,
            tempDir, "rev", NullLogger<IKahuna>.Instance);

        return new Node { Backend = backend, Receipts = receipts, Records = records, Intents = intents, Transfer = transfer };
    }

    private static PersistenceRequestItem KvItem(string key, long revision = 1) => new(
        key, [1, 2], revision,
        expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
        lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
        lastModifiedNode: 0, lastModifiedPhysical: revision, lastModifiedCounter: 0,
        state: (int)KeyValueState.Set);

    private static PersistenceRequestItem LockItem(string resource) => new(
        resource, [3], 1,
        expiresNode: 0, expiresPhysical: 1000, expiresCounter: 0,
        lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
        lastModifiedNode: 0, lastModifiedPhysical: 1, lastModifiedCounter: 0,
        state: (int)LockState.Locked);

    /// <summary>First generated hash key space owned by / not owned by partition 2.</summary>
    private static string HashSpace(bool ownedByPartition2)
    {
        for (int i = 0; ; i++)
        {
            string space = $"ps{i}";
            if ((PartitionDataEnumerator.HashPartitionOfKeySpace(space, HashPoolSize) == 2) == ownedByPartition2)
                return space;
        }
    }

    // ── durable purge (backend rows, store slices, floor, marker) ────────────────

    [Fact]
    public async Task Purge_RemovesEverythingThePartitionOwns_AndLeavesOtherPartitionsIntact()
    {
        Node node = MakeNode();
        string ownedHash = HashSpace(ownedByPartition2: true);
        string foreignHash = HashSpace(ownedByPartition2: false);
        int foreignPartition = PartitionDataEnumerator.HashPartitionOfKeySpace(foreignHash, HashPoolSize);

        // Partition 2 owns the "ranged1" space and the owned hash space (kv + locks); the foreign
        // space, its lock, its floor and its store entries must survive untouched.
        Assert.True(node.Backend.StoreKeyValues([
            KvItem("ranged1/a"), KvItem($"{ownedHash}/k"), KvItem($"{foreignHash}/k")]));
        Assert.True(node.Backend.StoreLocks([LockItem($"{ownedHash}/l"), LockItem($"{foreignHash}/l")]));
        Assert.True(node.Backend.StoreDurabilityFloors([(2, 42), (foreignPartition, 7)]));

        HLCTimestamp ownedTx = Ts(1000), foreignTx = Ts(2000);
        node.Receipts.Record(ownedTx, "ranged1/a", null, KeyValueDurability.Persistent);
        node.Receipts.Record(foreignTx, $"{foreignHash}/k", null, KeyValueDurability.Persistent);

        List<TransactionParticipantRef> manifest = [new("ranged1/anchor", KeyValueDurability.Persistent)];
        node.Records.Apply(new InitializeTransactionCommand(ownedTx, 1, "coord", "ranged1/anchor", Ts(1100), Ts(9000), 42, manifest, ownedTx, ownedTx));
        node.Records.Apply(new CommitTransactionCommand(ownedTx, 1, 42, ownedTx, Ts(1100)));
        List<TransactionParticipantRef> foreignManifest = [new($"{foreignHash}/anchor", KeyValueDurability.Persistent)];
        node.Records.Apply(new InitializeTransactionCommand(foreignTx, 1, "coord", $"{foreignHash}/anchor", Ts(2100), Ts(9000), 43, foreignManifest, foreignTx, foreignTx));

        node.Intents.Apply(new PrepareIntentCommand(new PreparedIntent(
            ownedTx, 1, "ranged1/pending", ManifestHash: 42, RecordAnchorKey: "ranged1/anchor", CommitTimestamp: Ts(1100),
            State: KeyValueState.Set, Value: [7], Bucket: null, Revision: 3, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: 2, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(6000),
            Resolution: PreparedIntentResolution.Pending)));

        // A half-install marker left by a crashed seeding attempt must be cleared by the purge.
        string markerPath = Path.Combine(tempDir, "partition-install-2_rev.incomplete");
        File.WriteAllBytes(markerPath, []);
        Assert.True(node.Transfer.IsInstallIncomplete(2));

        Assert.True(await node.Transfer.PurgeUnhostedPartitionAsync(2, () => true, TestContext.Current.CancellationToken));

        Assert.Null(node.Backend.GetKeyValue("ranged1/a"));
        Assert.Null(node.Backend.GetKeyValue($"{ownedHash}/k"));
        Assert.Null(node.Backend.GetLock($"{ownedHash}/l"));
        Assert.NotNull(node.Backend.GetKeyValue($"{foreignHash}/k"));
        Assert.NotNull(node.Backend.GetLock($"{foreignHash}/l"));

        Assert.False(node.Receipts.Contains(ownedTx, "ranged1/a", KeyValueDurability.Persistent));
        Assert.True(node.Receipts.Contains(foreignTx, $"{foreignHash}/k", KeyValueDurability.Persistent));
        Assert.Null(node.Records.Get(ownedTx, 1));
        Assert.NotNull(node.Records.Get(foreignTx, 1));
        Assert.Null(node.Intents.Get("ranged1/pending"));

        Assert.Equal(-1, node.Backend.GetDurabilityFloor(2));
        Assert.Equal(7, node.Backend.GetDurabilityFloor(foreignPartition));

        Assert.False(node.Transfer.IsInstallIncomplete(2));
        Assert.False(File.Exists(markerPath));
    }

    [Fact]
    public async Task Purge_RefusesUnlessAbsenceIsCommitted()
    {
        Node node = MakeNode();
        Assert.True(node.Backend.StoreKeyValues([KvItem("ranged1/a")]));
        Assert.True(node.Backend.StoreDurabilityFloors([(2, 42)]));

        // The committed-absence probe answers false (a transient condition — stale map, re-gain,
        // no local leader): nothing may be touched.
        Assert.False(await node.Transfer.PurgeUnhostedPartitionAsync(2, () => false, TestContext.Current.CancellationToken));

        Assert.NotNull(node.Backend.GetKeyValue("ranged1/a"));
        Assert.Equal(42, node.Backend.GetDurabilityFloor(2));
    }

    [Fact]
    public async Task Purge_IsIdempotent_SoRestartRederivationCanRerunIt()
    {
        Node node = MakeNode();
        Assert.True(node.Backend.StoreKeyValues([KvItem("ranged1/a")]));

        Assert.True(await node.Transfer.PurgeUnhostedPartitionAsync(2, () => true, TestContext.Current.CancellationToken));
        Assert.True(await node.Transfer.PurgeUnhostedPartitionAsync(2, () => true, TestContext.Current.CancellationToken));

        Assert.Null(node.Backend.GetKeyValue("ranged1/a"));
    }

    // ── actor-resident eviction (key-value entries and lock leases) ──────────────

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);
        return node;
    }

    /// <summary>Two hash key spaces owned by different partitions of a 4-partition pool.</summary>
    private static (string SpaceA, int PartitionA, string SpaceB, int PartitionB) TwoSpacesOnDifferentPartitions()
    {
        string spaceA = "ev0";
        int partitionA = PartitionDataEnumerator.HashPartitionOfKeySpace(spaceA, 4);

        for (int i = 1; ; i++)
        {
            string spaceB = $"ev{i}";
            int partitionB = PartitionDataEnumerator.HashPartitionOfKeySpace(spaceB, 4);
            if (partitionB != partitionA)
                return (spaceA, partitionA, spaceB, partitionB);
        }
    }

    [Fact]
    public async Task EvictPartition_DropsResidentEntriesOfThatPartitionOnly_AndNeverLosesBackendData()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        (string spaceA, int partitionA, string spaceB, _) = TwoSpacesOnDifferentPartitions();

        // Ephemeral entries exist only in the actors; a persistent entry also has a backend row.
        byte[] value = Encoding.UTF8.GetBytes("v");
        (KeyValueResponseType t1, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, $"{spaceA}/eph", value, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Ephemeral, ct);
        Assert.Equal(KeyValueResponseType.Set, t1);
        (KeyValueResponseType t2, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, $"{spaceB}/eph", value, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Ephemeral, ct);
        Assert.Equal(KeyValueResponseType.Set, t2);
        (KeyValueResponseType t3, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, $"{spaceA}/dur", value, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, t3);

        await manager.KeyValues.EvictPartitionEntriesAsync(partitionA);

        // The evicted partition's ephemeral entry is gone; the other partition's is untouched.
        (KeyValueResponseType gone, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{spaceA}/eph", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, gone);

        (KeyValueResponseType kept, ReadOnlyKeyValueEntry? keptEntry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{spaceB}/eph", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct);
        Assert.Equal(KeyValueResponseType.Get, kept);
        Assert.NotNull(keptEntry);

        // Eviction drops only the resident copy: the persistent key is re-served from the backend.
        (KeyValueResponseType durType, ReadOnlyKeyValueEntry? durEntry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{spaceA}/dur", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, durType);
        Assert.NotNull(durEntry);
        Assert.Equal(value, durEntry!.Value);
    }

    [Fact]
    public async Task EvictPartition_DropsResidentLockLeasesOfThatPartitionOnly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        (string spaceA, int partitionA, string spaceB, _) = TwoSpacesOnDifferentPartitions();
        byte[] owner = Encoding.UTF8.GetBytes("holder-1");

        (LockResponseType lockedA, _) = await node.Kahuna.LocateAndTryLock($"{spaceA}/res", owner, 60_000, LockDurability.Ephemeral, ct);
        Assert.Equal(LockResponseType.Locked, lockedA);
        (LockResponseType lockedB, _) = await node.Kahuna.LocateAndTryLock($"{spaceB}/res", owner, 60_000, LockDurability.Ephemeral, ct);
        Assert.Equal(LockResponseType.Locked, lockedB);

        await manager.Locks.EvictUnhostedPartitionLocksAsync(partitionA);

        // The evicted partition's lease is gone — a different owner can acquire immediately —
        // while the other partition's lease still holds its owner out.
        byte[] otherOwner = Encoding.UTF8.GetBytes("holder-2");
        (LockResponseType reacquired, _) = await node.Kahuna.LocateAndTryLock($"{spaceA}/res", otherOwner, 60_000, LockDurability.Ephemeral, ct);
        Assert.Equal(LockResponseType.Locked, reacquired);

        (LockResponseType stillHeld, _) = await node.Kahuna.LocateAndTryLock($"{spaceB}/res", otherOwner, 60_000, LockDurability.Ephemeral, ct);
        Assert.Equal(LockResponseType.Busy, stillHeld);
    }
}
