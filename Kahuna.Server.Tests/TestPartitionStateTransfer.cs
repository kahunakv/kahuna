
using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The whole-partition snapshot export/import that seeds a placement replica once the partition's
/// WAL has been compacted: the export bundles the partition's key-values, persistent locks and
/// durable-store slices; the import verifies the whole stream first, then installs it
/// purge-then-apply under a durable incomplete marker so stale keys never survive, a truncated or
/// corrupt stream is a clean no-op, re-delivery is idempotent, and a crash mid-install is
/// observable and re-driven rather than served.
/// </summary>
public sealed class TestPartitionStateTransfer : IDisposable
{
    private const int HashPoolSize = 3;

    private readonly string tempDir = Path.Combine(Path.GetTempPath(), "kahuna-pst-" + Guid.NewGuid().ToString("N"));

    public TestPartitionStateTransfer() => Directory.CreateDirectory(tempDir);

    public void Dispose()
    {
        try { Directory.Delete(tempDir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    /// <summary>First generated hash key space that does NOT hash onto <paramref name="partitionId"/> —
    /// jump-hash placement is data-dependent, so foreign spaces are computed, never assumed.</summary>
    private static string HashSpaceNotOwnedBy(int partitionId)
    {
        for (int i = 0; ; i++)
        {
            string space = $"hs{i}";
            if (PartitionDataEnumerator.HashPartitionOfKeySpace(space, HashPoolSize) != partitionId)
                return space;
        }
    }

    /// <summary>"ranged1" is a single unsplit range on partition 2; "ranged2" is split at
    /// "ranged2/m" between partition 1 and split-created partition 4; everything else hashes.</summary>
    private static RangeMap BuildMap() => new([
        new RangeDescriptor { KeySpace = "ranged1", PartitionId = 2, Generation = 1 },
        new RangeDescriptor { KeySpace = "ranged2", EndKey = "ranged2/m", PartitionId = 1, Generation = 2 },
        new RangeDescriptor { KeySpace = "ranged2", StartKey = "ranged2/m", PartitionId = 4, Generation = 2 }
    ]);

    private sealed class Node
    {
        public required IPersistenceBackend Backend { get; init; }
        public required CompletionReceiptStore Receipts { get; init; }
        public required TransactionRecordStore Records { get; init; }
        public required PreparedIntentStore Intents { get; init; }
        public required PartitionStateTransfer Transfer { get; init; }
        public bool DrainInvoked;
    }

    private Node MakeNode(IPersistenceBackend? backend = null, string? storagePath = null, Action<Node>? onDrain = null)
    {
        backend ??= new MemoryPersistenceBackend();
        RangeMap map = BuildMap();

        CompletionReceiptStore receipts = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();

        PartitionDataEnumerator enumerator = new(backend, () => map, HashPoolSize);

        Node node = null!;
        PartitionStateTransfer transfer = new(
            enumerator, backend, receipts, records, intents,
            () => map, HashPoolSize,
            () => { node!.DrainInvoked = true; onDrain?.Invoke(node!); return Task.CompletedTask; },
            storagePath, "rev", NullLogger<IKahuna>.Instance);

        node = new Node { Backend = backend, Receipts = receipts, Records = records, Intents = intents, Transfer = transfer };
        return node;
    }

    private static PersistenceRequestItem KvItem(string key, long revision = 1) => new(
        key, [1, 2], revision,
        expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
        lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
        lastModifiedNode: 0, lastModifiedPhysical: revision, lastModifiedCounter: 0,
        state: (int)KeyValueState.Set);

    private static PersistenceRequestItem LockItem(string resource, long fencingToken = 1) => new(
        resource, [3], fencingToken,
        expiresNode: 0, expiresPhysical: 1000, expiresCounter: 0,
        lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
        lastModifiedNode: 0, lastModifiedPhysical: 1, lastModifiedCounter: 0,
        state: (int)LockState.Locked);

    private static async Task<byte[]> Export(Node node, int partitionId, long upToIndex = 42)
    {
        await using Stream stream = await node.Transfer.ExportPartitionState(
            partitionId, upToIndex, TestContext.Current.CancellationToken);
        using MemoryStream buffer = new();
        await stream.CopyToAsync(buffer, TestContext.Current.CancellationToken);
        return buffer.ToArray();
    }

    private static Task Import(Node node, int partitionId, byte[] snapshot) =>
        node.Transfer.ImportPartitionState(partitionId, new MemoryStream(snapshot), TestContext.Current.CancellationToken);

    // ── round trips ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task RoundTrip_KeyRangePartition_CarriesKeyValuesAndStoreSlices()
    {
        Node source = MakeNode();

        // Partition 2 owns the whole "ranged1" space; "ranged2" and the foreign hash space belong elsewhere.
        string foreign = HashSpaceNotOwnedBy(2);
        Assert.True(source.Backend.StoreKeyValues([
            KvItem("ranged1/a", 3), KvItem("ranged1/z", 5),
            KvItem("ranged2/a"), KvItem($"{foreign}/k")]));

        HLCTimestamp txId = Ts(1000);
        source.Receipts.Record(txId, "ranged1/a", "ranged1/anchor", KeyValueDurability.Persistent);
        source.Receipts.Record(Ts(1001), $"{foreign}/k", null, KeyValueDurability.Persistent);

        List<TransactionParticipantRef> manifest = [new("ranged1/anchor", KeyValueDurability.Persistent)];
        source.Records.Apply(new InitializeTransactionCommand(txId, 1, "coord", "ranged1/anchor", Ts(1100), Ts(9000), 42, manifest, txId, txId));
        source.Records.Apply(new CommitTransactionCommand(txId, 1, 42, txId, Ts(1100)));

        source.Intents.Apply(new PrepareIntentCommand(new PreparedIntent(
            txId, 1, "ranged1/pending", ManifestHash: 42, RecordAnchorKey: "ranged1/anchor", CommitTimestamp: Ts(1100),
            State: KeyValueState.Set, Value: [7], Bucket: null, Revision: 3, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: 2, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(6000),
            Resolution: PreparedIntentResolution.Pending)));

        byte[] snapshot = await Export(source, 2);
        Assert.True(source.DrainInvoked);

        Node target = MakeNode();
        await Import(target, 2, snapshot);

        Assert.Equal(3, target.Backend.GetKeyValue("ranged1/a")!.Revision);
        Assert.Equal(5, target.Backend.GetKeyValue("ranged1/z")!.Revision);
        Assert.Null(target.Backend.GetKeyValue("ranged2/a"));
        Assert.Null(target.Backend.GetKeyValue($"{foreign}/k"));

        Assert.True(target.Receipts.Contains(txId, "ranged1/a", KeyValueDurability.Persistent));
        Assert.False(target.Receipts.Contains(Ts(1001), $"{foreign}/k", KeyValueDurability.Persistent));
        Assert.Equal(TransactionDecision.Commit, target.Records.Get(txId, 1)!.Decision);
        Assert.Equal(PreparedIntentResolution.Pending, target.Intents.Get("ranged1/pending")!.Resolution);
    }

    [Fact]
    public async Task RoundTrip_HashPartition_CarriesItsLocks()
    {
        Node source = MakeNode();

        int partitionId = PartitionDataEnumerator.HashPartitionOfKeySpace("hspace", HashPoolSize);

        Assert.True(source.Backend.StoreKeyValues([KvItem("hspace/k1"), KvItem("hspace/k2")]));
        Assert.True(source.Backend.StoreLocks([LockItem("hspace/l1", 9), LockItem("other-space/l9")]));

        byte[] snapshot = await Export(source, partitionId);

        Node target = MakeNode();
        await Import(target, partitionId, snapshot);

        Assert.NotNull(target.Backend.GetKeyValue("hspace/k1"));
        Assert.NotNull(target.Backend.GetKeyValue("hspace/k2"));
        Assert.Equal(9, target.Backend.GetLock("hspace/l1")!.FencingToken);

        // A lock in a space this partition does not own travels only with its own partition.
        if (PartitionDataEnumerator.HashPartitionOfKeySpace("other-space", HashPoolSize) != partitionId)
            Assert.Null(target.Backend.GetLock("other-space/l9"));
    }

    // ── purge semantics ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Import_PurgesStaleOwnedRows_AndLeavesForeignPartitionsUntouched()
    {
        Node source = MakeNode();
        Assert.True(source.Backend.StoreKeyValues([KvItem("ranged1/live", 8)]));

        byte[] snapshot = await Export(source, 2);

        // The target held partition 2 before: it still has a key the source deleted while this
        // node was away ("ranged1/stale") — a merge would resurrect it — plus rows of other
        // partitions that the install must not touch.
        string foreign = HashSpaceNotOwnedBy(2);
        Node target = MakeNode();
        Assert.True(target.Backend.StoreKeyValues([
            KvItem("ranged1/stale", 2), KvItem("ranged1/live", 2), KvItem("ranged2/x"), KvItem($"{foreign}/k")]));
        Assert.True(target.Backend.StoreLocks([LockItem($"{foreign}/l")]));

        await Import(target, 2, snapshot);

        Assert.Null(target.Backend.GetKeyValue("ranged1/stale"));
        Assert.Equal(8, target.Backend.GetKeyValue("ranged1/live")!.Revision);
        Assert.NotNull(target.Backend.GetKeyValue("ranged2/x"));
        Assert.NotNull(target.Backend.GetKeyValue($"{foreign}/k"));
        Assert.NotNull(target.Backend.GetLock($"{foreign}/l"));
    }

    [Fact]
    public async Task Import_IsIdempotentOnRedelivery()
    {
        Node source = MakeNode();
        Assert.True(source.Backend.StoreKeyValues([KvItem("ranged1/a", 4)]));

        byte[] snapshot = await Export(source, 2);

        Node target = MakeNode(storagePath: tempDir);
        await Import(target, 2, snapshot);
        await Import(target, 2, snapshot);

        Assert.Equal(4, target.Backend.GetKeyValue("ranged1/a")!.Revision);
        Assert.False(target.Transfer.IsInstallIncomplete(2));
    }

    // ── failure discipline ───────────────────────────────────────────────────────

    [Fact]
    public async Task TruncatedOrCorruptStream_LeavesTargetUntouched()
    {
        Node source = MakeNode();
        Assert.True(source.Backend.StoreKeyValues([KvItem("ranged1/a", 4)]));

        byte[] snapshot = await Export(source, 2);

        // Pre-seed the target with an owned row: if a bad stream reached the purge, it would vanish.
        Node target = MakeNode(storagePath: tempDir);
        Assert.True(target.Backend.StoreKeyValues([KvItem("ranged1/prior", 1)]));

        // Truncation: cut the stream in half.
        await Assert.ThrowsAsync<KahunaServerException>(() => Import(target, 2, snapshot[..(snapshot.Length / 2)]));

        // Corruption: flip a byte near the end (checksums must catch it).
        byte[] corrupt = (byte[])snapshot.Clone();
        corrupt[^3] ^= 0xFF;
        await Assert.ThrowsAsync<KahunaServerException>(() => Import(target, 2, corrupt));

        // Wrong partition: the header pins the snapshot to its partition.
        await Assert.ThrowsAsync<KahunaServerException>(() => Import(target, 3, snapshot));

        Assert.NotNull(target.Backend.GetKeyValue("ranged1/prior"));
        Assert.False(target.Transfer.IsInstallIncomplete(2));
    }

    [Fact]
    public async Task FailureMidInstall_LeavesIncompleteMarker_AndRetryCompletesAndClearsIt()
    {
        Node source = MakeNode();
        Assert.True(source.Backend.StoreKeyValues([KvItem("ranged1/a", 4)]));
        byte[] snapshot = await Export(source, 2);

        FailingStoreBackend failing = new(new MemoryPersistenceBackend()) { FailNextStoreKeyValues = true };
        Node target = MakeNode(backend: failing, storagePath: tempDir);

        // The apply fails after the marker was written and the purge ran: the install must be
        // observably incomplete, never silently half-served.
        await Assert.ThrowsAsync<KahunaServerException>(() => Import(target, 2, snapshot));
        Assert.True(target.Transfer.IsInstallIncomplete(2));

        // The sender's retry re-drives the whole install and completes it.
        await Import(target, 2, snapshot);
        Assert.False(target.Transfer.IsInstallIncomplete(2));
        Assert.Equal(4, target.Backend.GetKeyValue("ranged1/a")!.Revision);
    }

    // ── node-local coherence around the install ──────────────────────────────────

    [Fact]
    public async Task Import_DrainsQueuedWritesBeforePurge_SoAStaleFlushCannotClobberInstalledRows()
    {
        Node source = MakeNode();
        int partitionId = PartitionDataEnumerator.HashPartitionOfKeySpace("hspace", HashPoolSize);
        Assert.True(source.Backend.StoreLocks([LockItem("hspace/l1", 300)]));

        byte[] snapshot = await Export(source, partitionId);

        // The target emulates the background writer still holding a queued pre-snapshot lock write
        // (fencing token 5): the drain callback lands it in the backend. Because the import drains
        // before it purges, that stale row is deleted with the rest of the partition and the
        // installed row survives; an import that skipped the drain would let the queued write land
        // afterwards and blindly overwrite the installed fencing-token high-water mark.
        Node target = MakeNode(onDrain: node => Assert.True(node.Backend.StoreLocks([LockItem("hspace/l1", 5)])));

        await Import(target, partitionId, snapshot);

        Assert.True(target.DrainInvoked);
        Assert.Equal(300, target.Backend.GetLock("hspace/l1")!.FencingToken);
    }

    [Fact]
    public async Task Import_InvalidatesResidentState_OnlyAfterASuccessfulInstall()
    {
        Node source = MakeNode();
        Assert.True(source.Backend.StoreKeyValues([KvItem("ranged1/a", 4)]));
        byte[] snapshot = await Export(source, 2);

        List<int> invalidated = [];
        Node target = MakeNode(storagePath: tempDir);
        target.Transfer.AddResidentStateInvalidationHook(partitionId =>
        {
            invalidated.Add(partitionId);
            return Task.CompletedTask;
        });

        // A corrupt stream never reaches the install phase, so nothing is invalidated.
        byte[] corrupt = (byte[])snapshot.Clone();
        corrupt[^3] ^= 0xFF;
        await Assert.ThrowsAsync<KahunaServerException>(() => Import(target, 2, corrupt));
        Assert.Empty(invalidated);

        await Import(target, 2, snapshot);
        Assert.Equal([2], invalidated);
    }

    [Fact]
    public async Task Import_HookFailure_LeavesInstallIncomplete_AndRetryCompletes()
    {
        Node source = MakeNode();
        Assert.True(source.Backend.StoreKeyValues([KvItem("ranged1/a", 4)]));
        byte[] snapshot = await Export(source, 2);

        // A node whose resident-state invalidation fails is a half-invalidated mixture that must
        // not serve the partition: the install stays observably incomplete and the sender's retry
        // re-drives it.
        bool failNext = true;
        Node target = MakeNode(storagePath: tempDir);
        target.Transfer.AddResidentStateInvalidationHook(_ =>
        {
            if (failNext)
            {
                failNext = false;
                throw new InvalidOperationException("resident-state eviction unavailable");
            }

            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() => Import(target, 2, snapshot));
        Assert.True(target.Transfer.IsInstallIncomplete(2));

        await Import(target, 2, snapshot);
        Assert.False(target.Transfer.IsInstallIncomplete(2));
        Assert.Equal(4, target.Backend.GetKeyValue("ranged1/a")!.Revision);
    }

    // ── replay convergence above the boundary ────────────────────────────────────

    [Fact]
    public async Task ReplayOfEntriesAlreadyReflected_ConvergesWithoutRegressingRevisions()
    {
        Node source = MakeNode();
        int partitionId = PartitionDataEnumerator.HashPartitionOfKeySpace("hspace", HashPoolSize);

        // The export may over-include state newer than the boundary (the at-least contract):
        // the snapshot reflects revision 3 while upToIndex corresponds to revision 2.
        Assert.True(source.Backend.StoreKeyValues([KvItem("hspace/k", 3)]));
        byte[] snapshot = await Export(source, partitionId, upToIndex: 2);

        Node target = MakeNode();
        await Import(target, partitionId, snapshot);

        // The receiver replays its retained entries above the boundary in order — including the
        // revision-3 write already reflected in the snapshot. In-order re-application ends at the
        // log tail, so the final state never regresses below what the snapshot carried.
        Assert.True(target.Backend.StoreKeyValues([KvItem("hspace/k", 3)]));

        Assert.Equal(3, target.Backend.GetKeyValue("hspace/k")!.Revision);
    }

    /// <summary>Delegating backend whose next StoreKeyValues fails once — the crash-mid-install seam.</summary>
    private sealed class FailingStoreBackend(IPersistenceBackend inner) : IPersistenceBackend
    {
        public bool FailNextStoreKeyValues;

        public bool StoreKeyValues(List<PersistenceRequestItem> items)
        {
            if (FailNextStoreKeyValues)
            {
                FailNextStoreKeyValues = false;
                return false;
            }

            return inner.StoreKeyValues(items);
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public KeyValueScanPage ScanKeyValues(string? cursor, int limit) => inner.ScanKeyValues(cursor, limit);
        public LockScanPage ScanLocks(string? cursor, int limit) => inner.ScanLocks(cursor, limit);
        public bool DeleteKeyValues(IReadOnlyList<string> keys) => inner.DeleteKeyValues(keys);
        public bool DeleteLocks(IReadOnlyList<string> resources) => inner.DeleteLocks(resources);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }
}
