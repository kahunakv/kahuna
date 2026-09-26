
using Google.Protobuf;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
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
    public async Task Import_ReplacesTheDurableStoreSlices_APendingIntentFromBeforeTheInstallDoesNotSurvive()
    {
        // The source applied the partition's log through entry 77 and holds one live intent.
        Node source = MakeNode();
        HLCTimestamp live = Ts(2000);
        Assert.True(source.Intents.Replicate(2, new RaftLog
        {
            Id = 77,
            LogType = ReplicationTypes.PreparedIntent,
            LogData = [.. PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(PendingIntent(live, "ranged1/live"))])]
        }));

        byte[] snapshot = await Export(source, 2);

        // The target held partition 2 before it fell behind: a transaction it saw prepare but whose settlement
        // applied below the snapshot boundary — never to be replayed here — left it a pending intent, a
        // receipt and an undecided record. Merged over the install, the intent is a permanent phantom holder
        // of its key (after a leader kill: hundreds of keys rejecting every commit indefinitely). Other
        // partitions' slices must not be touched.
        HLCTimestamp stale = Ts(1000);
        Node target = MakeNode();
        target.Intents.Apply(new PrepareIntentCommand(PendingIntent(stale, "ranged1/phantom")));
        target.Intents.Apply(new PrepareIntentCommand(PendingIntent(stale, "ranged2/keep")));
        target.Receipts.Record(stale, "ranged1/phantom", "ranged1/phantom", KeyValueDurability.Persistent);
        target.Receipts.Record(stale, "ranged2/keep", "ranged2/keep", KeyValueDurability.Persistent);
        List<TransactionParticipantRef> manifest = [new("ranged1/phantom", KeyValueDurability.Persistent)];
        target.Records.Apply(new InitializeTransactionCommand(stale, 1, "coord", "ranged1/phantom", Ts(1100), Ts(9000), 42, manifest, stale, stale));
        List<TransactionParticipantRef> keepManifest = [new("ranged2/keep", KeyValueDurability.Persistent)];
        target.Records.Apply(new InitializeTransactionCommand(Ts(1001), 1, "coord", "ranged2/keep", Ts(1100), Ts(9000), 42, keepManifest, Ts(1001), Ts(1001)));

        await Import(target, 2, snapshot);

        Assert.Null(target.Intents.Get("ranged1/phantom"));
        Assert.False(target.Receipts.Contains(stale, "ranged1/phantom", KeyValueDurability.Persistent));
        Assert.Null(target.Records.Get(stale, 1));

        Assert.NotNull(target.Intents.Get("ranged1/live"));
        Assert.NotNull(target.Intents.Get("ranged2/keep"));
        Assert.True(target.Receipts.Contains(stale, "ranged2/keep", KeyValueDurability.Persistent));
        Assert.NotNull(target.Records.Get(Ts(1001), 1));

        // The installed slice carries the exporter's applied position: the tail the target replays below it is
        // history for the advisory fence.
        Assert.Equal(77, target.Intents.GetLedgerReflectedThroughIndex(2));
        Assert.True(target.Intents.IsHistoricalApply(2, 77));
        Assert.False(target.Intents.IsHistoricalApply(2, 78));
        // The real export reads its position before the intent walk, so with no entry applied during the walk
        // the whole history window is fully reflected: a replay there installs nothing the section lacks.
        Assert.Equal(77, target.Intents.GetLedgerFullyReflectedThroughIndex(2));
        Assert.True(target.Intents.IsFullyReflectedApply(2, 77));

        // The key is free: a later transaction's prepare is admitted, as on every other replica.
        Assert.Equal(TransactionApplyOutcome.Applied,
            target.Intents.Apply(new PrepareIntentCommand(PendingIntent(Ts(3000), "ranged1/phantom")), 2).Outcome);
    }

    private static PreparedIntent PendingIntent(HLCTimestamp txId, string key) => new(
        txId, 1, key, ManifestHash: 42, RecordAnchorKey: key, CommitTimestamp: new HLCTimestamp(txId.N, txId.L + 100, txId.C),
        State: KeyValueState.Set, Value: [7], Bucket: null, Revision: 3, Expires: HLCTimestamp.Zero,
        NoRevision: false, BaseRevision: 2, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(6000),
        Resolution: PreparedIntentResolution.Pending);

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
    // ── export cost and wire fidelity ────────────────────────────────────────────

    private static PreparedIntent Intent(string key, long txPhysical, int valueSize = 16, string? anchor = null) => new(
        TransactionId: Ts(txPhysical), Epoch: 1, Key: key,
        ManifestHash: 42, RecordAnchorKey: anchor ?? key,
        CommitTimestamp: Ts(txPhysical + 1),
        State: KeyValueState.Set, Value: Enumerable.Repeat((byte)(txPhysical & 0xFF), valueSize).ToArray(), Bucket: null,
        Revision: 3, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: 2, BaseState: KeyValueState.Set,
        RecoveryDeadline: Ts(6000), Resolution: PreparedIntentResolution.Pending);

    private static RaftLog IntentLog(params PreparedIntentCommand[] commands) =>
        new() { LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    /// <summary>Commits <paramref name="intent"/> through <paramref name="partitionId"/>'s log, so the store's
    /// committed-head ledger slice for that partition gains a head.</summary>
    private static void CommitThroughLog(PreparedIntentStore store, PreparedIntent intent, int partitionId)
    {
        Assert.True(store.Replicate(partitionId, IntentLog(new PrepareIntentCommand(intent))));
        Assert.True(store.Replicate(partitionId, IntentLog(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true))));
        Assert.True(store.Replicate(partitionId, IntentLog(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key))));
    }

    private static void AddRecord(TransactionRecordStore records, HLCTimestamp txId, string anchor, bool commit)
    {
        List<TransactionParticipantRef> manifest = [new(anchor, KeyValueDurability.Persistent), new(anchor + "/p2", KeyValueDurability.Persistent)];
        // The decision deadline must lie beyond the commit attempt, or the commit transition is refused.
        records.Apply(new InitializeTransactionCommand(txId, 1, "coord", anchor, Ts(txId.L + 100), Ts(txId.L + 9_000_000), 42, manifest, txId, txId));
        if (commit)
        {
            records.Apply(new CommitTransactionCommand(txId, 1, 42, txId, Ts(txId.L + 100)));
            Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
        }
    }

    /// <summary>Owned rows on partition 2 ("ranged1"), foreign rows on partition 1 ("ranged2/a…"), on
    /// split-created partition 4 ("ranged2/z…") and on a foreign hash space.</summary>
    private static void PopulateStores(Node node, int ownedIntents, int ownedRecords, int ownedReceipts, int foreignPerSpace)
    {
        string foreignHash = HashSpaceNotOwnedBy(2);
        string[] foreignSpaces = ["ranged2/a", "ranged2/z", foreignHash + "/k"];

        for (int i = 0; i < ownedIntents; i++)
            node.Intents.Apply(new PrepareIntentCommand(Intent($"ranged1/i{i:D6}", 10_000 + i, valueSize: 200, anchor: "ranged1/anchor")));

        // A few committed heads so the exported ledger slice is not empty.
        for (int i = 0; i < 5; i++)
            CommitThroughLog(node.Intents, Intent($"ranged1/committed{i}", 50_000 + i), partitionId: 2);

        for (int i = 0; i < ownedRecords; i++)
            AddRecord(node.Records, Ts(20_000 + i), $"ranged1/anchor{i:D6}", commit: i % 2 == 0);

        for (int i = 0; i < ownedReceipts; i++)
            node.Receipts.Record(Ts(30_000 + i), $"ranged1/r{i:D6}", i % 3 == 0 ? null : "ranged1/anchor", KeyValueDurability.Persistent);

        foreach (string space in foreignSpaces)
        {
            for (int i = 0; i < foreignPerSpace; i++)
            {
                node.Intents.Apply(new PrepareIntentCommand(Intent($"{space}{i:D6}", 60_000 + i)));
                AddRecord(node.Records, Ts(70_000 + i), $"{space}{i:D6}", commit: true);
                node.Receipts.Record(Ts(80_000 + i), $"{space}{i:D6}", null, KeyValueDurability.Persistent);
            }
        }
    }

    /// <summary>The store section as the exporter wrote it: everything after the header, key-value pages and lock pages.</summary>
    private static byte[] StoreSectionOf(byte[] snapshot)
    {
        using MemoryStream input = new(snapshot);

        PartitionStateHeader.Parser.ParseDelimitedFrom(input);
        while (RangeSnapshotPage.Parser.ParseDelimitedFrom(input).HasMore) { }
        while (PartitionStateLockPage.Parser.ParseDelimitedFrom(input).HasMore) { }

        return snapshot[(int)input.Position..];
    }

    /// <summary>The store section built the materialising way — whole-store snapshots filtered into lists, each
    /// list serialised to one array, the arrays wrapped in a section message — which is the wire format the
    /// receiver parses. The exporter must produce exactly these bytes without any of those copies.</summary>
    private static byte[] MaterialisedStoreSection(Node node, int partitionId)
    {
        RangeMap map = BuildMap();
        bool Owned(string key) => PartitionDataEnumerator.OwnerOfKey(map, key, HashPoolSize) == partitionId;

        List<PreparedIntent> intents = [.. node.Intents.SnapshotRange(null, null).Where(i => Owned(i.Key))];
        List<TransactionRecord> records = [.. node.Records.SnapshotRange(null, null).Where(r => Owned(r.RecordAnchorKey))];
        List<CompletionReceiptRecord> receipts = [.. node.Receipts.SnapshotRange(null, null).Where(r => Owned(r.Key))];

        PartitionStateStoreSection section = new()
        {
            CompletionReceipts = receipts.Count > 0
                ? UnsafeByteOperations.UnsafeWrap(CompletionReceiptStore.SerializeImport(receipts, partitionId))
                : ByteString.Empty,
            TransactionRecords = records.Count > 0
                ? UnsafeByteOperations.UnsafeWrap(TransactionRecordStore.SerializeRecords(records))
                : ByteString.Empty,
            // The streamed export reads its position before the intent walk; nothing applies during this
            // materialisation, so the position read here is that same cut.
            PreparedIntents = UnsafeByteOperations.UnsafeWrap(node.Intents.SerializePartitionIntents(partitionId, intents, node.Intents.GetAppliedLogIndex(partitionId)))
        };

        KvStateMachineTransfer.FnvHashStream hasher = new();
        hasher.Write(section.CompletionReceipts.Span);
        hasher.Write(section.TransactionRecords.Span);
        hasher.Write(section.PreparedIntents.Span);
        section.Checksum = hasher.Hash;

        using MemoryStream output = new();
        section.WriteDelimitedTo(output);
        return output.ToArray();
    }

    [Fact]
    public async Task Export_StreamsTheStoreSection_ByteForByteAsTheMaterialisedShape()
    {
        Node source = MakeNode();

        // Enough owned intents that the intent payload spans several pooled segments, so the splice into the
        // section crosses segment boundaries; the ledger slice, records with participants, receipts with and
        // without anchors, and foreign rows on three other partitions are all present.
        PopulateStores(source, ownedIntents: 2_500, ownedRecords: 300, ownedReceipts: 300, foreignPerSpace: 200);

        byte[] snapshot = await Export(source, 2);
        byte[] section = StoreSectionOf(snapshot);

        Assert.True(section.Length > 2 * SegmentedBufferStream.SegmentSize);
        Assert.Equal(MaterialisedStoreSection(source, 2), section);

        // The receiver installs exactly the owned slice from the streamed section.
        Node target = MakeNode();
        await Import(target, 2, snapshot);

        Assert.NotNull(target.Intents.Get("ranged1/i000000"));
        Assert.NotNull(target.Intents.Get("ranged1/i002499"));
        Assert.Null(target.Intents.Get("ranged2/a000000"));
        Assert.Equal(TransactionDecision.Commit, target.Records.Get(Ts(20_000), 1)!.Decision);
        Assert.Null(target.Records.Get(Ts(70_000), 1));
        Assert.True(target.Receipts.Contains(Ts(30_000), "ranged1/r000000", KeyValueDurability.Persistent));
        Assert.False(target.Receipts.Contains(Ts(80_000), "ranged2/a000000", KeyValueDurability.Persistent));
        Assert.True(target.Intents.TryGetLedgerHead(2, "ranged1/committed0", out _, out _, out _));
        Assert.Equal(source.Intents.SnapshotLedger(2), target.Intents.SnapshotLedger(2));
    }

    [Fact]
    public async Task Export_OmitsEmptyRecordAndReceiptFields_ExactlyAsTheMaterialisedShape()
    {
        Node source = MakeNode();
        PopulateStores(source, ownedIntents: 3, ownedRecords: 0, ownedReceipts: 0, foreignPerSpace: 50);

        // Partition 2 owns intents only; partition 4 owns nothing at all (the section still carries its ledger marker).
        Assert.Equal(MaterialisedStoreSection(source, 2), StoreSectionOf(await Export(source, 2)));
        Assert.Equal(MaterialisedStoreSection(source, 4), StoreSectionOf(await Export(source, 4)));
    }

    private static (long Intents, long Records, long Receipts) MeasureSliceWriters(int foreignRows)
    {
        PreparedIntentStore intents = new();
        TransactionRecordStore records = new();
        CompletionReceiptStore receipts = new();

        for (int i = 0; i < 200; i++)
        {
            intents.Apply(new PrepareIntentCommand(Intent($"a/{i:D6}", 1_000 + i)));
            AddRecord(records, Ts(1_000 + i), $"a/{i:D6}", commit: true);
            receipts.Record(Ts(1_000 + i), $"a/{i:D6}", "a/anchor", KeyValueDurability.Persistent);
        }

        for (int i = 0; i < foreignRows; i++)
        {
            intents.Apply(new PrepareIntentCommand(Intent($"b/{i:D6}", 100_000 + i)));
            AddRecord(records, Ts(100_000 + i), $"b/{i:D6}", commit: true);
            receipts.Record(Ts(100_000 + i), $"b/{i:D6}", null, KeyValueDurability.Persistent);
        }

        // An allocation-free ownership test, so the measurement is the walk and the owned rows only.
        static bool Owned(string key) => key.StartsWith("a/", StringComparison.Ordinal);

        long Measure(Action write)
        {
            write(); // warm-up: JIT, pooled sub-messages
            long before = GC.GetAllocatedBytesForCurrentThread();
            write();
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        long intentBytes = Measure(() => Assert.Equal(200, intents.WritePartitionSection(Stream.Null, 7, Owned)));
        long recordBytes = Measure(() => Assert.Equal(200, records.WritePartitionRecords(Stream.Null, Owned)));
        long receiptBytes = Measure(() => Assert.Equal(200, receipts.WritePartitionReceipts(Stream.Null, 7, Owned)));

        return (intentBytes, recordBytes, receiptBytes);
    }

    [Fact]
    public void StoreSliceWriters_AllocateForTheOwnedSlice_NotForTheWholeStore()
    {
        // The same owned slice (200 rows per store) next to 200 vs. 40,000 foreign rows: a writer that copied the
        // store before filtering (the old snapshot-then-filter shape) would allocate hundreds of kilobytes more on
        // the large store; the streaming walk materialises no foreign row, so its allocation does not move.
        (long smallIntents, long smallRecords, long smallReceipts) = MeasureSliceWriters(foreignRows: 200);
        (long largeIntents, long largeRecords, long largeReceipts) = MeasureSliceWriters(foreignRows: 40_000);

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"bytes per slice write with 200 vs 40,000 foreign rows — intents {smallIntents}/{largeIntents}, records {smallRecords}/{largeRecords}, receipts {smallReceipts}/{largeReceipts}");

        const long slack = 16 * 1024;
        Assert.True(largeIntents <= smallIntents + slack, $"intents: {smallIntents} B with 200 foreign rows, {largeIntents} B with 40,000");
        Assert.True(largeRecords <= smallRecords + slack, $"records: {smallRecords} B with 200 foreign rows, {largeRecords} B with 40,000");
        Assert.True(largeReceipts <= smallReceipts + slack, $"receipts: {smallReceipts} B with 200 foreign rows, {largeReceipts} B with 40,000");
    }

    [Fact]
    public async Task IntentSliceWriter_UnderConcurrentForeignChurn_WritesEveryOwnedIntentExactlyOnce()
    {
        // The walk is lock-free while prepares and purges of other keys grow, shrink and re-bucket the map. Every
        // intent present for the whole walk must be written exactly once — the property the export's at-least
        // contract rests on — and no key outside the owned set may leak into the slice.
        PreparedIntentStore store = new();

        HashSet<string> owned = [];
        for (int i = 0; i < 2_000; i++)
        {
            string key = $"a/{i:D6}";
            owned.Add(key);
            store.Apply(new PrepareIntentCommand(Intent(key, 1_000 + i)));
        }

        static bool Owned(string key) => key.StartsWith("a/", StringComparison.Ordinal);

        using CancellationTokenSource stop = new();
        Task churn = Task.Run(() =>
        {
            int generation = 0;
            while (!stop.IsCancellationRequested)
            {
                int gen = generation++;
                for (int i = 0; i < 3_000; i++)
                    store.Apply(new PrepareIntentCommand(Intent($"b/{gen}/{i:D6}", 500_000 + i)));

                store.PurgeWhere(key => key.StartsWith("b/", StringComparison.Ordinal) && (key.GetHashCode() & 3) != 0);
            }
        }, TestContext.Current.CancellationToken);

        try
        {
            for (int iteration = 0; iteration < 150; iteration++)
            {
                using SegmentedBufferStream payload = new();
                int written = store.WritePartitionSection(payload, 7, Owned);
                Assert.Equal(owned.Count, written);

                byte[] bytes = new byte[payload.Length];
                Assert.Equal(bytes.Length, payload.Read(bytes, 0, bytes.Length));
                PreparedIntentStore.PartitionIntentSection section = PreparedIntentStore.DeserializePartitionIntents(bytes);

                HashSet<string> seen = [];
                foreach (PreparedIntent intent in section.Intents)
                {
                    Assert.True(owned.Contains(intent.Key), $"foreign key {intent.Key} leaked into the slice");
                    Assert.True(seen.Add(intent.Key), $"key {intent.Key} written twice");
                }

                Assert.Equal(owned.Count, seen.Count);
            }
        }
        finally
        {
            stop.Cancel();
            await churn;
        }
    }

    [Fact]
    public async Task Export_BuildsTheSnapshotInPooledSegments_AndAMultiSegmentSnapshotRoundTrips()
    {
        Node source = MakeNode();

        // ~4 MB of key-value rows plus a store section: the snapshot spans dozens of segments.
        List<PersistenceRequestItem> rows = [];
        byte[] value = new byte[8 * 1024];
        for (int i = 0; i < 512; i++)
        {
            value[0] = (byte)i;
            rows.Add(new PersistenceRequestItem(
                $"ranged1/k{i:D6}", (byte[])value.Clone(), i + 1,
                expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
                lastModifiedNode: 0, lastModifiedPhysical: i + 1, lastModifiedCounter: 0,
                state: (int)KeyValueState.Set));
        }
        Assert.True(source.Backend.StoreKeyValues(rows));
        PopulateStores(source, ownedIntents: 400, ownedRecords: 50, ownedReceipts: 50, foreignPerSpace: 20);

        await using Stream stream = await source.Transfer.ExportPartitionState(2, 42, TestContext.Current.CancellationToken);

        // Structural proof that no buffer of the snapshot's size exists: the stream is segmented and every
        // segment is a small-object-heap array.
        SegmentedBufferStream segmented = Assert.IsType<SegmentedBufferStream>(stream);
        Assert.True(segmented.Length > 4 * 1024 * 1024);
        Assert.True(segmented.SegmentCount > 60);
        for (int i = 0; i < segmented.SegmentCount; i++)
            Assert.True(segmented.GetSegment(i).Array!.Length < 85_000);

        using MemoryStream copy = new();
        await stream.CopyToAsync(copy, TestContext.Current.CancellationToken);
        Assert.Equal(segmented.Length, copy.Length);

        Node target = MakeNode();
        await Import(target, 2, copy.ToArray());

        Assert.Equal(512, target.Backend.GetKeyValue("ranged1/k000511")!.Revision);
        Assert.Equal(8 * 1024, target.Backend.GetKeyValue("ranged1/k000000")!.Value!.Length);
        Assert.NotNull(target.Intents.Get("ranged1/i000399"));
        Assert.Equal(source.Intents.SnapshotLedger(2), target.Intents.SnapshotLedger(2));
    }

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
