
using Google.Protobuf;
using Kommander;
using Kommander.Data;

using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The whole-partition state transfer for user data partitions: Kommander invokes it to seed a
/// replica that cannot catch up by log backfill because the partition's WAL has been compacted
/// below what the replica needs — the normal steady state once checkpoints run. The export bundles
/// everything the partition owns on this node: its key-value rows (key-range descriptors and hash
/// key spaces alike, via <see cref="PartitionDataEnumerator"/>), its persistent locks, and its
/// slices of the durable transaction stores (completion receipts, transaction records, prepared
/// intents) — the entries whose Raft log records are compacted below the snapshot boundary and can
/// never be replayed on the receiver. Ephemeral key-values and ephemeral locks are excluded by
/// construction: they are never Raft-replicated and live only on the leader.
///
/// <para>
/// <b>Export contract.</b> The snapshot must reflect <b>at least</b> everything applied at
/// <c>upToIndex</c>; newer state is allowed, because the receiver installs its WAL boundary at
/// <c>upToIndex</c> and replays any retained entries above it — re-applying an entry already
/// reflected in the snapshot converges (in-order replay ends at the log tail). To guarantee the
/// floor, the export first drains the background writer: every applied entry's row is then visible
/// to the physical-family scan the enumerator reads. The export is not one consistent cut — keys
/// read late in the paged scan may reflect later applies than keys read early — which the
/// at-least contract explicitly permits.
/// </para>
///
/// <para>
/// <b>Import discipline.</b> The whole stream is read and checksum-verified <i>before anything is
/// mutated</i>, so a truncated or corrupt snapshot is a clean no-op. The install itself is
/// purge-then-apply — merging instead of purging would resurrect keys deleted while this node was
/// not a replica — and is bracketed by a durable install marker: the marker is created before the
/// purge and removed only after the apply and the stores' durable snapshots complete, so a crash
/// mid-install leaves the partition observably incomplete (<see cref="IsInstallIncomplete"/>) and
/// the sender's retry re-drives the whole install rather than anything serving a half-installed
/// range. Re-delivery of the same snapshot is idempotent: the purge clears whatever the previous
/// attempt applied and the apply rewrites it. The stores' per-partition snapshots are persisted
/// before returning because the WAL boundary installed right after compacts the very log entries
/// the imported receipts/records/intents came from — without a durable store snapshot a cold
/// restart could not reconstruct them.
/// </para>
/// </summary>
internal sealed class PartitionStateTransfer : IRaftPartitionStateTransfer
{
    /// <summary>Entries per exported page (bounded memory + checksum granularity).</summary>
    private const int PageSize = 256;

    /// <summary>
    /// Per-partition mutual exclusion between a seeding install and the un-host purge: the
    /// placement planner can remove and re-add a replica in quick succession, and without this
    /// gate a loss-triggered purge still walking the backend could delete rows a re-gain's
    /// snapshot install just wrote.
    /// </summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, SemaphoreSlim> installGates = new();

    private SemaphoreSlim InstallGateOf(int partitionId) =>
        installGates.GetOrAdd(partitionId, static _ => new SemaphoreSlim(1, 1));

    private readonly PartitionDataEnumerator enumerator;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly CompletionReceiptStore completionReceiptStore;

    private readonly TransactionRecordStore transactionRecordStore;

    private readonly PreparedIntentStore preparedIntentStore;

    private readonly Func<RangeMap> currentMap;

    private readonly int hashPoolSize;

    /// <summary>Drains the background writer so the physical-family scan reflects every applied entry.</summary>
    private readonly Func<Task> drainPersistence;

    private readonly string? storagePath;

    private readonly string storageRevision;

    private readonly ILogger<IKahuna> logger;

    public PartitionStateTransfer(
        PartitionDataEnumerator enumerator,
        IPersistenceBackend persistenceBackend,
        CompletionReceiptStore completionReceiptStore,
        TransactionRecordStore transactionRecordStore,
        PreparedIntentStore preparedIntentStore,
        Func<RangeMap> currentMap,
        int hashPoolSize,
        Func<Task> drainPersistence,
        string? storagePath,
        string storageRevision,
        ILogger<IKahuna> logger)
    {
        this.enumerator = enumerator;
        this.persistenceBackend = persistenceBackend;
        this.completionReceiptStore = completionReceiptStore;
        this.transactionRecordStore = transactionRecordStore;
        this.preparedIntentStore = preparedIntentStore;
        this.currentMap = currentMap;
        this.hashPoolSize = hashPoolSize;
        this.drainPersistence = drainPersistence;
        this.storagePath = storagePath;
        this.storageRevision = storageRevision;
        this.logger = logger;
    }

    // ── export ───────────────────────────────────────────────────────────────────

    public async Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
    {
        // Every applied entry's row must be visible to the scan before it can be exported, or the
        // snapshot could reflect less than upToIndex and lose data on the receiver.
        await drainPersistence().ConfigureAwait(false);

        RangeMap map = currentMap();

        MemoryStream stream = new();

        new PartitionStateHeader { PartitionId = partitionId, UpToIndex = upToIndex }.WriteDelimitedTo(stream);

        await foreach (IReadOnlyList<(string Key, ReadOnlyKeyValueEntry Entry)> page in
            enumerator.EnumerateKeyValuesAsync(partitionId, PageSize, ct).ConfigureAwait(false))
            KvStateMachineTransfer.WritePage(stream, page, hasMore: true);

        KvStateMachineTransfer.WritePage(stream, [], hasMore: false);

        await foreach (IReadOnlyList<(string Resource, LockEntry Entry)> lockPage in
            enumerator.EnumerateLocksAsync(partitionId, PageSize, ct).ConfigureAwait(false))
            WriteLockPage(stream, lockPage, hasMore: true);

        WriteLockPage(stream, [], hasMore: false);

        WriteStoreSection(stream, partitionId, map);

        logger.LogExportedPartitionState(partitionId, upToIndex, stream.Length);

        stream.Position = 0;
        return stream;
    }

    private void WriteStoreSection(Stream stream, int partitionId, RangeMap map)
    {
        List<CompletionReceiptRecord> receipts = [];
        foreach (CompletionReceiptRecord record in completionReceiptStore.SnapshotRange(null, null))
        {
            if (PartitionDataEnumerator.OwnerOfKey(map, record.Key, hashPoolSize) == partitionId)
                receipts.Add(record);
        }

        List<TransactionRecord> records = [];
        foreach (TransactionRecord record in transactionRecordStore.SnapshotRange(null, null))
        {
            if (PartitionDataEnumerator.OwnerOfKey(map, record.RecordAnchorKey, hashPoolSize) == partitionId)
                records.Add(record);
        }

        List<PreparedIntent> intents = [];
        foreach (PreparedIntent intent in preparedIntentStore.SnapshotRange(null, null))
        {
            if (PartitionDataEnumerator.OwnerOfKey(map, intent.Key, hashPoolSize) == partitionId)
                intents.Add(intent);
        }

        PartitionStateStoreSection section = new()
        {
            CompletionReceipts = receipts.Count > 0
                ? UnsafeByteOperations.UnsafeWrap(CompletionReceiptStore.SerializeImport(receipts, partitionId))
                : ByteString.Empty,
            TransactionRecords = records.Count > 0
                ? UnsafeByteOperations.UnsafeWrap(TransactionRecordStore.SerializeRecords(records))
                : ByteString.Empty,
            PreparedIntents = intents.Count > 0
                ? UnsafeByteOperations.UnsafeWrap(PreparedIntentStore.SerializeIntents(intents))
                : ByteString.Empty
        };

        section.Checksum = StoreChecksumOf(section);
        section.WriteDelimitedTo(stream);
    }

    // ── import ───────────────────────────────────────────────────────────────────

    public async Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
    {
        // ── Phase 1: read and verify the whole stream before mutating anything, so a truncated or
        // corrupt snapshot leaves the prior state untouched and the sender simply retries. ──
        PartitionStateHeader header = ParseDelimited(PartitionStateHeader.Parser, snapshot, "header");

        if (header.PartitionId != partitionId)
            throw new KahunaServerException(
                $"ImportPartitionState: snapshot is for partition {header.PartitionId} but was delivered for partition {partitionId}.");

        List<PersistenceRequestItem> kvItems = [];

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            RangeSnapshotPage page = ParseDelimited(RangeSnapshotPage.Parser, snapshot, "key-value page");

            ulong expected = KvStateMachineTransfer.ChecksumOf(page.Entries);
            if (expected != page.Checksum)
                throw new KahunaServerException(
                    $"ImportPartitionState: key-value page checksum mismatch (expected {expected}, got {page.Checksum}) — corrupt snapshot.");

            foreach (RangeSnapshotEntry entry in page.Entries)
                kvItems.Add(KvStateMachineTransfer.ToPersistenceItem(entry));

            if (!page.HasMore)
                break;
        }

        List<PersistenceRequestItem> lockItems = [];

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            PartitionStateLockPage page = ParseDelimited(PartitionStateLockPage.Parser, snapshot, "lock page");

            ulong expected = LockChecksumOf(page.Entries);
            if (expected != page.Checksum)
                throw new KahunaServerException(
                    $"ImportPartitionState: lock page checksum mismatch (expected {expected}, got {page.Checksum}) — corrupt snapshot.");

            foreach (PartitionStateLockEntry entry in page.Entries)
                lockItems.Add(ToLockPersistenceItem(entry));

            if (!page.HasMore)
                break;
        }

        PartitionStateStoreSection section = ParseDelimited(PartitionStateStoreSection.Parser, snapshot, "store section");

        ulong sectionChecksum = StoreChecksumOf(section);
        if (sectionChecksum != section.Checksum)
            throw new KahunaServerException(
                $"ImportPartitionState: store section checksum mismatch (expected {sectionChecksum}, got {section.Checksum}) — corrupt snapshot.");

        IReadOnlyList<TransactionRecord> records = section.TransactionRecords.Length > 0
            ? TransactionRecordStore.DeserializeRecords(section.TransactionRecords.ToByteArray())
            : [];
        IReadOnlyList<PreparedIntent> intents = section.PreparedIntents.Length > 0
            ? PreparedIntentStore.DeserializeIntents(section.PreparedIntents.ToByteArray())
            : [];
        byte[] receiptBytes = section.CompletionReceipts.Length > 0 ? section.CompletionReceipts.ToByteArray() : [];

        ct.ThrowIfCancellationRequested();

        // ── Phase 2: install. Marker → purge → apply → durable store snapshots → clear. A crash
        // anywhere in here leaves the marker on disk; the sender's retry re-drives the whole
        // sequence, and the purge makes the re-drive idempotent. Serialized per partition against
        // the un-host purge, so a loss-triggered purge can never interleave with a seeding install
        // of the same partition and delete freshly installed rows. ──
        SemaphoreSlim installGate = InstallGateOf(partitionId);
        await installGate.WaitAsync(ct).ConfigureAwait(false);

        try
        {
            MarkInstallIncomplete(partitionId);

            await PurgePartitionBackendRowsAsync(partitionId, ct).ConfigureAwait(false);

            if (kvItems.Count > 0 && !persistenceBackend.StoreKeyValues(kvItems))
                throw new KahunaServerException("ImportPartitionState: StoreKeyValues failed to persist the snapshot.");

            if (lockItems.Count > 0 && !persistenceBackend.StoreLocks(lockItems))
                throw new KahunaServerException("ImportPartitionState: StoreLocks failed to persist the snapshot.");

            if (receiptBytes.Length > 0 && !completionReceiptStore.Replicate(partitionId, new RaftLog
                {
                    LogType = ReplicationTypes.CompletionReceipt,
                    LogData = receiptBytes
                }))
                throw new KahunaServerException("ImportPartitionState: completion-receipt apply failed.");

            transactionRecordStore.ImportRecords(records);
            preparedIntentStore.ImportIntents(intents);

            // The WAL boundary installed right after this import compacts the log entries the imported
            // receipts/records/intents were originally replicated through — a cold restart could never
            // replay them — so their durable per-partition snapshots must exist before we report success.
            if (!completionReceiptStore.PersistSnapshot(partitionId)
                || !transactionRecordStore.PersistSnapshot(partitionId)
                || !preparedIntentStore.PersistSnapshot(partitionId))
                throw new KahunaServerException(
                    "ImportPartitionState: a durable store snapshot could not be persisted; the install is left marked incomplete for retry.");

            ClearInstallIncomplete(partitionId);
        }
        finally
        {
            installGate.Release();
        }

        logger.LogImportedPartitionState(partitionId, kvItems.Count, lockItems.Count, records.Count, intents.Count);
    }

    /// <summary>
    /// Removes everything this node retains for a partition the committed map no longer hosts
    /// here: the backend key-value and lock rows, the durable stores' slices (receipts, records,
    /// intents — with their emptied per-partition snapshots re-persisted so a cold restart cannot
    /// resurrect them from stale snapshot files), the persisted durability floor, and any
    /// half-install marker. Serialized per partition against <see cref="ImportPartitionState"/> so
    /// a purge can never delete rows a concurrent seeding install just wrote; the
    /// <paramref name="stillUnhosted"/> probe is re-evaluated under the gate and between backend
    /// pages, so a re-gain committed mid-purge aborts the walk instead of racing the new copy.
    /// Idempotent — re-running on an already-clean partition is a no-op — which is what lets a
    /// crash mid-purge be repaired by re-deriving the intent from the committed map at startup.
    /// Returns false when aborted (re-gained) or when a durable step could not complete; the
    /// startup re-derivation converges what a failed attempt left behind.
    /// </summary>
    internal async Task<bool> PurgeUnhostedPartitionAsync(int partitionId, Func<bool> stillUnhosted, CancellationToken ct)
    {
        SemaphoreSlim installGate = InstallGateOf(partitionId);
        await installGate.WaitAsync(ct).ConfigureAwait(false);

        try
        {
            if (!stillUnhosted())
                return false;

            // Flush the background writer first: a write applied just before the loss and still
            // queued would otherwise land after the purge and resurrect rows.
            await drainPersistence().ConfigureAwait(false);

            if (!await PurgePartitionBackendRowsAsync(partitionId, ct, stillUnhosted).ConfigureAwait(false))
                return false;

            RangeMap map = currentMap();

            completionReceiptStore.PurgeWhere(key => PartitionDataEnumerator.OwnerOfKey(map, key, hashPoolSize) == partitionId);
            transactionRecordStore.PurgeWhere(anchor => PartitionDataEnumerator.OwnerOfKey(map, anchor, hashPoolSize) == partitionId);
            preparedIntentStore.PurgeWhere(key => PartitionDataEnumerator.OwnerOfKey(map, key, hashPoolSize) == partitionId);

            // Re-persist the emptied slices: without this, a cold restart would reload the purged
            // receipts/records/intents from the stale per-partition snapshot files.
            if (!completionReceiptStore.PersistSnapshot(partitionId)
                || !transactionRecordStore.PersistSnapshot(partitionId)
                || !preparedIntentStore.PersistSnapshot(partitionId))
                return false;

            if (!persistenceBackend.RemoveDurabilityFloor(partitionId))
                return false;

            ClearInstallIncomplete(partitionId);
            return true;
        }
        finally
        {
            installGate.Release();
        }
    }

    /// <summary>
    /// Physically removes every backend row the partition owns — the shared purge primitive of the
    /// whole-partition install (and of un-hosting a replica). The enumerator's cursor resumes
    /// strictly after already-visited keys, so deleting a page's keys never disturbs the scan.
    /// When <paramref name="proceed"/> is supplied it is re-evaluated before every delete batch and
    /// a false answer aborts the walk (returning false) — the un-host purge uses it to stop the
    /// moment the partition is re-hosted. Returns true when the walk completed.
    /// </summary>
    internal async Task<bool> PurgePartitionBackendRowsAsync(int partitionId, CancellationToken ct, Func<bool>? proceed = null)
    {
        await foreach (IReadOnlyList<(string Key, ReadOnlyKeyValueEntry Entry)> page in
            enumerator.EnumerateKeyValuesAsync(partitionId, PageSize, ct).ConfigureAwait(false))
        {
            if (proceed is not null && !proceed())
                return false;

            List<string> keys = new(page.Count);
            foreach ((string key, _) in page)
                keys.Add(key);

            if (!persistenceBackend.DeleteKeyValues(keys))
                throw new KahunaServerException($"Failed to remove partition #{partitionId} key-value rows during install/purge.");
        }

        await foreach (IReadOnlyList<(string Resource, LockEntry Entry)> page in
            enumerator.EnumerateLocksAsync(partitionId, PageSize, ct).ConfigureAwait(false))
        {
            if (proceed is not null && !proceed())
                return false;

            List<string> resources = new(page.Count);
            foreach ((string resource, _) in page)
                resources.Add(resource);

            if (!persistenceBackend.DeleteLocks(resources))
                throw new KahunaServerException($"Failed to remove partition #{partitionId} lock rows during install/purge.");
        }

        return true;
    }

    // ── install marker ───────────────────────────────────────────────────────────

    /// <summary>
    /// Whether a whole-partition install for <paramref name="partitionId"/> started but never
    /// completed — its local data is a half-installed mixture that must not be trusted until the
    /// next snapshot delivery re-drives the install. Always false for purely in-memory deployments
    /// (no durable path ⇒ nothing survives the crash that could have been half-installed).
    /// </summary>
    internal bool IsInstallIncomplete(int partitionId) =>
        MarkerPath(partitionId) is { } path && File.Exists(path);

    private void MarkInstallIncomplete(int partitionId)
    {
        if (MarkerPath(partitionId) is { } path)
            File.WriteAllBytes(path, []);
    }

    private void ClearInstallIncomplete(int partitionId)
    {
        if (MarkerPath(partitionId) is { } path)
            File.Delete(path);
    }

    private string? MarkerPath(int partitionId) =>
        string.IsNullOrWhiteSpace(storagePath)
            ? null
            : Path.Combine(storagePath, $"partition-install-{partitionId}_{storageRevision}.incomplete");

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static T ParseDelimited<T>(MessageParser<T> parser, Stream stream, string what) where T : class, IMessage<T>
    {
        T? message;
        try
        {
            message = parser.ParseDelimitedFrom(stream);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new KahunaServerException($"ImportPartitionState: truncated or corrupt snapshot stream at {what} — {ex.Message}");
        }

        if (message is null)
            throw new KahunaServerException($"ImportPartitionState: truncated snapshot stream (missing {what}).");

        return message;
    }

    private static void WriteLockPage(Stream stream, IReadOnlyList<(string Resource, LockEntry Entry)> items, bool hasMore)
    {
        PartitionStateLockPage page = new() { HasMore = hasMore };

        foreach ((string resource, LockEntry entry) in items)
        {
            PartitionStateLockEntry message = new()
            {
                Resource = resource,
                FencingToken = entry.FencingToken,
                ExpiresNode = entry.Expires.N,
                ExpiresPhysical = entry.Expires.L,
                ExpiresCounter = entry.Expires.C,
                LastUsedNode = entry.LastUsed.N,
                LastUsedPhysical = entry.LastUsed.L,
                LastUsedCounter = entry.LastUsed.C,
                LastModifiedNode = entry.LastModified.N,
                LastModifiedPhysical = entry.LastModified.L,
                LastModifiedCounter = entry.LastModified.C,
                State = (int)entry.State
            };

            if (entry.Owner is not null)
                message.Owner = UnsafeByteOperations.UnsafeWrap(entry.Owner);

            page.Entries.Add(message);
        }

        page.Checksum = LockChecksumOf(page.Entries);
        page.WriteDelimitedTo(stream);
    }

    private static PersistenceRequestItem ToLockPersistenceItem(PartitionStateLockEntry entry)
    {
        byte[]? owner = entry.HasOwner ? entry.Owner.ToByteArray() : null;

        return new PersistenceRequestItem(
            entry.Resource,
            owner,
            entry.FencingToken,
            entry.ExpiresNode, entry.ExpiresPhysical, entry.ExpiresCounter,
            entry.LastUsedNode, entry.LastUsedPhysical, entry.LastUsedCounter,
            entry.LastModifiedNode, entry.LastModifiedPhysical, entry.LastModifiedCounter,
            entry.State);
    }

    private static ulong LockChecksumOf(IEnumerable<PartitionStateLockEntry> entries)
    {
        KvStateMachineTransfer.FnvHashStream hasher = new();
        foreach (PartitionStateLockEntry entry in entries)
            entry.WriteTo(hasher);
        return hasher.Hash;
    }

    /// <summary>FNV-1a 64 over the three store payloads in field order (checksum field excluded).</summary>
    private static ulong StoreChecksumOf(PartitionStateStoreSection section)
    {
        KvStateMachineTransfer.FnvHashStream hasher = new();
        hasher.Write(section.CompletionReceipts.Span);
        hasher.Write(section.TransactionRecords.Span);
        hasher.Write(section.PreparedIntents.Span);
        return hasher.Hash;
    }
}
