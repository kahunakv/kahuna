
using Google.Protobuf;
using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication.Protos;
using Kahuna.Utils;

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
/// read late in the paged scan may reflect later applies than keys read early, and the durable
/// stores' slices are walked lock-free with the ownership filter inside the walk — which the
/// at-least contract explicitly permits. The export costs O(partition) memory, not O(node): each
/// store streams only its owned rows, and the snapshot is built in pooled 64 KB segments rather
/// than one doubling buffer.
/// </para>
///
/// <para>
/// <b>Import discipline.</b> The whole stream is read, checksum-verified and decoded <i>before anything
/// is mutated</i>, so a truncated or corrupt snapshot is a clean no-op. That verification pass keeps
/// nothing but counts and positions, and the apply pass then streams rows and durable-store entries
/// from the staged snapshot into the backend and the stores, so the install's memory is a page of rows
/// plus the stores' own contents, never a second copy of the partition: materialising the store section
/// once to verify it and again to decode it is what ran a node out of memory at a partition of a few
/// hundred thousand transaction records. The install itself is
/// drain-then-purge-then-apply: the background writer is drained first so a queued pre-snapshot
/// flush cannot land after the install and blindly overwrite an installed row, and the purge
/// (rather than a merge) prevents resurrecting keys deleted while this node was not a replica.
/// The durable stores' slices are purged the same way before their installed slices land, so a
/// receipt, record or still-pending intent this node retained from before the install cannot
/// outlive it (a merged-over pending intent is a permanent phantom holder of its key).
/// The sequence is bracketed by a durable install marker: the marker is created before the
/// purge and removed only after the apply, the stores' durable snapshots, and the resident-state
/// invalidation complete, so a crash mid-install leaves the partition observably incomplete
/// (<see cref="IsInstallIncomplete"/>) and the sender's retry re-drives the whole install rather
/// than anything serving a half-installed range. Re-delivery of the same snapshot is idempotent:
/// the purge clears whatever the previous attempt applied and the apply rewrites it. The stores'
/// per-partition snapshots are persisted before returning because the WAL boundary installed right
/// after compacts the very log entries the imported receipts/records/intents came from — without a
/// durable store snapshot a cold restart could not reconstruct them. Finally, node-local resident
/// caches for the partition are invalidated (<see cref="residentStateInvalidationHooks"/>): every
/// mutation below the boundary arrived only through this install, so resident lock leases and
/// key-value entries are stale and must never again be served over the installed rows.
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

    /// <summary>
    /// Callbacks that drop node-local resident state (actor-cached lock leases and key-value
    /// entries) for a partition after a snapshot install replaced its backend rows. Mutations below
    /// the snapshot boundary reach this node only through the install — the replicators never see
    /// them, so no cache-coherence apply ever advances a resident entry — and resident entries are
    /// served with precedence over the backend (a lock grant mints fencingToken+1 straight from the
    /// resident lease). Without this invalidation, a node re-seeded after falling behind or after a
    /// replica move would, on a later leader promotion, mint fencing tokens below the installed
    /// high-water mark — regressing and reusing tokens that were already granted.
    /// </summary>
    private readonly List<Func<int, Task>> residentStateInvalidationHooks = [];

    /// <summary>Registers a resident-state invalidation callback; see <see cref="residentStateInvalidationHooks"/>.</summary>
    internal void AddResidentStateInvalidationHook(Func<int, Task> hook) => residentStateInvalidationHooks.Add(hook);

    /// <summary>
    /// Callbacks told, once an install completed, the log index the installed projection reflects. The
    /// entries at or below it never reach the replicators on this node, so a per-node apply high-water
    /// mark derived from delivered entries alone would stay below what the projection actually holds.
    /// </summary>
    private readonly List<Action<int, long>> installedThroughObservers = [];

    /// <summary>Registers an installed-boundary observer; see <see cref="installedThroughObservers"/>.</summary>
    internal void AddInstalledThroughObserver(Action<int, long> observer) => installedThroughObservers.Add(observer);

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

    // Whether an installed partition snapshot must carry the committed-head ledger (see ImportPartitionState).
    private readonly bool requireLedgerOnInstall;

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
        ILogger<IKahuna> logger,
        bool requireLedgerOnInstall = false)
    {
        this.requireLedgerOnInstall = requireLedgerOnInstall;
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

        // Segmented and pooled: the snapshot grows one 64 KB segment at a time, so an export of any size
        // allocates no doubling-growth ladder of ever-larger large-object-heap buffers, and an export that
        // repeats on a cadence (a rescue loop re-seeding a follower) reuses the segments it returned.
        SegmentedBufferStream stream = new();

        try
        {
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
        }
        catch
        {
            // A refused page or a cancellation abandons the export: hand the segments back to the pool now
            // rather than when the garbage collector gets to the half-built stream.
            stream.Dispose();
            throw;
        }

        logger.LogExportedPartitionState(partitionId, upToIndex, stream.Length);

        return stream;
    }

    private void WriteStoreSection(Stream stream, int partitionId, RangeMap map)
    {
        // Each store streams its owned slice straight into a pooled payload buffer through one reused entry
        // message, with the ownership filter inside the walk: an export reads every entry the node holds
        // once (the walk) but materialises only the partition's rows (the payload), and never copies a whole
        // store or builds one protobuf object per row. The three payloads are then spliced into the section
        // by hand, byte-for-byte what serialising a PartitionStateStoreSection over them would produce, so
        // no exact-size copy of a payload is ever taken.
        //
        // The intent side (intents plus the partition's committed-head ledger) is captured BEFORE the record
        // side, and the order is load-bearing: the installing replica re-judges every bundled commit delivered
        // after the boundary against the installed intent set and ledger, unless the installed record already
        // carries the outcome. Capturing the intent side first puts its position at or before the record
        // side's, so each such commit is either already decided in the records or judged against intent/ledger
        // state that is exact at its position once the entries between them are applied. The walks are not
        // point-in-time cuts (see each store's writer); the argument holds per key because every entry above
        // the boundary — which precedes every walk — is replayed in order on top of what was captured.
        Func<string, bool> isOwned = key => PartitionDataEnumerator.OwnerOfKey(map, key, hashPoolSize) == partitionId;

        using SegmentedBufferStream intents = new();
        using SegmentedBufferStream records = new();
        using SegmentedBufferStream receipts = new();

        // Always written, even with no intents: the section carries the ledger (possibly empty) and the
        // marker that tells the importer it was written by a build that has one.
        preparedIntentStore.WritePartitionSection(intents, partitionId, isOwned);
        int recordCount = transactionRecordStore.WritePartitionRecords(records, isOwned);
        int receiptCount = completionReceiptStore.WritePartitionReceipts(receipts, partitionId, isOwned);

        // A payload field is present only when it carries rows: an empty bytes field is omitted from the
        // wire exactly as proto3 omits it, and the importer treats an absent field as no rows.
        WriteDelimitedStoreSection(
            stream,
            receiptCount > 0 ? receipts : null,
            recordCount > 0 ? records : null,
            intents);
    }

    /// <summary>
    /// Writes a length-delimited <see cref="PartitionStateStoreSection"/> whose payload fields are the given
    /// buffers (null = absent) and whose checksum is FNV-1a 64 over the payload bytes in field order — the
    /// same bytes <c>WriteDelimitedTo</c> would produce for a section message wrapping those payloads, without
    /// ever holding a payload in one contiguous array.
    /// </summary>
    private static void WriteDelimitedStoreSection(
        Stream stream, SegmentedBufferStream? receipts, SegmentedBufferStream? records, SegmentedBufferStream intents)
    {
        KvStateMachineTransfer.FnvHashStream hasher = new();
        HashPayload(hasher, receipts);
        HashPayload(hasher, records);
        HashPayload(hasher, intents);
        ulong checksum = hasher.Hash;

        int size =
            PayloadFieldSize(PartitionStateStoreSection.CompletionReceiptsFieldNumber, receipts)
            + PayloadFieldSize(PartitionStateStoreSection.TransactionRecordsFieldNumber, records)
            + PayloadFieldSize(PartitionStateStoreSection.PreparedIntentsFieldNumber, intents)
            + CodedOutputStream.ComputeTagSize(PartitionStateStoreSection.ChecksumFieldNumber)
            + CodedOutputStream.ComputeUInt64Size(checksum);

        using CodedOutputStream output = new(stream, leaveOpen: true);

        output.WriteLength(size);
        WritePayloadField(output, stream, PartitionStateStoreSection.CompletionReceiptsFieldNumber, receipts);
        WritePayloadField(output, stream, PartitionStateStoreSection.TransactionRecordsFieldNumber, records);
        WritePayloadField(output, stream, PartitionStateStoreSection.PreparedIntentsFieldNumber, intents);
        output.WriteTag(PartitionStateStoreSection.ChecksumFieldNumber, WireFormat.WireType.Varint);
        output.WriteUInt64(checksum);
    }

    private static void HashPayload(KvStateMachineTransfer.FnvHashStream hasher, SegmentedBufferStream? payload)
    {
        if (payload is null)
            return;

        for (int i = 0; i < payload.SegmentCount; i++)
            hasher.Write(payload.GetSegment(i).AsSpan());
    }

    private static int PayloadLength(SegmentedBufferStream payload)
    {
        if (payload.Length > int.MaxValue)
            throw new KahunaServerException($"ExportPartitionState: a store payload of {payload.Length} bytes exceeds the protobuf field limit.");

        return (int)payload.Length;
    }

    private static int PayloadFieldSize(int fieldNumber, SegmentedBufferStream? payload)
    {
        if (payload is null)
            return 0;

        int length = PayloadLength(payload);
        return CodedOutputStream.ComputeTagSize(fieldNumber) + CodedOutputStream.ComputeLengthSize(length) + length;
    }

    // Writes the field's tag and length through the encoder, then hands the payload segments to the
    // underlying stream directly: the encoder is flushed first so the two writers never interleave.
    private static void WritePayloadField(CodedOutputStream output, Stream stream, int fieldNumber, SegmentedBufferStream? payload)
    {
        if (payload is null)
            return;

        output.WriteTag(fieldNumber, WireFormat.WireType.LengthDelimited);
        output.WriteLength(PayloadLength(payload));
        output.Flush();

        payload.Position = 0;
        payload.CopyTo(stream);
    }

    // ── import ───────────────────────────────────────────────────────────────────

    /// <summary>Key-value or lock rows buffered before each backend write of the install: bounds the rows held
    /// between the stream and the backend without paying one backend write per 256-row page.</summary>
    private const int ApplyBatchRows = 4_096;

    /// <summary>Value bytes buffered before each backend write of the install, whichever limit comes first.</summary>
    private const long ApplyBatchBytes = 16L * 1024 * 1024;

    /// <summary>
    /// Partitions with an install running on this node. A second install of the same partition while one runs
    /// is refused rather than queued: it would verify and then re-purge underneath the first one's apply, and a
    /// queued attempt pins its whole staged snapshot for as long as it waits. Kommander already serializes
    /// installs on the partition executor, so this only ever fires if that changes; the sender retries.
    /// </summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, byte> installsInProgress = new();

    public async Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
    {
        if (!installsInProgress.TryAdd(partitionId, 0))
            throw new KahunaServerException(
                $"ImportPartitionState: an install of partition {partitionId} is already running on this node; refusing a concurrent one.");

        SegmentedBufferStream? spool = null;

        try
        {
            Stream input = snapshot;

            // The install reads the stream twice (verify, then apply), so it needs to seek. Kommander stages every
            // snapshot in a seekable buffer; any other caller's stream is staged once here, in pooled segments.
            if (!snapshot.CanSeek)
            {
                spool = new SegmentedBufferStream();
                await snapshot.CopyToAsync(spool, ct).ConfigureAwait(false);
                spool.Position = 0;
                input = spool;
            }

            await ImportSeekableAsync(partitionId, input, ct).ConfigureAwait(false);
        }
        finally
        {
            spool?.Dispose();
            installsInProgress.TryRemove(partitionId, out _);
        }
    }

    /// <summary>
    /// The install proper, in two passes over a seekable snapshot so that its memory is bounded by a page of rows
    /// and one durable-store entry, never by the size of the partition. Phase 1 reads the whole stream, verifies
    /// every checksum and decodes every entry, keeping nothing but counts, the store payloads' positions and the
    /// intent section. Phase 2 rewinds and streams the rows and entries into the backend and the stores. Both
    /// the key-value/lock pages and the three durable payloads are therefore read once to verify and once to
    /// apply; the only state held whole is the intent section, whose replacement of the partition's intents and
    /// committed-head ledger must be atomic, and whose size is bounded by in-flight transactions and the ledger's
    /// retention window rather than by throughput.
    /// </summary>
    private async Task ImportSeekableAsync(int partitionId, Stream input, CancellationToken ct)
    {
        // ── Phase 1: read, verify and decode the whole stream before mutating anything, so a truncated or corrupt
        // snapshot leaves the prior state untouched and the sender simply retries. ──
        PartitionStateHeader header = ParseDelimited(PartitionStateHeader.Parser, input, "header");

        if (header.PartitionId != partitionId)
            throw new KahunaServerException(
                $"ImportPartitionState: snapshot is for partition {header.PartitionId} but was delivered for partition {partitionId}.");

        long rowsStart = input.Position;

        int keyValueCount = ReadKeyValuePages(input, store: null, ct);
        int lockCount = ReadLockPages(input, store: null, ct);

        StoreSectionLayout layout = ReadStoreSectionLayout(input);

        int receiptCount = CountDecoded(CompletionReceiptStore.ReadReceipts(new PayloadStream(input, layout.Receipts)), "completion receipts");
        int recordCount = CountDecoded(TransactionRecordStore.ReadRecords(new PayloadStream(input, layout.Records)), "transaction records");

        // An empty section is an exporter that predates the ledger (it wrote nothing when it had no intents);
        // it decodes as no intents and no ledger, which the install below refuses when the ledger is required.
        PreparedIntentStore.PartitionIntentSection intentSection;
        try
        {
            intentSection = layout.Intents.Length > 0
                ? PreparedIntentStore.DeserializePartitionIntents(new PayloadStream(input, layout.Intents))
                : new PreparedIntentStore.PartitionIntentSection([], null, HLCTimestamp.Zero);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new KahunaServerException($"ImportPartitionState: corrupt snapshot at prepared intents — {ex.Message}");
        }

        if (requireLedgerOnInstall && intentSection.Ledger is null)
            throw new KahunaServerException(
                $"ImportPartitionState: the snapshot for partition {partitionId} carries no committed-head ledger (exported by a build that predates it); " +
                "refusing to install it while OnePhaseApplyTimeValidation is enabled.");

        ct.ThrowIfCancellationRequested();

        // ── Phase 2: install. Marker → purge → apply → durable store snapshots → clear. A crash, or a failure,
        // anywhere in here leaves the marker on disk; the sender's retry re-drives the whole sequence, and the
        // purge makes the re-drive idempotent: whatever a failed attempt applied is removed before the next one
        // applies, so no attempt ever lands on top of another's partial state. Serialized per partition against
        // the un-host purge, so a loss-triggered purge can never interleave with a seeding install of the same
        // partition and delete freshly installed rows. ──
        SemaphoreSlim installGate = InstallGateOf(partitionId);
        await installGate.WaitAsync(ct).ConfigureAwait(false);

        try
        {
            MarkInstallIncomplete(partitionId);

            // Flush the background writer before purging, exactly like the un-host purge: a write
            // queued by a pre-snapshot apply and still unflushed would otherwise land after the
            // install. Lock rows are blind upserts, so a late write resurrects an already-superseded
            // fencing-token high-water mark. Key-value current rows advance monotonically, but the
            // purge removes the rows the guard would compare against, so a late write would still
            // re-insert pre-snapshot state over an installed gap.
            await drainPersistence().ConfigureAwait(false);

            await PurgePartitionBackendRowsAsync(partitionId, ct).ConfigureAwait(false);

            // The durable stores' slices follow the same purge-then-apply discipline as the rows, for the
            // same reason: whatever this node retained for the partition from before the install — a receipt,
            // a record, above all a still-pending intent — describes history whose continuation lies below the
            // snapshot boundary, compacted away and never to be replayed here. Merged over the installed state,
            // that retention does not age out: a pending intent whose settlement this node never saw stays a
            // phantom holder of its key for good, rejecting every later prepare of the key as a foreign holder,
            // refusing every bundled commit of it at apply, freezing the key's row and committed head on this
            // node and answering NotApplied to every fence ask about it (the shape seen after a leader kill,
            // where the restarted node re-attested to the fence at half throughput indefinitely). Purging first
            // also means the retired slice is released before the installed one is decoded into the stores.
            Func<string, bool> isOwned = OwnedKeyPredicate(currentMap(), partitionId);
            completionReceiptStore.PurgeWhere(isOwned);
            transactionRecordStore.PurgeWhere(isOwned);

            input.Position = rowsStart;

            ReadKeyValuePages(input, batch =>
            {
                if (!persistenceBackend.StoreKeyValues(batch))
                    throw new KahunaServerException("ImportPartitionState: StoreKeyValues failed to persist the snapshot.");
            }, ct);

            ReadLockPages(input, batch =>
            {
                if (!persistenceBackend.StoreLocks(batch))
                    throw new KahunaServerException("ImportPartitionState: StoreLocks failed to persist the snapshot.");
            }, ct);

            // Each entry is decoded off the staged snapshot and folded into its store before the next is read.
            completionReceiptStore.ImportRange(CompletionReceiptStore.ReadReceipts(new PayloadStream(input, layout.Receipts)));
            transactionRecordStore.ImportRecords(TransactionRecordStore.ReadRecords(new PayloadStream(input, layout.Records)));
            preparedIntentStore.ReplacePartitionIntents(partitionId, intentSection, requireLedgerOnInstall, isOwned);

            // The WAL boundary installed right after this import compacts the log entries the imported
            // receipts/records/intents were originally replicated through — a cold restart could never
            // replay them — so their durable per-partition snapshots must exist before we report success.
            if (!completionReceiptStore.PersistSnapshot(partitionId)
                || !transactionRecordStore.PersistSnapshot(partitionId)
                || !preparedIntentStore.PersistSnapshot(partitionId))
                throw new KahunaServerException(
                    "ImportPartitionState: a durable store snapshot could not be persisted; the install is left marked incomplete for retry.");

            // Drop resident actor state that predates the install; anything left resident would be
            // served with precedence over the freshly installed rows. Runs while the partition's
            // apply stream is quiescent (the install owns the partition's single-writer executor),
            // so nothing can repopulate a stale entry concurrently. A failure propagates: the
            // install stays marked incomplete and the sender re-drives it, because a half-invalidated
            // node must not serve the partition.
            foreach (Func<int, Task> hook in residentStateInvalidationHooks)
                await hook(partitionId).ConfigureAwait(false);

            ClearInstallIncomplete(partitionId);

            foreach (Action<int, long> observer in installedThroughObservers)
                observer(partitionId, header.UpToIndex);
        }
        finally
        {
            installGate.Release();
        }

        logger.LogImportedPartitionState(partitionId, keyValueCount, lockCount, recordCount, receiptCount, intentSection.Intents.Count, input.Length);
    }

    /// <summary>
    /// Reads the key-value pages from the current position through the terminal page, verifying each page's
    /// checksum. With a <paramref name="store"/> callback the rows are handed to it in batches of at most
    /// <see cref="ApplyBatchRows"/> rows or <see cref="ApplyBatchBytes"/> value bytes; each batch is a fresh
    /// list the callback may retain. Returns the row count.
    /// </summary>
    private static int ReadKeyValuePages(Stream input, Action<List<PersistenceRequestItem>>? store, CancellationToken ct)
    {
        int count = 0;
        List<PersistenceRequestItem>? batch = store is null ? null : new(ApplyBatchRows);
        long batchBytes = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            RangeSnapshotPage page = ParseDelimited(RangeSnapshotPage.Parser, input, "key-value page");

            ulong expected = KvStateMachineTransfer.ChecksumOf(page.Entries);
            if (expected != page.Checksum)
                throw new KahunaServerException(
                    $"ImportPartitionState: key-value page checksum mismatch (expected {expected}, got {page.Checksum}) — corrupt snapshot.");

            count += page.Entries.Count;

            if (batch is not null)
            {
                foreach (RangeSnapshotEntry entry in page.Entries)
                {
                    PersistenceRequestItem item = KvStateMachineTransfer.ToPersistenceItem(entry);
                    batch.Add(item);
                    batchBytes += item.Value?.Length ?? 0;

                    if (batch.Count >= ApplyBatchRows || batchBytes >= ApplyBatchBytes)
                    {
                        store!(batch);
                        batch = new(ApplyBatchRows);
                        batchBytes = 0;
                    }
                }
            }

            if (!page.HasMore)
                break;
        }

        if (batch is { Count: > 0 })
            store!(batch);

        return count;
    }

    /// <summary>The lock-page counterpart of <see cref="ReadKeyValuePages"/>.</summary>
    private static int ReadLockPages(Stream input, Action<List<PersistenceRequestItem>>? store, CancellationToken ct)
    {
        int count = 0;
        List<PersistenceRequestItem>? batch = store is null ? null : new(ApplyBatchRows);
        long batchBytes = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            PartitionStateLockPage page = ParseDelimited(PartitionStateLockPage.Parser, input, "lock page");

            ulong expected = LockChecksumOf(page.Entries);
            if (expected != page.Checksum)
                throw new KahunaServerException(
                    $"ImportPartitionState: lock page checksum mismatch (expected {expected}, got {page.Checksum}) — corrupt snapshot.");

            count += page.Entries.Count;

            if (batch is not null)
            {
                foreach (PartitionStateLockEntry entry in page.Entries)
                {
                    PersistenceRequestItem item = ToLockPersistenceItem(entry);
                    batch.Add(item);
                    batchBytes += item.Value?.Length ?? 0;

                    if (batch.Count >= ApplyBatchRows || batchBytes >= ApplyBatchBytes)
                    {
                        store!(batch);
                        batch = new(ApplyBatchRows);
                        batchBytes = 0;
                    }
                }
            }

            if (!page.HasMore)
                break;
        }

        if (batch is { Count: > 0 })
            store!(batch);

        return count;
    }

    /// <summary>Enumerates a lazily decoded store payload to its end, turning a decode failure into a corrupt-snapshot
    /// error, and returns the entry count.</summary>
    private static int CountDecoded<T>(IEnumerable<T> entries, string what)
    {
        try
        {
            int count = 0;
            foreach (T _ in entries)
                count++;
            return count;
        }
        catch (Exception ex) when (ex is InvalidProtocolBufferException or InvalidDataException)
        {
            throw new KahunaServerException($"ImportPartitionState: corrupt snapshot at {what} — {ex.Message}");
        }
    }

    /// <summary>Position and length of one store payload inside the staged snapshot; a zero length is an absent field.</summary>
    private readonly record struct PayloadRange(long Offset, long Length);

    private readonly record struct StoreSectionLayout(PayloadRange Receipts, PayloadRange Records, PayloadRange Intents);

    /// <summary>
    /// Walks the length-delimited <see cref="PartitionStateStoreSection"/> at the current position without
    /// materialising it: records where each payload field lies, reads the checksum, verifies it by hashing the
    /// payloads in place, and leaves the stream at the section's end. Parsing the section as a message would
    /// copy every payload into one contiguous array — the transaction-record payload alone is hundreds of
    /// megabytes on a busy partition. Field handling matches the generated parser: a repeated scalar field keeps
    /// its last occurrence, and a known field number carried with a different wire type is an unknown field.
    /// </summary>
    private static StoreSectionLayout ReadStoreSectionLayout(Stream input)
    {
        const string what = "store section";

        ulong declared = ReadVarint(input, what);
        long start = input.Position;

        if (declared > (ulong)(input.Length - start))
            throw new KahunaServerException($"ImportPartitionState: truncated snapshot stream at {what} — declares {declared} bytes, {input.Length - start} remain.");

        long end = start + (long)declared;

        PayloadRange receipts = default, records = default, intents = default;
        ulong checksum = 0;

        while (input.Position < end)
        {
            ulong tag = ReadVarint(input, what);
            int fieldNumber = (int)(tag >> 3);
            WireFormat.WireType wireType = (WireFormat.WireType)(tag & 7);

            if (fieldNumber == 0)
                throw new KahunaServerException($"ImportPartitionState: corrupt snapshot at {what} — invalid field tag.");

            switch (wireType)
            {
                case WireFormat.WireType.LengthDelimited:
                {
                    ulong size = ReadVarint(input, what);
                    long offset = input.Position;

                    if (size > (ulong)(end - offset))
                        throw new KahunaServerException($"ImportPartitionState: corrupt snapshot at {what} — field {fieldNumber} overruns the section.");

                    PayloadRange range = new(offset, (long)size);

                    if (fieldNumber == PartitionStateStoreSection.CompletionReceiptsFieldNumber)
                        receipts = range;
                    else if (fieldNumber == PartitionStateStoreSection.TransactionRecordsFieldNumber)
                        records = range;
                    else if (fieldNumber == PartitionStateStoreSection.PreparedIntentsFieldNumber)
                        intents = range;

                    input.Position = offset + (long)size;
                    break;
                }

                case WireFormat.WireType.Varint:
                {
                    ulong value = ReadVarint(input, what);
                    if (fieldNumber == PartitionStateStoreSection.ChecksumFieldNumber)
                        checksum = value;
                    break;
                }

                case WireFormat.WireType.Fixed64:
                    SkipWithin(input, 8, end, what);
                    break;

                case WireFormat.WireType.Fixed32:
                    SkipWithin(input, 4, end, what);
                    break;

                default:
                    throw new KahunaServerException($"ImportPartitionState: corrupt snapshot at {what} — unsupported wire type {wireType}.");
            }
        }

        if (input.Position != end)
            throw new KahunaServerException($"ImportPartitionState: corrupt snapshot at {what} — a field overruns the section.");

        // FNV-1a 64 over the three payloads in field order (checksum field excluded), as the exporter wrote it.
        KvStateMachineTransfer.FnvHashStream hasher = new();
        byte[] buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(SegmentedBufferStream.SegmentSize);
        try
        {
            HashRange(input, receipts, hasher, buffer);
            HashRange(input, records, hasher, buffer);
            HashRange(input, intents, hasher, buffer);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }

        input.Position = end;

        if (hasher.Hash != checksum)
            throw new KahunaServerException(
                $"ImportPartitionState: store section checksum mismatch (expected {hasher.Hash}, got {checksum}) — corrupt snapshot.");

        return new StoreSectionLayout(receipts, records, intents);
    }

    private static void HashRange(Stream input, PayloadRange range, KvStateMachineTransfer.FnvHashStream hasher, byte[] buffer)
    {
        input.Position = range.Offset;
        long remaining = range.Length;

        while (remaining > 0)
        {
            int read = input.Read(buffer, 0, (int)Math.Min(buffer.Length, remaining));
            if (read <= 0)
                throw new KahunaServerException("ImportPartitionState: truncated snapshot stream at store section.");

            hasher.Write(buffer.AsSpan(0, read));
            remaining -= read;
        }
    }

    private static void SkipWithin(Stream input, int count, long end, string what)
    {
        if (end - input.Position < count)
            throw new KahunaServerException($"ImportPartitionState: corrupt snapshot at {what} — a field overruns the section.");

        input.Position += count;
    }

    private static ulong ReadVarint(Stream input, string what)
    {
        ulong result = 0;

        for (int shift = 0; shift < 64; shift += 7)
        {
            int b = input.ReadByte();
            if (b < 0)
                throw new KahunaServerException($"ImportPartitionState: truncated snapshot stream at {what}.");

            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
        }

        throw new KahunaServerException($"ImportPartitionState: corrupt snapshot at {what} — malformed varint.");
    }

    /// <summary>
    /// A read-only window onto one store payload of the staged snapshot, so a store decoder reads exactly that
    /// payload straight out of the staged buffer. Each read positions the underlying stream itself, so windows
    /// never depend on where a previous reader left it; only one window is read at a time.
    /// </summary>
    private sealed class PayloadStream(Stream inner, PayloadRange range) : Stream
    {
        private long position;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => range.Length;

        public override long Position
        {
            get => position;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            long remaining = range.Length - position;
            if (remaining <= 0 || buffer.IsEmpty)
                return 0;

            if (buffer.Length > remaining)
                buffer = buffer[..(int)remaining];

            inner.Position = range.Offset + position;
            int read = inner.Read(buffer);
            if (read <= 0)
                throw new KahunaServerException("ImportPartitionState: truncated snapshot stream inside a store payload.");

            position += read;
            return read;
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
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

            Func<string, bool> isOwned = OwnedKeyPredicate(currentMap(), partitionId);

            completionReceiptStore.PurgeWhere(isOwned);
            transactionRecordStore.PurgeWhere(isOwned);
            preparedIntentStore.PurgeWhere(isOwned);
            preparedIntentStore.PurgePartitionLedger(partitionId);

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

    /// <summary>The ownership filter both the install and the un-host purge scope their durable-store purges
    /// with: a key (or record anchor) the given map assigns to <paramref name="partitionId"/>.</summary>
    private Func<string, bool> OwnedKeyPredicate(RangeMap map, int partitionId) =>
        key => PartitionDataEnumerator.OwnerOfKey(map, key, hashPoolSize) == partitionId;

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
        // One coded stream per page, for the same reason as KvStateMachineTransfer.ChecksumOf.
        KvStateMachineTransfer.FnvHashStream hasher = new();
        using CodedOutputStream output = new(hasher, leaveOpen: true);
        foreach (PartitionStateLockEntry entry in entries)
            entry.WriteTo(output);
        output.Flush();
        return hasher.Hash;
    }
}
