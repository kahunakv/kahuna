using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Moves a key range's state between partitions: the live range locks held in the actors, the durable-2PC
/// metadata (transaction records, prepared intents, participant completion receipts), and the key-value
/// pages themselves. This is the data path a split, a merge, or a replica seeding walks.
///
/// Both directions of each transfer live here — reading the local side, and replicating or forwarding it to
/// the destination partition's leader — because a half-applied transfer is the failure mode that matters and
/// the compensating action belongs next to the action.
/// </summary>
internal sealed class RangeStateTransferService
{
    private readonly KeyValuesRuntime runtime;

    private readonly KeyValuesManager manager;

    internal RangeStateTransferService(KeyValuesRuntime runtime, KeyValuesManager manager)
    {
        this.runtime = runtime;
        this.manager = manager;
    }

    /// <summary>
    /// Test-only injection point: when set and it returns true for a destination partition,
    /// <see cref="ImportCompletionReceiptsReplicated"/> reports failure without replicating, simulating a
    /// split/merge receipt handoff that could not be made durable so cutover must abort. Never wired in
    /// production paths.
    /// </summary>
    internal Func<int, bool>? ReplicateReceiptImportFault { get; set; }

    /// <summary>
    /// Test-only injection point: when set and it returns true for a participant partition,
    /// <see cref="ForgetCompletionReceiptsReplicated"/> reports failure without replicating, simulating a receipt
    /// forget that could not be made durable so the decision must keep the participant unreleased. Invoked exactly
    /// once per replicated forget, so a hook that always returns false also serves as the observation point for how
    /// many forget replications a GC pass issues. Never wired in production paths.
    /// </summary>
    internal Func<int, bool>? ReplicateReceiptForgetFault { get; set; }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private IInterNodeCommunication interNodeCommunication => runtime.InterNodeCommunication;

    private IPersistenceBackend persistenceBackend => runtime.PersistenceBackend;

    private KeyValueLocator locator => runtime.Locator;

    private RangeMapStore rangeMapStore => runtime.RangeMapStore;

    private KeySpaceRegistry keySpaceRegistry => runtime.KeySpaceRegistry;

    private SnapshotFloorStore snapshotFloorStore => runtime.SnapshotFloorStore;

    private CompletionReceiptStore completionReceiptStore => runtime.CompletionReceiptStore;

    private TransactionRecordStore transactionRecordStore => runtime.TransactionRecordStore;

    private PreparedIntentStore preparedIntentStore => runtime.PreparedIntentStore;

    private Writes.PartitionWriteAggregator writeAggregator => runtime.WriteAggregator;

    private IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter => runtime.BackgroundWriter;

    private KeyValueActorRing ephemeralKeyValuesRouter => runtime.Routers.Ephemeral;

    private KeyValueActorRing persistentKeyValuesRouter => runtime.Routers.Persistent;

    private Writes.DurableReplicationGateway durableReplication => runtime.DurableReplication;

    private KvStateMachineTransfer kvStateMachineTransfer => manager.KvStateMachineTransfer;

    private Task<bool> ReplicateDurableThroughScheduler(int partitionId, string logType, byte[] data, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken) =>
        runtime.DurableReplication.ReplicateDurableThroughScheduler(partitionId, logType, data, admissionClass, cancellationToken);

    private Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) =>
        manager.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);

    private static ValueTask<KeyValueResponse?> AskKeyValueActor(
        KeyValueActorRing router,
        KeyValueRequest request) => KeyValueActorRouters.AskKeyValueActor(router, request);

    /// <summary>
    /// Returns a snapshot of all live range-lock entries stored in the local actor for
    /// <paramref name="keySpace"/>. Used by <c>KvStateMachineTransfer</c> to read lock
    /// state before serializing it into a range-snapshot stream.
    /// <para>
    /// <b>Persistent-only assumption.</b> Range locks for key-range spaces are always acquired
    /// through the persistent router (the split/merge path only applies to persistent spaces —
    /// ephemeral data is not transferable). Ephemeral range locks, if any exist, are held in
    /// the ephemeral router's actor pool and are not returned here. Split/merge callers must not
    /// rely on this method for ephemeral key spaces.
    /// </para>
    /// </summary>
    internal async Task<List<KeyValueRangeLock>> GetRangeLocksAsync(string keySpace)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.GetRangeLocks,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            keySpace,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 0, default);

        try
        {
            KeyValueResponse? response = await AskKeyValueActor(persistentKeyValuesRouter, request);
            return response?.RangeLockList ?? [];
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Injects <paramref name="locks"/> directly into the local actor's <c>LocksByRange</c>
    /// for <paramref name="keySpace"/> — no conflict checks, no acquire logic. Entries that
    /// duplicate an already-held lock (same tx + overlapping bounds) are silently skipped.
    /// Used by <c>KvStateMachineTransfer</c> to restore clamped locks into a destination
    /// partition after a split or merge.
    /// <para>
    /// <b>Persistent-only assumption.</b> Routes to the persistent actor pool for the same
    /// reason as <see cref="GetRangeLocksAsync"/> — applies only to persistent key-range
    /// spaces. Ephemeral range locks are not injected and do not need to be transferred.
    /// </para>
    /// </summary>
    internal async Task ImportRangeLocksAsync(string keySpace, List<KeyValueRangeLock> locks)
    {
        if (locks.Count == 0)
            return;

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.ImportRangeLocks,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            keySpace,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 0, default);

        request.RangeLockImportList = locks;

        try
        {
            await AskKeyValueActor(persistentKeyValuesRouter, request);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Returns the live range locks held in the actor pool for <paramref name="keySpace"/>
    /// on the leader of <paramref name="partitionId"/>. Forwards via IPC when this node is
    /// not the leader.
    /// </summary>
    internal async Task<List<KeyValueRangeLock>> GetRangeLocksFromPartitionLeaderAsync(
        string keySpace,
        int partitionId,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
            return await GetRangeLocksAsync(keySpace).ConfigureAwait(false);

        // A partition this node does not host resolves through the placement-safe funnel; an
        // unroutable target answers "no locks" rather than throwing — the split/merge lock
        // transfer is best-effort with a post-cutover confirm loop.
        string? leader = await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is null)
            return [];
        if (leader == raft.GetLocalEndpoint())
            return await GetRangeLocksAsync(keySpace).ConfigureAwait(false);

        return await interNodeCommunication.GetRangeLocks(leader, keySpace, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Injects <paramref name="locks"/> into the actor pool for <paramref name="keySpace"/>
    /// on the leader of <paramref name="partitionId"/>. Forwards via IPC when this node is
    /// not the leader.
    /// </summary>
    internal async Task ImportRangeLocksToPartitionLeaderAsync(
        string keySpace,
        int partitionId,
        List<KeyValueRangeLock> locks,
        CancellationToken cancellationToken)
    {
        if (locks.Count == 0)
            return;

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
        {
            await ImportRangeLocksAsync(keySpace, locks).ConfigureAwait(false);
            return;
        }

        // Placement-safe resolution; an unroutable target skips the inject — the post-cutover
        // confirm-and-reimport loop re-drives any lock that did not land.
        string? leader = await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is null)
            return;
        if (leader == raft.GetLocalEndpoint())
        {
            await ImportRangeLocksAsync(keySpace, locks).ConfigureAwait(false);
            return;
        }

        await interNodeCommunication.ImportRangeLocks(leader, keySpace, locks, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Completion receipts whose key falls in <c>[startKey, endKey)</c> from this node's local store.
    /// Read locally (the receipt store is node-global, like the backend the range export reads).
    /// </summary>
    internal IReadOnlyCollection<CompletionReceiptRecord> GetLocalCompletionReceiptsForRange(string? startKey, string? endKey)
        => completionReceiptStore.SnapshotRange(startKey, endKey);

    /// <summary>Canonical transaction records whose anchor moves into <c>[startKey, endKey)</c> — the durable
    /// outcomes a split/merge must hand to the destination partition.</summary>
    internal IReadOnlyList<Transactions.Data.TransactionRecord> GetLocalTransactionRecordsForRange(string? startKey, string? endKey)
        => transactionRecordStore.SnapshotRange(startKey, endKey);

    /// <summary>Prepared intents whose key moves into <c>[startKey, endKey)</c> — the unresolved 2PC state a
    /// split/merge must hand to the destination partition.</summary>
    internal IReadOnlyList<Transactions.Data.PreparedIntent> GetLocalPreparedIntentsForRange(string? startKey, string? endKey)
        => preparedIntentStore.SnapshotRange(startKey, endKey);

    /// <summary>
    /// Replicates moved canonical records and prepared intents onto the destination partition through the ordered
    /// durable seam (which forwards to a remote destination leader), reconstructing them via the ordinary
    /// deterministic apply so every replica of the destination range holds them after cutover. Returns whether the
    /// whole handoff was durable; a false return must abort the split/merge cutover, so unresolved 2PC state is
    /// never stranded on a retired partition. Empty inputs are a no-op success.
    /// </summary>
    internal async Task<bool> ImportDurableTransactionStateToPartitionLeaderAsync(
        int partitionId,
        IReadOnlyList<Transactions.Data.TransactionRecord> records,
        IReadOnlyList<Transactions.Data.PreparedIntent> intents,
        CancellationToken cancellationToken)
    {
        if (records.Count > 0)
        {
            byte[] recordDelta = TransactionRecordStore.SerializeReconstructionDelta(records);
            // Topology-transfer imports must land during cutover regardless of local write pressure — admit as
            // Terminal so an ordinary-write burst on the destination cannot reject the handoff.
            if (!await ReplicateDurableThroughScheduler(partitionId, ReplicationTypes.TransactionRecord, recordDelta, Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false))
                return false;
        }

        if (intents.Count > 0)
        {
            byte[] intentDelta = PreparedIntentStore.SerializeDelta(
                intents.Select(i => (Transactions.Data.PreparedIntentCommand)new Transactions.Data.PrepareIntentCommand(i)));
            if (!await ReplicateDurableThroughScheduler(partitionId, ReplicationTypes.PreparedIntent, intentDelta, Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false))
                return false;
        }

        return true;
    }

    /// <summary>Records transferred completion receipts into this node's local store (state-transfer seeding).</summary>
    internal void ImportCompletionReceipts(IReadOnlyCollection<CompletionReceiptRecord> receiptsToImport)
        => completionReceiptStore.ImportRange(receiptsToImport);

    // ── split/merge data movement through partition leaders ─────────────────────────────

    /// <summary>Entries per copied page — the same bound the range transfer's export uses.</summary>
    private const int RangeCopyPageSize = 256;

    private const int RangeCopyMaxAttempts = 10;

    private const int RangeCopyRetryDelayMs = 200;

    /// <summary>Whether the partition has a committed replica set (per-partition placement); an empty
    /// set is legacy full replication, where every node holds every partition's data locally.</summary>
    private bool IsPlacedPartition(int partitionId) => raft.GetPartitionReplicas(partitionId).Count > 0;

    /// <summary>
    /// Copies <c>[startKey, endKey)</c> of <paramref name="keySpace"/> at the MVCC snapshot
    /// <paramref name="snapshotTs"/> from the source range into the destination partition — the
    /// split/merge bulk and catch-up copy. Under legacy full replication (neither partition has a
    /// committed replica set) this is the historical local export/import: every node holds the
    /// data, so reading and writing the local backend is exact. Under placement the driving node
    /// may host neither side, so the copy pages the range through the locator (which routes each
    /// read to the source partition's leader) and replicates every page onto the destination
    /// partition's Raft log via its leader — every replica of the destination applies the entries
    /// through the ordinary consumer-apply path, a destination-leader change mid-copy loses
    /// nothing, and a replica that is down catches up from the retained log. Returns false when
    /// the copy could not complete; the caller must abort before cutover.
    /// <para>
    /// <paramref name="readerTransactionId"/> is the identity the pages are read under. The
    /// split's catch-up copy runs while its quiesce range lock has stamped a write intent on every
    /// resident key of the range; a foreign snapshot read meeting those live intents answers
    /// MustRetry forever, so the catch-up must read as the lock's owner. Zero for reads outside a
    /// quiesce window (the bulk copy, the merge).
    /// </para>
    /// </summary>
    internal async Task<bool> CopyRangeToPartitionAsync(
        string keySpace,
        string? startKey,
        string? endKey,
        HLCTimestamp snapshotTs,
        int sourcePartitionId,
        int destinationPartitionId,
        HLCTimestamp readerTransactionId,
        CancellationToken cancellationToken)
    {
        if (!IsPlacedPartition(sourcePartitionId) && !IsPlacedPartition(destinationPartitionId))
        {
            // The reader identity matters here exactly as on the paged path below: a split's catch-up
            // export runs under its own quiesce range lock, and a tx-zero read meeting the lock's
            // write intents would answer MustRetry for every resident key of the range.
            Stream snapshot = await kvStateMachineTransfer.ExportRangeAsync(
                keySpace, startKey, endKey, snapshotTs, KeyValueDurability.Persistent, cancellationToken,
                readerTransactionId).ConfigureAwait(false);

            await kvStateMachineTransfer.ImportRangeAsync(snapshot, cancellationToken).ConfigureAwait(false);
            return true;
        }

        string? cursorKey = startKey;
        bool cursorInclusive = true;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            KeyValueGetByRangeResult? page = null;
            KeyValueResponseType lastType = KeyValueResponseType.MustRetry;

            for (int attempt = 0; attempt < RangeCopyMaxAttempts; attempt++)
            {
                KeyValueGetByRangeResult candidate = await LocateAndGetByRange(
                    readerTransactionId, keySpace,
                    cursorKey, cursorInclusive,
                    endKey, false,
                    RangeCopyPageSize, snapshotTs,
                    KeyValueDurability.Persistent, cancellationToken).ConfigureAwait(false);

                lastType = candidate.Type;

                if (candidate.Type is KeyValueResponseType.Get)
                {
                    page = candidate;
                    break;
                }

                if (candidate.Type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication))
                {
                    logger.LogWarning(
                        "Range copy read failed KeySpace={KeySpace} Type={Type}", keySpace, candidate.Type);
                    return false;
                }

                await Task.Delay(RangeCopyRetryDelayMs, cancellationToken).ConfigureAwait(false);
            }

            if (page is null)
            {
                logger.LogWarning(
                    "Range copy read did not settle KeySpace={KeySpace} LastType={Type}", keySpace, lastType);
                return false;
            }

            if (page.Items.Count > 0)
            {
                using MemoryStream frame = new();
                KvStateMachineTransfer.WritePage(frame, page.Items, hasMore: false);

                if (!await ReplicateKeyValueRangePageToPartitionLeaderAsync(
                        destinationPartitionId, frame.ToArray(), cancellationToken).ConfigureAwait(false))
                    return false;
            }

            if (!page.HasMore || page.Items.Count == 0)
                return true;

            cursorKey = page.Items[^1].Item1;   // resume strictly after the last key
            cursorInclusive = false;
        }
    }

    /// <summary>
    /// Replicates one checksummed page of moved key-values onto <paramref name="partitionId"/>'s
    /// Raft log via its leader, forwarding over IPC when the leader is remote. Bounded retries on
    /// an unresolvable leader; false means the page is not durable and the caller must abort.
    /// </summary>
    internal async Task<bool> ReplicateKeyValueRangePageToPartitionLeaderAsync(
        int partitionId, byte[] page, CancellationToken cancellationToken)
    {
        for (int attempt = 0; attempt < RangeCopyMaxAttempts; attempt++)
        {
            if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
            {
                if (await ReplicateKeyValueRangePageLocal(partitionId, page, cancellationToken).ConfigureAwait(false))
                    return true;
            }
            else
            {
                string? leader = await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);

                if (leader is not null)
                {
                    bool replicated = leader == raft.GetLocalEndpoint()
                        ? await ReplicateKeyValueRangePageLocal(partitionId, page, cancellationToken).ConfigureAwait(false)
                        : await interNodeCommunication.ReplicateKeyValueRangePage(leader, partitionId, page, cancellationToken).ConfigureAwait(false);

                    if (replicated)
                        return true;
                }
            }

            await Task.Delay(RangeCopyRetryDelayMs, cancellationToken).ConfigureAwait(false);
        }

        return false;
    }

    /// <summary>
    /// Applies one checksummed page of moved key-values on this node by replicating each entry as
    /// an ordinary committed key-value log record on <paramref name="partitionId"/> — one atomic
    /// batched proposal through the write aggregator, admitted as terminal so ordinary write
    /// pressure on the destination cannot reject a cutover-gating copy. The entries then apply on
    /// every replica through the standard consumer-apply path with full wire fidelity (revision,
    /// timestamps, tombstone state). Re-replicating the same page (a retry after an ambiguous
    /// failure) converges: applies are newest-wins by revision and the backend upsert is
    /// idempotent. Returns false when the page is corrupt or the proposal did not commit.
    /// </summary>
    public async Task<bool> ReplicateKeyValueRangePageLocal(int partitionId, byte[] page, CancellationToken cancellationToken)
    {
        Kahuna.Server.Replication.Protos.RangeSnapshotPage? parsed;
        try
        {
            parsed = Kahuna.Server.Replication.Protos.RangeSnapshotPage.Parser.ParseDelimitedFrom(new MemoryStream(page));
        }
        catch (Google.Protobuf.InvalidProtocolBufferException ex)
        {
            logger.LogWarning("Range-copy page is corrupt: {Message}", ex.Message);
            return false;
        }

        if (parsed is null || KvStateMachineTransfer.ChecksumOf(parsed.Entries) != parsed.Checksum)
        {
            logger.LogWarning("Range-copy page failed checksum verification for partition {Partition}", partitionId);
            return false;
        }

        if (parsed.Entries.Count == 0)
            return true;

        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        List<RaftProposalEntry> entries = new(parsed.Entries.Count);

        foreach (Kahuna.Server.Replication.Protos.RangeSnapshotEntry entry in parsed.Entries)
        {
            KeyValueState state = (KeyValueState)entry.State;

            Kahuna.Server.Replication.Protos.KeyValueMessage kvm = new()
            {
                Type = (int)(state == KeyValueState.Deleted ? KeyValueRequestType.TryDelete : KeyValueRequestType.TrySet),
                Key = entry.Key,
                Revision = entry.Revision,
                ExpireNode = entry.ExpiresNode,
                ExpirePhysical = entry.ExpiresPhysical,
                ExpireCounter = entry.ExpiresCounter,
                LastUsedNode = entry.LastUsedNode,
                LastUsedPhysical = entry.LastUsedPhysical,
                LastUsedCounter = entry.LastUsedCounter,
                LastModifiedNode = entry.LastModifiedNode,
                LastModifiedPhysical = entry.LastModifiedPhysical,
                LastModifiedCounter = entry.LastModifiedCounter,
                TimeNode = currentTime.N,
                TimePhysical = currentTime.L,
                TimeCounter = currentTime.C
                // Transaction identity intentionally absent: the copied value is a terminal committed
                // state whose completion receipt travels in the separate receipt handoff — carrying the
                // identity here would re-record receipts on apply.
            };

            if (entry.HasValue)
                kvm.Value = entry.Value;

            entries.Add(new RaftProposalEntry(
                ReplicationTypes.KeyValues,
                ReplicationSerializer.Serialize(kvm),
                AutoCommit: true,
                ExpectedGeneration: 0));
        }

        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Writes.DurableProposalSubmission submission = new(
            partitionId,
            entries,
            completion,
            Writes.WriteAdmissionClass.Terminal,
            durableReplication.ApplyDurableEntriesOnCommit,
            fenceKey: null,
            fenceGeneration: 0);

        if (!writeAggregator.TryEnqueue(submission))
            return false;

        using CancellationTokenRegistration _ = cancellationToken.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetResult(false), completion);
        return await submission.Committed.ConfigureAwait(false);
    }

    // Items per gather page. The number of receipts/records/intents in a busy range has no ceiling,
    // so an unpaged response can exceed the transport's message limit (gRPC defaults to 4 MB) and
    // fail the whole gather with ResourceExhausted; paging bounds each response instead. Sized so a
    // page of intents — the heaviest kind, each carrying a full value — stays far under the limit
    // for ordinary value sizes.
    private const int TransactionStatePageSize = 512;

    /// <summary>
    /// Answers one page of the transaction state a moving key range carries — completion receipts,
    /// and the serialized canonical transaction records and prepared intents whose key/anchor falls
    /// in <c>[startKey, endKey)</c> — but only when this node holds confirmed leadership of the
    /// source partition: a follower's stores can lag the newest prepared intent, and a handoff
    /// missing an intent would strand its transaction after cutover. A false answer means "not the
    /// leader, route elsewhere", never "no state".
    ///
    /// <para><paramref name="kinds"/> selects which kinds the answer carries. A positive
    /// <paramref name="maxItems"/> pages the answer and requires exactly one kind (a shared cursor
    /// cannot page heterogeneous kinds): the page resumes strictly after <paramref name="cursor"/>,
    /// cuts at a key boundary so items that share one range key never straddle pages, and reports
    /// the resume key in <c>NextCursor</c> when more remain. <paramref name="maxItems"/> of zero
    /// answers everything in one response — the pre-paging behaviour.</para>
    /// </summary>
    public async Task<(bool Ok, List<CompletionReceiptRecord> Receipts, byte[] TransactionRecords, byte[] PreparedIntents, bool HasMore, string? NextCursor)> GetRangeTransactionStateLocal(
        int partitionId, string? startKey, string? endKey, KeyValueRangeStateKinds kinds, string? cursor, int maxItems, CancellationToken cancellationToken)
    {
        if (!await raft.ConfirmLeadershipIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
            return (false, [], [], [], false, null);

        if (kinds == 0)
            kinds = KeyValueRangeStateKinds.All;

        // Paging needs one unambiguous key domain for the cursor; a multi-kind page has none.
        bool paged = maxItems > 0 && kinds is KeyValueRangeStateKinds.Receipts or KeyValueRangeStateKinds.Records or KeyValueRangeStateKinds.Intents;

        List<CompletionReceiptRecord> receipts = [];
        IReadOnlyList<TransactionRecord> records = [];
        IReadOnlyList<PreparedIntent> intents = [];
        bool hasMore = false;
        string? nextCursor = null;

        if (kinds.HasFlag(KeyValueRangeStateKinds.Receipts))
        {
            IReadOnlyCollection<CompletionReceiptRecord> matched = completionReceiptStore.SnapshotRange(startKey, endKey);
            if (paged)
                (receipts, hasMore, nextCursor) = PageByKey(matched, static r => r.Key, cursor, maxItems);
            else
                receipts = [.. matched];
        }

        if (kinds.HasFlag(KeyValueRangeStateKinds.Records))
        {
            IReadOnlyList<TransactionRecord> matched = transactionRecordStore.SnapshotRange(startKey, endKey);
            if (paged)
                (records, hasMore, nextCursor) = PageByKey(matched, static r => r.RecordAnchorKey, cursor, maxItems);
            else
                records = matched;
        }

        if (kinds.HasFlag(KeyValueRangeStateKinds.Intents))
        {
            IReadOnlyList<PreparedIntent> matched = preparedIntentStore.SnapshotRange(startKey, endKey);
            if (paged)
                (intents, hasMore, nextCursor) = PageByKey(matched, static i => i.Key, cursor, maxItems);
            else
                intents = matched;
        }

        return (
            true,
            receipts,
            records.Count > 0 ? TransactionRecordStore.SerializeRecords(records) : [],
            intents.Count > 0 ? PreparedIntentStore.SerializeIntents(intents) : [],
            hasMore,
            nextCursor);
    }

    /// <summary>
    /// Cuts one page out of an unordered range snapshot: items strictly after <paramref name="cursor"/>
    /// by their range key (ordinal), sorted, cut at a key boundary at or past <paramref name="maxItems"/>.
    /// Whole key-groups only — the resume is strictly-after by key, so an item of a split key would
    /// otherwise be skipped by the next page. A page therefore always carries at least one full key,
    /// even when that key alone exceeds the cap.
    /// </summary>
    private static (List<T> Page, bool HasMore, string? NextCursor) PageByKey<T>(
        IEnumerable<T> matched, Func<T, string> keyOf, string? cursor, int maxItems)
    {
        List<T> eligible = [];

        foreach (T item in matched)
        {
            if (cursor is not null && string.CompareOrdinal(keyOf(item), cursor) <= 0)
                continue;

            eligible.Add(item);
        }

        eligible.Sort((a, b) => string.CompareOrdinal(keyOf(a), keyOf(b)));

        List<T> page = new(Math.Min(eligible.Count, maxItems));

        int index = 0;
        while (index < eligible.Count)
        {
            // Take the whole run of items sharing this key.
            string key = keyOf(eligible[index]);
            int groupEnd = index;
            while (groupEnd < eligible.Count && string.CompareOrdinal(keyOf(eligible[groupEnd]), key) == 0)
                groupEnd++;

            if (page.Count > 0 && page.Count + (groupEnd - index) > maxItems)
                break;

            for (int i = index; i < groupEnd; i++)
                page.Add(eligible[i]);

            index = groupEnd;

            if (page.Count >= maxItems)
                break;
        }

        bool hasMore = index < eligible.Count;
        string? nextCursor = hasMore && page.Count > 0 ? keyOf(page[^1]) : null;

        return (page, hasMore, nextCursor);
    }

    /// <summary>
    /// Gathers the moving range's transaction state from the source partition's leader, forwarding
    /// over IPC when the leader is remote. Used by split/merge when the source range has a
    /// committed replica set, so the gather reads the authoritative stores rather than this node's
    /// possibly-empty local projection. Bounded retries across leader changes.
    ///
    /// <para>Each kind is paged separately with a per-kind key cursor, so no single response can
    /// exceed the transport's message limit however many items the range holds. Cross-page
    /// consistency matches the callers' needs: the settle barrier re-gathers until a clean pass, and
    /// the pre-cutover handoff runs under the quiesce after the barrier confirmed the range clean,
    /// so an item that appears mid-gather is one the presumed-abort recovery already covers.</para>
    /// </summary>
    internal Task<(bool Ok, IReadOnlyCollection<CompletionReceiptRecord> Receipts, IReadOnlyList<TransactionRecord> Records, IReadOnlyList<PreparedIntent> Intents)> GetRangeTransactionStateFromPartitionLeaderAsync(
        int sourcePartitionId, string? startKey, string? endKey, CancellationToken cancellationToken) =>
        GetRangeTransactionStateFromPartitionLeaderAsync(sourcePartitionId, startKey, endKey, KeyValueRangeStateKinds.All, cancellationToken);

    /// <inheritdoc cref="GetRangeTransactionStateFromPartitionLeaderAsync(int, string?, string?, CancellationToken)"/>
    internal async Task<(bool Ok, IReadOnlyCollection<CompletionReceiptRecord> Receipts, IReadOnlyList<TransactionRecord> Records, IReadOnlyList<PreparedIntent> Intents)> GetRangeTransactionStateFromPartitionLeaderAsync(
        int sourcePartitionId, string? startKey, string? endKey, KeyValueRangeStateKinds kinds, CancellationToken cancellationToken)
    {
        List<CompletionReceiptRecord> receipts = [];
        List<TransactionRecord> records = [];
        List<PreparedIntent> intents = [];

        foreach (KeyValueRangeStateKinds kind in (KeyValueRangeStateKinds[])
                 [KeyValueRangeStateKinds.Receipts, KeyValueRangeStateKinds.Records, KeyValueRangeStateKinds.Intents])
        {
            if (!kinds.HasFlag(kind))
                continue;

            string? cursor = null;

            while (true)
            {
                (bool ok, List<CompletionReceiptRecord> pageReceipts, byte[] recordBytes, byte[] intentBytes, bool hasMore, string? nextCursor) =
                    await GetRangeTransactionStatePageAsync(sourcePartitionId, startKey, endKey, kind, cursor, cancellationToken).ConfigureAwait(false);

                if (!ok)
                    return (false, [], [], []);

                receipts.AddRange(pageReceipts);
                if (recordBytes.Length > 0)
                    records.AddRange(TransactionRecordStore.DeserializeRecords(recordBytes));
                if (intentBytes.Length > 0)
                    intents.AddRange(PreparedIntentStore.DeserializeIntents(intentBytes));

                // An old peer answers the whole set unpaged (HasMore false) — the loop ends after one page.
                if (!hasMore || nextCursor is null)
                    break;

                cursor = nextCursor;
            }
        }

        return (true, receipts, records, intents);
    }

    /// <summary>
    /// Fetches one page of one kind from the source partition's confirmed leader, local or remote,
    /// with bounded retries across leader changes. A page from a new leader after a mid-gather
    /// leader change is exact for every item that existed throughout: the stores replicate through
    /// the partition, and the cursor addresses items by range key, not by store position.
    /// </summary>
    private async Task<(bool Ok, List<CompletionReceiptRecord> Receipts, byte[] TransactionRecords, byte[] PreparedIntents, bool HasMore, string? NextCursor)> GetRangeTransactionStatePageAsync(
        int sourcePartitionId, string? startKey, string? endKey, KeyValueRangeStateKinds kind, string? cursor, CancellationToken cancellationToken)
    {
        for (int attempt = 0; attempt < RangeCopyMaxAttempts; attempt++)
        {
            (bool ok, List<CompletionReceiptRecord> receipts, byte[] recordBytes, byte[] intentBytes, bool hasMore, string? nextCursor) =
                await GetRangeTransactionStateLocal(sourcePartitionId, startKey, endKey, kind, cursor, TransactionStatePageSize, cancellationToken).ConfigureAwait(false);

            if (!ok)
            {
                string? leader = await raft.TryResolveLeader(sourcePartitionId, cancellationToken).ConfigureAwait(false);

                if (leader is not null && leader != raft.GetLocalEndpoint())
                    (ok, receipts, recordBytes, intentBytes, hasMore, nextCursor) = await interNodeCommunication.GetRangeTransactionState(
                        leader, sourcePartitionId, startKey, endKey, kind, cursor, TransactionStatePageSize, cancellationToken).ConfigureAwait(false);
            }

            if (ok)
                return (true, receipts, recordBytes, intentBytes, hasMore, nextCursor);

            await Task.Delay(RangeCopyRetryDelayMs, cancellationToken).ConfigureAwait(false);
        }

        return (false, [], [], [], false, null);
    }

    /// <summary>
    /// Replicates moved completion receipts onto the destination partition's Raft log, so every replica of
    /// the destination range holds them and a destination-leader change right after cutover still resolves a
    /// re-commit as <c>Committed</c>. Records them locally on success for immediate read-your-writes (the
    /// committed entry re-records idempotently on this node's own apply and on every follower). Returns whether
    /// the handoff was durable; a false return must abort the split/merge cutover.
    /// </summary>
    internal async Task<bool> ImportCompletionReceiptsReplicated(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToImport,
        CancellationToken cancellationToken)
    {
        if (receiptsToImport.Count == 0)
            return true;

        if (ReplicateReceiptImportFault is not null && ReplicateReceiptImportFault(partitionId))
            return false;

        byte[] data = CompletionReceiptStore.SerializeImport(receiptsToImport, partitionId);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId, ReplicationTypes.CompletionReceipt, data, cancellationToken: cancellationToken).ConfigureAwait(false);

        if (!result.Success)
        {
            logger.LogWarning(
                "Failed to replicate completion-receipt handoff Partition={Partition} Status={Status}",
                partitionId, result.Status);
            return false;
        }

        completionReceiptStore.ImportRange(receiptsToImport);
        return true;
    }

    /// <summary>
    /// Routes moved completion receipts to the leader of <paramref name="partitionId"/> for a replicated
    /// handoff. Forwards via IPC when this node is not the leader. Returns whether the handoff was durable on
    /// the destination partition; used by split/merge to gate cutover.
    /// </summary>
    internal async Task<bool> ImportCompletionReceiptsToPartitionLeaderAsync(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToImport,
        CancellationToken cancellationToken)
    {
        if (receiptsToImport.Count == 0)
            return true;

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
            return await ImportCompletionReceiptsReplicated(partitionId, receiptsToImport, cancellationToken).ConfigureAwait(false);

        // A null target (partition not hosted here and no replica known) reports "not durable";
        // the caller retries the handoff later rather than failing the drive.
        string? leader = await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is null)
            return false;
        if (leader == raft.GetLocalEndpoint())
            return await ImportCompletionReceiptsReplicated(partitionId, receiptsToImport, cancellationToken).ConfigureAwait(false);

        return await interNodeCommunication.ImportCompletionReceipts(leader, partitionId, receiptsToImport, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a receipt <b>forget</b> onto a participant partition's Raft log, so every replica of that
    /// partition drops the proof — not just the node that ran the coordinator drive. Forgets locally on success.
    /// Returns whether the forget was durable; only then may the decision record persist <c>ReceiptReleased</c>.
    /// </summary>
    internal async Task<bool> ForgetCompletionReceiptsReplicated(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToForget,
        CancellationToken cancellationToken)
    {
        if (receiptsToForget.Count == 0)
            return true;

        if (ReplicateReceiptForgetFault is not null && ReplicateReceiptForgetFault(partitionId))
            return false;

        byte[] data = CompletionReceiptStore.SerializeImport(receiptsToForget, partitionId, forget: true);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId, ReplicationTypes.CompletionReceipt, data, cancellationToken: cancellationToken).ConfigureAwait(false);

        if (!result.Success)
        {
            logger.LogWarning(
                "Failed to replicate completion-receipt forget Partition={Partition} Status={Status}",
                partitionId, result.Status);
            return false;
        }

        foreach (CompletionReceiptRecord receipt in receiptsToForget)
            completionReceiptStore.Forget(receipt.TransactionId, receipt.Key);

        return true;
    }

    /// <summary>
    /// Routes a receipt forget to the leader of <paramref name="partitionId"/> for a replicated forget, forwarding
    /// via IPC when this node is not the leader. Returns whether the forget was durable on that partition.
    /// </summary>
    internal async Task<bool> ForgetCompletionReceiptsToPartitionLeaderAsync(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToForget,
        CancellationToken cancellationToken)
    {
        if (receiptsToForget.Count == 0)
            return true;

        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
            return await ForgetCompletionReceiptsReplicated(partitionId, receiptsToForget, cancellationToken).ConfigureAwait(false);

        // A null target (partition not hosted here and no replica known) reports "not durable";
        // the record keeps its receipts and the GC pass retries the forget later.
        string? leader = await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is null)
            return false;
        if (leader == raft.GetLocalEndpoint())
            return await ForgetCompletionReceiptsReplicated(partitionId, receiptsToForget, cancellationToken).ConfigureAwait(false);

        return await interNodeCommunication.ImportCompletionReceipts(leader, partitionId, receiptsToForget, cancellationToken, forget: true).ConfigureAwait(false);
    }

}
