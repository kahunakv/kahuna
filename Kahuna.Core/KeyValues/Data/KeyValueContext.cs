
using Nixie;
using Kommander;
using Kommander.Time;
using Kahuna.Utils;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Nixie.Routers;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueContext
{
    private long approximateStoreBytes;

    // Intrusive doubly-linked LRU list: head = coldest (next eviction candidate),
    // tail = hottest (most recently used). Maintained by InsertStoreEntry (LinkAtTail),
    // RemoveStoreEntry (Unlink), and TouchEntry (move-to-tail on access).
    private KeyValueEntry? lruHead;
    private KeyValueEntry? lruTail;

    // Bounded FIFO of transaction IDs for which this actor made a confirmed terminal decision
    // (Committed or RolledBack). Used by TryCommitMutationsHandler / TryRollbackMutationsHandler
    // to tell "ack-loss re-commit on the same node" from "never prepared here (leader changed
    // between prepare and commit)":
    //   - WriteIntent == null && MVCC gone && WasCommittedHere  → safe Committed (ack-loss)
    //   - WriteIntent == null && MVCC gone && !WasCommittedHere → MustRetry (never prepared here)
    // Prepare state (WriteIntent + MVCC) is set only on the preparing leader's actor and is NOT
    // Raft-replicated, so a new leader genuinely cannot have seen the prepare.
    private const int RecentDecisionCap = 1024;
    private readonly Queue<HLCTimestamp>    recentCommittedOrder    = new();
    private readonly HashSet<HLCTimestamp>  recentCommittedTxns     = new();
    private readonly Queue<HLCTimestamp>    recentRolledBackOrder   = new();
    private readonly HashSet<HLCTimestamp>  recentRolledBackTxns    = new();

    // Min-heap keyed by (Expires, key): yields earliest-expiring entries first.
    // Lazy-deletion: pop and re-validate (key present in Store and Expires matches) before acting.
    private readonly PriorityQueue<string, HLCTimestamp> expiryHeap = new();

    // FIFO of keys whose entry just transitioned to Deleted or Undefined.
    // Lazy-validated on drain: re-check State before evicting.
    private readonly Queue<string> tombstoneQueue = new();

    public IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> ActorContext { get; }
    
    public IActorRef<BackgroundWriterActor, BackgroundWriteRequest> BackgroundWriter { get; }
    
    public IActorRef<BalancingActor<KeyValueProposalActor, KeyValueProposalRequest>, KeyValueProposalRequest> ProposalRouter { get; }

    /// <summary>
    /// Off-mailbox worker router for two-phase-commit Raft round trips (prepare/commit/rollback), so a
    /// participant handler can dispatch its Raft call rather than await it inline on the actor mailbox.
    /// Null only in bare test contexts that never exercise the phase-two dispatch path.
    /// </summary>
    public IActorRef<BalancingActor<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>, KeyValuePhaseTwoRequest>? PhaseTwoRouter { get; }

    public IRaft Raft  { get; }

    public KeySpaceRegistry KeySpaceRegistry { get; }

    public RangeMapStore RangeMapStore { get; }

    public SnapshotFloorStore? SnapshotFloorStore { get; }

    /// <summary>
    /// Node-local persistent-participant completion receipts. Consulted when a re-commit finds the
    /// write intent and MVCC entry gone, to tell a durable ack-loss re-commit (receipt present →
    /// Committed) from a request that never committed here (no receipt → MustRetry).
    /// </summary>
    public CompletionReceiptStore CompletionReceiptStore { get; }

    /// <summary>
    /// The node's durable coordinator decision store. On the anchor key's persistent commit the handler
    /// installs the embedded <c>CommitDecided</c> record here, so the anchor value, its completion receipt,
    /// and the decision record all land from the one committed proposal. Null only in bare test contexts.
    /// </summary>
    public Transactions.CoordinatorDecisionStore? CoordinatorDecisionStore { get; }

    public IPersistenceBackend PersistenceBackend  { get; }

    public BTree<string, KeyValueEntry> Store  { get; }
    
    public Dictionary<string, KeyValueWriteIntent> LocksByPrefix  { get; }

    public Dictionary<string, List<KeyValueRangeLock>> LocksByRange { get; }

    public Dictionary<int, KeyValueProposal> Proposals { get; }

    /// <summary>
    /// In-flight two-phase-commit dispatches whose Raft round trip is running off the mailbox, keyed by
    /// a monotonic <c>phaseTwoId</c>. The completion handler removes an entry on the first completion
    /// for its id; a duplicate completion then finds nothing and no-ops. Mutated only on the actor
    /// thread — no synchronisation required.
    /// </summary>
    internal Dictionary<int, PendingPhaseTwo> PendingPhaseTwos { get; } = new();

    private int phaseTwoIdCounter;

    /// <summary>Allocates the next monotonic <c>phaseTwoId</c> for a phase-two dispatch. Actor-thread only.</summary>
    internal int NextPhaseTwoId() => ++phaseTwoIdCounter;

    /// <summary>
    /// Per-actor map of in-flight backend reads. The key is <c>(key, revision, isExists)</c>,
    /// where negative revisions are sentinels distinguishing the read shape:
    /// <list type="bullet">
    ///   <item>Latest-point TryGet: <c>(key, -1, false)</c></item>
    ///   <item>Latest-point TryExists: <c>(key, -1, true)</c></item>
    ///   <item>By-revision TryGet: <c>(key, revision, false)</c></item>
    ///   <item>By-revision TryExists: <c>(key, revision, true)</c></item>
    ///   <item>Bucket scan (plain, non-transactional, non-snapshot): <c>(prefix, -2, false)</c></item>
    ///   <item>Prefix-from-disk scan (non-snapshot): <c>(prefix, -3, false)</c></item>
    /// </list>
    /// The <c>isExists</c> dimension prevents a TryGet and TryExists for the same key+revision
    /// from coalescing onto a single continuation whose fixed <c>responseType</c> would produce
    /// the wrong shape for one of the callers. Transactional and snapshot scans are never
    /// registered here (they carry a null scan key), so they neither coalesce nor evict a
    /// concurrent plain scan's entry.
    /// Stage 1 registers; stage 3 removes before resolving all waiters.
    /// Mutated only on the actor thread — no synchronisation required.
    /// </summary>
    internal Dictionary<(string Key, long Revision, bool IsExists), Handlers.ReadContinuation> PendingReads { get; } = new();

    public KahunaConfiguration Configuration  { get; }

    public ILogger<IKahuna> Logger  { get; }

    public long ApproximateStoreBytes => approximateStoreBytes;

    public KeyValueEntry? LruHead => lruHead;

    public PriorityQueue<string, HLCTimestamp> ExpiryHeap => expiryHeap;

    public Queue<string> TombstoneQueue => tombstoneQueue;

    public int CollectBatchMax =>
        Configuration.CollectBatchMax > 0
            ? Configuration.CollectBatchMax
            : Math.Max(Configuration.CacheEntriesToRemove, 1);
    
    public KeyValueContext(
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext,
        BTree<string, KeyValueEntry> store,
        Dictionary<string, KeyValueWriteIntent> locksByPrefix,
        Dictionary<string, List<KeyValueRangeLock>> locksByRange,
        Dictionary<int, KeyValueProposal> proposals,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IActorRef<BalancingActor<KeyValueProposalActor, KeyValueProposalRequest>, KeyValueProposalRequest> proposalRouter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KeySpaceRegistry keySpaceRegistry,
        RangeMapStore rangeMapStore,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger,
        SnapshotFloorStore? snapshotFloorStore = null,
        CompletionReceiptStore? completionReceiptStore = null,
        Transactions.CoordinatorDecisionStore? coordinatorDecisionStore = null,
        IActorRef<BalancingActor<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>, KeyValuePhaseTwoRequest>? phaseTwoRouter = null
    )
    {
        ActorContext = actorContext;
        Store = store;
        LocksByPrefix = locksByPrefix;
        LocksByRange = locksByRange;
        Proposals = proposals;
        BackgroundWriter = backgroundWriter;
        ProposalRouter = proposalRouter;
        PhaseTwoRouter = phaseTwoRouter;
        PersistenceBackend = persistenceBackend;
        Raft = raft;
        KeySpaceRegistry = keySpaceRegistry;
        RangeMapStore = rangeMapStore;
        Configuration = configuration;
        Logger = logger;
        SnapshotFloorStore = snapshotFloorStore;
        CompletionReceiptStore = completionReceiptStore ?? new CompletionReceiptStore();
        CoordinatorDecisionStore = coordinatorDecisionStore;
    }

    /// <summary>
    /// Updates LastUsed and repositions the entry at the hot (tail) end of the LRU list.
    /// Call on every read or write that constitutes genuine recency. Idempotent when already
    /// at tail. Safe to call only after the entry has been inserted via InsertStoreEntry.
    /// </summary>
    public void TouchEntry(KeyValueEntry entry, HLCTimestamp lastUsed)
    {
        entry.LastUsed = lastUsed;
        if (entry.StoreKey is null) return; // not linked (disk/scan entry — populateCache:false path)
        if (entry == lruTail) return; // already hottest
        Unlink(entry);
        LinkAtTail(entry);
    }

    private void LinkAtTail(KeyValueEntry entry)
    {
        entry.LruPrev = lruTail;
        entry.LruNext = null;
        if (lruTail is not null)
            lruTail.LruNext = entry;
        else
            lruHead = entry; // first entry in the list
        lruTail = entry;
    }

    private void Unlink(KeyValueEntry entry)
    {
        if (entry.LruPrev is not null)
            entry.LruPrev.LruNext = entry.LruNext;
        else
            lruHead = entry.LruNext; // entry was head

        if (entry.LruNext is not null)
            entry.LruNext.LruPrev = entry.LruPrev;
        else
            lruTail = entry.LruPrev; // entry was tail

        entry.LruPrev = null;
        entry.LruNext = null;
    }

    public void EnqueueExpiry(string key, HLCTimestamp expires)
    {
        if (expires != HLCTimestamp.Zero)
            expiryHeap.Enqueue(key, expires);
    }

    public void EnqueueTombstone(string key) => tombstoneQueue.Enqueue(key);

    public bool IsOverStoreBudget()
    {
        if (Store.Count > Configuration.MaxEntriesPerActor)
            return true;

        // Fold heap/queue node overhead into the byte budget so stale-node accumulation
        // (one new heap node per TryExtend, old node not removable from PriorityQueue) is
        // visible to budget pressure and eventually triggers collection to drain the duplicates.
        // Per-node byte estimates:
        //   expiry heap  ≈ 40 B (HLCTimestamp struct 24 B + string ref 8 B + array slot 8 B)
        //   tombstone queue ≈ 16 B (string ref 8 B + array slot 8 B)
        //
        // Known limitation: duplicate nodes from repeated TryExtend carry future timestamps and
        // are not drainable until their deadlines pass (Step 1b breaks at the first non-elapsed
        // node). During that window this check can stay true and trigger collect cycles that
        // cannot immediately shrink the heap. Steady-state is bounded (≈ renewals-per-TTL-window
        // per key), not unbounded. Real dedup (track latest-expiry per key, drop stale on pop
        // regardless of elapse) is the long-term fix, tracked as a separate follow-up.
        long totalBytes = approximateStoreBytes
            + ((long)expiryHeap.Count * 40)
            + ((long)tombstoneQueue.Count * 16);

        return totalBytes > Configuration.MaxBytesPerActor;
    }

    public void InsertStoreEntry(string key, KeyValueEntry entry)
    {
        if (Store.TryGetValue(key, out KeyValueEntry? existing))
        {
#if DEBUG
            System.Diagnostics.Debug.Assert(
                existing.CachedBytes == KeyValueStoreAccounting.EstimateEntryBytes(key, existing) - (key.Length * sizeof(char)),
                $"CachedBytes drift on replace: key={key}, cached={existing.CachedBytes}, computed={KeyValueStoreAccounting.EstimateEntryBytes(key, existing) - (key.Length * sizeof(char))}");
#endif
            approximateStoreBytes -= (key.Length * sizeof(char)) + existing.CachedBytes;
            if (existing != entry)
                Unlink(existing); // replacing with a new entry object; unlink the old one
        }

        // Initialize CachedBytes the first time an entry enters the store. For new entries
        // (no Revisions, no MvccEntries) EstimateEntryBytes is O(1); subsequent mutations
        // maintain CachedBytes incrementally via AdjustEstimatedEntryBytes / AdjustEntryValueBytes.
        // Guard relies on insert-before-adjust ordering: callers must not call AdjustEstimatedEntryBytes
        // on this entry before InsertStoreEntry, or CachedBytes would be non-zero and skip init.
        if (entry.CachedBytes == 0)
            entry.CachedBytes = KeyValueStoreAccounting.EstimateEntryBytes(key, entry) - (key.Length * sizeof(char));

        entry.StoreKey = key;
        Store.Insert(key, entry);
        approximateStoreBytes += (key.Length * sizeof(char)) + entry.CachedBytes;
        if (entry != existing)   // same guard as Unlink above — symmetric, prevents double-link
            LinkAtTail(entry);

        // Auto-register with the expiry heap and tombstone queue so entries inserted through any
        // path (handlers, disk-load, test helpers) are tracked without requiring callers to enqueue
        // separately. Handlers that later mutate Expires or State on an existing entry still call
        // EnqueueExpiry / EnqueueTombstone explicitly for those in-place transitions.
        //
        // Undefined is intentionally excluded: handlers insert Undefined stubs as cache-miss
        // placeholders (TryGet, TryExists, TryAcquireExclusiveLock) that are usually promoted to
        // Set or Deleted immediately after the disk read completes. Auto-enqueueing them would
        // produce one stale tombstone queue entry per disk-loaded persistent key. Handlers that
        // determine a key is genuinely absent call EnqueueTombstone explicitly.
        if (entry.Expires != HLCTimestamp.Zero)
            expiryHeap.Enqueue(key, entry.Expires);
        if (entry.State is KeyValueState.Deleted)
            tombstoneQueue.Enqueue(key);
    }

    public bool RemoveStoreEntry(string key)
    {
        if (!Store.TryGetValue(key, out KeyValueEntry? entry))
            return false;

        Store.Remove(key);
        approximateStoreBytes -= (key.Length * sizeof(char)) + entry.CachedBytes;
        Unlink(entry);
        entry.StoreKey = null;
        return true;
    }

    public void AdjustEntryValueBytes(KeyValueEntry entry, int previousValueLength, int newValueLength)
    {
        int delta = newValueLength - previousValueLength;
        approximateStoreBytes += delta;
        entry.CachedBytes += delta;
    }

    public void AdjustEstimatedEntryBytes(KeyValueEntry entry, long delta)
    {
        approximateStoreBytes += delta;
        entry.CachedBytes += delta;
    }

    public void ScheduleFollowUpCollect()
    {
        if (ActorContext is null)
            return;

        ActorContext.Self.Send(new KeyValueRequest(KeyValueRequestType.Collect));
    }

    public void RecordCommitted(HLCTimestamp txId)
    {
        if (!recentCommittedTxns.Add(txId))
            return;
        recentCommittedOrder.Enqueue(txId);
        if (recentCommittedOrder.Count > RecentDecisionCap)
            recentCommittedTxns.Remove(recentCommittedOrder.Dequeue());
    }

    public bool WasCommittedHere(HLCTimestamp txId) => recentCommittedTxns.Contains(txId);

    public void RecordRolledBack(HLCTimestamp txId)
    {
        if (!recentRolledBackTxns.Add(txId))
            return;
        recentRolledBackOrder.Enqueue(txId);
        if (recentRolledBackOrder.Count > RecentDecisionCap)
            recentRolledBackTxns.Remove(recentRolledBackOrder.Dequeue());
    }

    public bool WasRolledBackHere(HLCTimestamp txId) => recentRolledBackTxns.Contains(txId);
}
