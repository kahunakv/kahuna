
using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.Server.Configuration;

public sealed class KahunaConfiguration
{
    public string HttpsCertificate { get; set; } = "";
    
    public string HttpsTrustedThumbprint { get; set; } = "";
    
    public string HttpsCertificatePassword { get; set; } = "";

    /// <summary>
    /// URL scheme prepended to bare peer endpoints when this node opens inter-node gRPC channels
    /// (leader forwarding, sequencer allocation). Fed from the same flag as Kommander's Raft channel
    /// scheme so both inter-node layers dial peers the same way. With "http://" the target ports
    /// must be cleartext HTTP/2 listeners; payloads then cross the network unencrypted.
    /// </summary>
    public string InterNodeGrpcScheme { get; set; } = "https://";

    public int LocksWorkers { get; set; }
    
    public int KeyValueWorkers { get; set; }
    
    public int BackgroundWriterWorkers { get; set; }

    /// <summary>
    /// Number of sequence actors. Each sequence name is consistent-hash routed to exactly one of them and
    /// served single-threaded, so this bounds how many distinct sequences can be allocating concurrently.
    /// A value &lt;= 0 auto-sizes.
    /// </summary>
    public int SequencerWorkers { get; set; } = 16;

    /// <summary>
    /// Values a sequence actor reserves from the durable record in a single compare-and-swap. Allocations
    /// inside the block are served from memory with no storage traffic, so this is directly the
    /// amortization factor: one Raft commit per this many values.
    /// <para>The trade is gaps. Whatever is left in a block when the node restarts, loses partition
    /// leadership, or evicts the sequence is never handed out — the same behaviour as a conventional
    /// database sequence cache. Set to <c>1</c> for gap-free allocation at one commit per value.</para>
    /// </summary>
    public int SequencerBlockSize { get; set; } = 1_000;

    /// <summary>
    /// Maximum idempotency entries retained in one sequence's durable record. Entries beyond the cap are
    /// dropped oldest-first whenever the record is written, which is what keeps the record — rewritten on
    /// every ceiling bump — from growing without bound as clients use fresh keys. A value &lt;= 0 disables
    /// the cap, leaving <see cref="SequencerIdempotencyRetentionTtl"/> as the only bound.
    /// </summary>
    public int SequencerIdempotencyRetentionMax { get; set; } = 256;

    /// <summary>
    /// Age after which an idempotency entry is dropped from a sequence's record. This is the window within
    /// which retrying a keyed reserve is guaranteed to replay the identical allocation; a retry after it
    /// has passed allocates fresh values. <see cref="TimeSpan.Zero"/> disables age pruning.
    /// </summary>
    public TimeSpan SequencerIdempotencyRetentionTtl { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Maximum sequences one actor keeps resident. Past the cap the least recently used are evicted,
    /// abandoning their reserved blocks (a gap). A value &lt;= 0 leaves residency unbounded.
    /// </summary>
    public int SequencerMaxSequencesPerActor { get; set; } = 10_000;

    /// <summary>
    /// How long a reserved block may be served purely from memory before the actor revalidates it
    /// against the durable record. Allocations inside the block normally touch no storage, so a node
    /// that lost partition leadership without noticing could keep serving a block belonging to a
    /// sequence that has since been deleted and recreated elsewhere — the revalidation read detects
    /// the new incarnation (or the routed leader change) and voids the stale window, bounding that
    /// exposure to this lease. A value &lt;= 0 disables revalidation.
    /// </summary>
    public TimeSpan SequencerBlockLease { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Number of dedicated worker threads serving Kahuna persistence-backend reads (point gets, scans,
    /// read-before-write). Owned by Kahuna and separate from Kommander's WAL read pool, so data-plane
    /// reads never contend with the WAL reads consensus/replication/recovery depend on. 0 or negative
    /// auto-sizes to the processor count.
    /// </summary>
    public int BackendReadIOThreads { get; set; } = 8;

    /// <summary>
    /// Number of dedicated worker threads serving background batch writes (StoreKeyValues / StoreLocks
    /// and revision pruning). The background writer submits every batch under a single queue key, and the
    /// scheduler runs at most one worker per queue, so 1 is the effective maximum — a larger value only
    /// creates permanently-idle threads. Kept at 1 because these writes are fsync-heavy and serialize on the
    /// backend anyway; isolating them keeps bulk flushes off both the WAL read pool and the backend read
    /// pool. Rejected if 0 or negative (which would otherwise auto-expand the pool to the processor count).
    /// </summary>
    public int BackendWriteIOThreads { get; set; } = 1;

    /// <summary>
    /// Per-partition pending-queue depth for the backend read scheduler before new reads are rejected with
    /// backpressure. Independent of Kommander's WAL read budget.
    /// </summary>
    public int BackendReadQueueDepth { get; set; } = 4096;

    public string Storage { get; set; } = "";
    
    public string StoragePath { get; set; } = "";
    
    public string StorageRevision { get; set; } = "";

    /// <summary>
    /// When true (default), the RocksDB KV/locks backend reads SSTs with direct I/O, bypassing the OS
    /// page cache so the block cache is the sole in-RAM read cache. Disable to use buffered reads
    /// backed by the page cache. Only affects the "rocksdb" storage backend.
    /// </summary>
    public bool RocksDbDirectReads { get; set; } = true;

    /// <summary>
    /// When true, enables RocksDB statistics collection for the KV/locks backend and periodic dumps to
    /// its LOG file. Off by default because statistics add per-operation overhead; enable only for
    /// tuning or diagnosis. Only affects the "rocksdb" storage backend.
    /// </summary>
    public bool RocksDbStatistics { get; set; }

    public TimeSpan ScriptCacheExpiration { get; set; }

    /// <summary>
    /// Maximum number of entries the script cache may hold. New entries are dropped when the limit is reached.
    /// </summary>
    public int ScriptCacheMaxEntries { get; set; } = 1_000;
    
    public int DefaultTransactionTimeout { get; set; } = 5000;

    /// <summary>
    /// Hard upper bound, in milliseconds, on how long any interactive transaction session may live. A
    /// caller-supplied timeout is clamped to this at Begin, so no admitted session can outlive it. This is
    /// the quantity that bounds server-side reclamation of a transaction's orphaned MVCC read snapshots: a
    /// zero-expiry snapshot whose owning transaction started longer ago than this bound (plus the reaper
    /// grace and participant-effect windows) is provably from a dead session and safe to reclaim, regardless
    /// of that session's own timeout. Keep it comfortably above the longest legitimate transaction.
    /// </summary>
    public int MaxTransactionTimeout { get; set; } = 300_000;

    /// <summary>
    /// Default milliseconds a caller queues for an admission slot when it does not ask for a specific budget.
    /// This is the door-wait, not the session lifetime: <see cref="MaxTransactionTimeout"/> bounds how long an
    /// admitted transaction may live, while this bounds how long an unadmitted one waits to begin. The two were
    /// once the same value, which meant a caller asking for a long-lived session also asked — silently — to
    /// park at the gate for that whole span. Kept short by default so a saturated node refuses quickly and the
    /// caller can back off, rather than holding the request open.
    /// </summary>
    public int DefaultAdmissionWaitMs { get; set; } = 5_000;

    /// <summary>
    /// Hard upper bound, in milliseconds, on any admission wait. A caller-supplied budget is clamped to this,
    /// so no caller can occupy a queue slot for longer than the operator allows. This bounds queue
    /// <i>duration</i>, complementing <see cref="TransactionPriorityMaxQueued"/>, which bounds only queue
    /// <i>depth</i> — without it a single patient caller can hold a slot indefinitely while others are refused.
    /// </summary>
    public int MaxAdmissionWaitMs { get; set; } = 30_000;

    /// <summary>
    /// Script transactions that may execute concurrently on this node before further ones are queued and
    /// started in priority order. A value &lt;= 0 disables the gate entirely: every transaction starts
    /// immediately and its priority is recorded for observability only, which is byte-for-byte the behavior
    /// of a build without an admission gate. Default is disabled, so enabling admission control is an
    /// explicit operator decision.
    /// </summary>
    public int MaxConcurrentTransactions { get; set; }

    /// <summary>
    /// Interactive transaction sessions that may be open concurrently on this node. Deliberately governed
    /// separately from <see cref="MaxConcurrentTransactions"/> and expected to be far more generous: a session
    /// holds its slot for as long as the client keeps it open, so a shared pool would let idle client-paced
    /// sessions starve script transactions. A value &lt;= 0 disables the session gate.
    /// </summary>
    public int MaxConcurrentSessions { get; set; }

    /// <summary>
    /// Slots, out of each ceiling above, that only <c>High</c> and <c>Critical</c> transactions may occupy.
    /// This is what stops a flood of bulk work from filling every slot and starving latency-critical work.
    /// Clamped below its ceiling so ordinary traffic always retains at least one slot. Zero — the default —
    /// means no class distinction: whoever is first in line wins.
    /// </summary>
    public int TransactionPriorityReservedSlots { get; set; }

    /// <summary>
    /// Milliseconds a queued transaction must wait to gain one effective priority level, compounding until it
    /// reaches just below <c>Critical</c>. This is the anti-starvation bound: background work queued behind an
    /// unending stream of important work still reaches the front rather than waiting forever. Lower values
    /// favour fairness, higher values favour honouring the stated priorities; a value &lt;= 0 disables aging
    /// and lets high-priority work defer low-priority work indefinitely.
    /// </summary>
    public int TransactionPriorityAgingThreshold { get; set; } = 1_000;

    /// <summary>
    /// Callers that may wait for an admission slot at once, per gate, before further ones are refused with a
    /// retryable result instead of being queued. Without this the ceiling bounds only how much work *runs*
    /// while the queue behind it grows with offered load — retaining a waiter, its continuation, and its
    /// cancellation registration each — which consumes the very memory the ceiling exists to protect, and
    /// does so precisely during the overload it is meant to survive. A value &lt;= 0 leaves the queue
    /// unbounded. Only has an effect when the corresponding ceiling is enabled.
    /// </summary>
    public int TransactionPriorityMaxQueued { get; set; } = 4_096;

    /// <summary>
    /// Upper bound, in milliseconds, on how long a single two-phase-commit <c>CommitLogs</c>/<c>RollbackLogs</c>
    /// Raft wait may block before it is cancelled and returns a retryable <c>OperationCancelled</c>
    /// (mapped to <c>MustRetry</c>) so the coordinator re-drives the same ticket against the settled
    /// partition instead of parking on a stuck leader. A value &lt;= 0 disables the deadline (unbounded wait).
    /// </summary>
    public int Phase2CommitTimeout { get; set; } = 5000;

    /// <summary>
    /// Upper bound, in milliseconds, on how long a resumable backend read (point read, bucket/range
    /// scan) may stay in flight before the periodic collect sweep expires it: its coalesced waiters are
    /// resolved with a retryable <c>MustRetry</c> and a completion arriving afterwards is dropped. This
    /// bounds the blast radius of a hung/slow disk read so it cannot strand callers indefinitely. A
    /// value &lt;= 0 disables the deadline (unbounded wait). Enforcement granularity is one
    /// <c>CollectionInterval</c>.
    /// </summary>
    public int ReadContinuationTimeout { get; set; } = 30_000;

    /// <summary>
    /// Maximum number of <em>ordinary</em> (user-request) messages that may be queued in a single
    /// key-value actor's inbox before further ordinary messages are rejected with a retryable
    /// <c>MustRetry</c> (mapped from Nixie's <c>ActorBusyException</c>). This backpressures a hot-key
    /// flood without ever rejecting control messages — completions, cache-coherence, and maintenance —
    /// which are exempt from the bound and delivered ahead of the backlog. A value &lt;= 0 disables the
    /// bound (unbounded inbox, original behavior). The default is generous so it only engages under a
    /// genuine flood, not normal bursts.
    /// </summary>
    public int MaxKeyValueActorInboxSize { get; set; } = 16_384;

    // ── Partition write aggregator (direct set/delete/extend coalescing) ────────────────────────────────

    /// <summary>Delay, from the oldest queued item, before a partition's accumulated direct writes are
    /// proposed. 0 dispatches an idle partition immediately (still batching work that piles up behind an
    /// in-flight batch); a small positive value trades a little single-write latency for cross-request
    /// coalescing. Zero is the low-latency escape hatch. Must not be negative.</summary>
    public int KeyValueWriteLingerMs { get; set; } = 1;

    /// <summary>Maximum log entries selected for one aggregator Raft call.</summary>
    public int KeyValueWriteMaxBatchItems { get; set; } = 512;

    /// <summary>Target serialized payload bytes per aggregator Raft call; an oversized single item dispatches
    /// alone regardless.</summary>
    public int KeyValueWriteMaxBatchBytes { get; set; } = 4 * 1024 * 1024;

    /// <summary>Maximum admitted items per partition, including those waiting behind an in-flight batch. A
    /// full partition rejects new writes with a retryable MustRetry.</summary>
    public int KeyValueWriteMaxQueuedItemsPerPartition { get; set; } = 8_192;

    /// <summary>Maximum admitted serialized bytes retained per partition, including in flight.</summary>
    public long KeyValueWriteMaxQueuedBytesPerPartition { get; set; } = 32L * 1024 * 1024;

    /// <summary>Extra per-partition item headroom above the per-partition cap reserved for terminal work (a
    /// durable transaction's decision/settle), so a partition saturated with ordinary writes still admits the
    /// step that finishes an already-prepared transaction. Must not be negative.</summary>
    public int KeyValueWriteTerminalReserveItemsPerPartition { get; set; } = 256;

    /// <summary>Extra per-partition byte headroom above the per-partition byte cap reserved for terminal work.</summary>
    public long KeyValueWriteTerminalReserveBytesPerPartition { get; set; } = 4L * 1024 * 1024;

    /// <summary>Node-global maximum admitted items across all partitions for ordinary writes; a burst spread over
    /// many partitions cannot retain unbounded memory in aggregate. A value &lt;= 0 disables the global bound.</summary>
    public long KeyValueWriteMaxQueuedItemsGlobal { get; set; } = 131_072;

    /// <summary>Node-global maximum admitted serialized bytes across all partitions for ordinary writes.
    /// A value &lt;= 0 disables the global byte bound.</summary>
    public long KeyValueWriteMaxQueuedBytesGlobal { get; set; } = 512L * 1024 * 1024;

    /// <summary>Extra node-global item headroom above the global cap reserved for terminal work, so global
    /// ordinary saturation cannot reject settlement anywhere on the node. Must not be negative.</summary>
    public long KeyValueWriteTerminalReserveItemsGlobal { get; set; } = 8_192;

    /// <summary>Extra node-global byte headroom above the global byte cap reserved for terminal work.</summary>
    public long KeyValueWriteTerminalReserveBytesGlobal { get; set; } = 64L * 1024 * 1024;

    /// <summary>Hard ceiling on a single admitted write's serialized bytes; a larger write is rejected with a
    /// retryable MustRetry rather than dispatched alone. Must be &gt;= the batch byte target so a legitimately
    /// large value below the ceiling still dispatches alone. A value &lt;= 0 disables the hard ceiling.</summary>
    public long KeyValueWriteMaxOperationBytes { get; set; } = 64L * 1024 * 1024;

    /// <summary>Maximum wall-clock time a dispatched aggregator batch's Raft round trip may take before the
    /// scheduler cancels it (the cancelled batch settles retryably). Bounds an in-flight batch so it cannot
    /// outlive queue age or hang shutdown. A value &lt;= 0 disables the deadline.</summary>
    public int KeyValueWriteBatchExecutionTimeoutMs { get; set; } = 30_000;

    /// <summary>Maximum time an admitted write may wait before dispatch; on expiry it is released as
    /// MustRetry. Must stay well below the write-intent lease so a released item is never proposed late.</summary>
    public int KeyValueWriteMaxQueueDelayMs { get; set; } = 1_000;

    /// <summary>Ordinary-submission inbox bound per aggregator lane; control messages (timer/completion/stop)
    /// are exempt. A value &lt;= 0 disables the bound.</summary>
    public int MaxKeyValueWriteAggregatorInboxSize { get; set; } = 16_384;

    /// <summary>
    /// Strict upper bound on the number of finalized transaction outcomes retained after their session is
    /// removed from the active map. A duplicate commit/rollback that arrives after the session is gone consults
    /// this retention and receives the same terminal answer (Committed/RolledBack) instead of an unknown result
    /// — the best-effort idempotency window. Beyond this many entries the oldest (by retention time) are evicted
    /// atomically, so the window never exceeds this many entries at rest; a duplicate whose outcome has been
    /// evicted receives an unknown <c>Errored</c>, never a conflict <c>Aborted</c>. A value &lt;= 0 <b>disables
    /// retention entirely</b> — nothing is retained, so every duplicate after removal reports unknown
    /// <c>Errored</c>.
    /// </summary>
    public int TransactionOutcomeRetentionMax { get; set; } = 10_000;

    /// <summary>
    /// Age after which a retained transaction outcome is pruned. This is the duration of the best-effort
    /// idempotency window: within it a duplicate finalize replays the recorded outcome; after it the entry
    /// is gone and a duplicate receives an unknown <c>Errored</c>. Pruned on the reaper's collection sweep.
    /// A value &lt;= 0 disables age pruning, leaving the size cap (<see cref="TransactionOutcomeRetentionMax"/>)
    /// as the only bound.
    /// </summary>
    public TimeSpan TransactionOutcomeRetentionTtl { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Strict upper bound on the number of <b>outstanding</b> durable coordinator decision records — those still
    /// being driven to completion — that this node admits. It gates admission of a new <c>Durable</c> transaction:
    /// a slot is reserved atomically before prepare and released once the transaction's decision is installed or
    /// its attempt ends, so concurrent admissions can never collectively exceed this bound. Only outstanding
    /// (not-yet-<c>Completed</c>) records count against it; completed records held for the idempotency window are
    /// bounded separately by <see cref="TransactionOutcomeRetentionTtl"/> and never consume durable-admission
    /// capacity. This is deliberately decoupled from <see cref="TransactionOutcomeRetentionMax"/> (the best-effort
    /// terminal-outcome cache), so steady durable throughput is not throttled by retained completed outcomes. A
    /// value &lt;= 0 <b>disables</b> the bound (unbounded admission).
    /// </summary>
    public int DurableDecisionOutstandingMax { get; set; } = 100_000;

    /// <summary>
    /// Maximum terminal transaction records the retention GC sweep reclaims in one <i>batch</i>, per node. A sweep
    /// drains the whole eligible backlog every collection tick, processed in batches of at most this many records;
    /// the cap bounds each batch's in-memory receipt/purge structures and each replicated entry's size, never the
    /// sweep's total. (Capping the total was a growth bug: it limited reclamation to cap ÷ collection-interval
    /// records per second, so any workload committing faster grew the store — and the checkpoint cost proportional
    /// to it — without bound.) Because a batch issues one replication per participant partition rather than one
    /// per record, this can be raised well above the count of partitions without multiplying round trips.
    /// A value &lt;= 0 removes the batch bound (the sweep processes everything eligible as one batch).
    /// </summary>
    public int DurableRecordGcMaxPerPass { get; set; } = 4_096;

    /// <summary>
    /// Age after which a participant completion receipt is dropped even though no coordinator acknowledgement
    /// released it. This is the backstop that bounds the receipt store on its own terms rather than only as a side
    /// effect of reclaiming the transaction record that owns it: a WAL replay on cold restart or partition leader
    /// change re-records receipts for transactions whose record was already reclaimed, and nothing would ever
    /// release those.
    ///
    /// <para>The default is twice <see cref="TransactionOutcomeRetentionTtl"/>: enough margin that the ordinary
    /// acknowledgement-driven release always gets to a live receipt first, so this only ever collects genuinely
    /// orphaned ones, and no more — this value is also the worst-case retention floor of the receipt store (a node
    /// committing at a steady rate retains roughly that rate times this window), so raising it costs memory
    /// proportionally. The margin absorbs the fact that age is measured from the receipt's transaction id, which
    /// overstates the true age by up to one transaction's lifetime. A value &lt;= 0 disables the backstop, leaving
    /// orphans retained for the node's lifetime.</para>
    /// </summary>
    public TimeSpan CompletionReceiptRetentionTtl { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Maximum partitions the prepared-intent recovery sweep drives in one collection pass, per node. Bounds the
    /// cross-partition fan-out (and the concurrent recovery lookups it issues) so a restart backlog spread across
    /// many partitions is drained across successive passes instead of one tick fanning out to every partition at
    /// once. A value &lt;= 0 disables the per-pass cap. Due intents on the deferred partitions remain due and are
    /// picked up next pass.
    /// </summary>
    public int DurableRecoveryMaxPartitionsPerPass { get; set; } = 64;

    /// <summary>
    /// When true (default), a durable transaction's post-decision resolution (materialize committed values, settle
    /// intents) runs off the commit critical path: finalize returns as soon as the decision record is durable, and
    /// settlement completes in the background (a lost run is finished by recovery). Reads and writes that meet a
    /// committed-but-unsettled intent resolve the outcome through the durable-intent visibility path — the
    /// canonical record locally, or routed to the anchor leader cross-node — so it never serves a stale value. This
    /// removes settlement from the commit critical path (measured ~+69% committed TPS, −42% commit p50 at 32
    /// workers on one embedded node).
    /// <para>Set false to await resolution inline before finalize returns (synchronous settlement) — the prior
    /// behavior, useful when a consumer wants the committed value materialized into MVCC before the commit returns
    /// on every node without relying on the durable-intent visibility path.</para>
    /// </summary>
    public bool DurableDeferredSettlement { get; set; } = true;

    /// <summary>
    /// When true (default), the post-decision materialization of a committed durable transaction is replicated
    /// BY REFERENCE: the record names the prepared intent (transaction id, epoch, key) instead of carrying the
    /// committed value again. Every replica already holds that value in its prepared-intent store from the
    /// moment the prepare delta applied, so the second copy of every committed value — a second serialization,
    /// a second write-ahead-log append, a second network send, and a second parse on every follower — is
    /// removed from every commit.
    /// <para><b>Rolling upgrade from a build that predates the record.</b> Every node in the cluster must run a
    /// build that APPLIES the by-reference record before any node PRODUCES one: an older node treats the record
    /// as an unknown message type and skips it, which is a silently lost write on that node. So set this to
    /// false on the upgraded nodes for the duration of a mixed-version rollout, then remove the override once
    /// every node runs the new build. Value-carrying records written while it is off apply forever, so nothing
    /// has to be migrated in either direction.</para>
    /// </summary>
    public bool DurableMaterializeByReference { get; set; } = true;

    /// <summary>
    /// Strict upper bound on the number of prepared intents resident across all partitions on this node. Checked
    /// at durable admission: a transaction whose prepares would push the resident count past this bound is
    /// refused with a retryable <c>MustRetry</c> before it prepares, so slow settlement cannot let resident
    /// prepared-intent state grow without limit. Complements <see cref="DurableDecisionOutstandingMax"/> (which
    /// bounds concurrent transactions) by bounding the intents those transactions hold. A value &lt;= 0 disables
    /// the count bound.
    /// </summary>
    public int DurablePreparedIntentMaxCount { get; set; } = 500_000;

    /// <summary>
    /// Strict upper bound on the resident prepared-intent value bytes across all partitions on this node, checked
    /// at durable admission alongside <see cref="DurablePreparedIntentMaxCount"/>. Bounds the memory a burst of
    /// large-value transactions can pin in unsettled intents. A value &lt;= 0 disables the byte bound.
    /// </summary>
    public long DurablePreparedIntentMaxBytes { get; set; } = 1L * 1024 * 1024 * 1024;

    /// <summary>
    /// Lower bound (ms) on the durable-transaction decision-deadline margin — the window past the commit timestamp
    /// within which a durable commit is still authorized before recovery may presume-abort it. The margin is
    /// derived per transaction as <c>clamp(DurableDecisionDeadlineMultiplier × observed-finalize-p99,
    /// floor, ceiling)</c>, so this floor is what a cold or low-latency node uses before enough finalize samples
    /// exist. Set it comfortably above a healthy finalize's two Raft barriers so normal commits are never
    /// presumed-aborted; too low spuriously aborts slow-but-alive coordinators under load.
    /// </summary>
    public long DurableDecisionDeadlineFloorMs { get; set; } = 5_000;

    /// <summary>
    /// Hard upper bound (ms) on the durable-transaction decision-deadline margin. Caps how long a genuinely dead
    /// coordinator's undecided record blocks recovery of its prepared intents, regardless of an anomalous p99.
    /// Must be &gt;= <see cref="DurableDecisionDeadlineFloorMs"/>.
    /// </summary>
    public long DurableDecisionDeadlineCeilingMs { get; set; } = 60_000;

    /// <summary>
    /// Multiplier applied to the observed finalize p99 when deriving the decision-deadline margin, giving healthy
    /// commits headroom above typical latency before the deadline expires. Clamped by the floor and ceiling above.
    /// </summary>
    public int DurableDecisionDeadlineMultiplier { get; set; } = 4;

    public int RevisionsToKeepCached { get; set; }

    public TimeSpan CacheEntryTtl { get; set; }
    
    public int CacheEntriesToRemove { get; set; }

    public TimeSpan CollectionInterval { get; set; } = TimeSpan.FromSeconds(60);

    public int MaxEntriesPerActor { get; set; } = 50_000;

    public long MaxBytesPerActor { get; set; } = 256L * 1024 * 1024;

    public int CollectBatchMax { get; set; } = 1_000;

    public int RevisionRetention { get; set; } = 16;

    public int DirtyObjectsWriterDelay { get; set; }

    /// <summary>
    /// Maximum persisted key/value revision records to keep per key. 0 keeps all revisions forever.
    /// </summary>
    public int PersistentRevisionRetentionCount { get; set; }

    /// <summary>
    /// Maximum age of persisted key/value revision records. <see cref="TimeSpan.Zero"/> disables age retention.
    /// </summary>
    public TimeSpan PersistentRevisionRetentionAge { get; set; }

    /// <summary>
    /// Minimum cadence for periodic persistent revision cleanup passes.
    /// </summary>
    public TimeSpan PersistentRevisionCleanupInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum revision records deleted per cleanup pass per backend worker.
    /// </summary>
    public int PersistentRevisionCleanupBatchSize { get; set; } = 1000;

    /// <summary>
    /// Queue keys touched by writes for targeted persistent revision cleanup.
    /// </summary>
    public bool PersistentRevisionCleanupOnWrite { get; set; } = true;

    /// <summary>
    /// Number of keys a KeyRange descriptor must contain before the auto-split trigger
    /// considers splitting it. 0 disables auto-split.
    /// </summary>
    public int RangeSplitThreshold { get; set; } = 1_000;

    /// <summary>
    /// Minimum number of keys each half must have after a range split.
    /// Prevents trivially small child ranges.
    /// </summary>
    public int RangeSplitMinRangeSize { get; set; } = 10;

    // ── Load-based split knobs ─────────────────────────────────────────────────
    // All off/inert by default so existing deployments are unaffected until opted in.
    // The load branch of the split trigger gates on rate AND saturation, sourced from the
    // Kommander per-partition signals (GetPartitionLogOpsPerSecond / GetPartitionWalQueueDepth
    // / GetPartitionCommitWaitMs).

    /// <summary>
    /// Rate gate. A KeyRange partition's log-replication rate (writes/sec, from
    /// <c>IRaft.GetPartitionLogOpsPerSecond</c>) must be at or above this before a load split is
    /// considered. <c>0</c> disables load-based splitting entirely (preserving count-only behaviour).
    /// </summary>
    public double RangeSplitLoadThreshold { get; set; }

    /// <summary>
    /// Primary saturation gate. The partition's WAL queue depth (from
    /// <c>IRaft.GetPartitionWalQueueDepth</c>) must be at or above this before a load split fires.
    /// Rate alone is never sufficient — it plateaus at the fsync ceiling, so a sustained backlog is
    /// what distinguishes an overloaded partition from a merely busy one.
    /// </summary>
    public int RangeSplitLoadMinQueueDepth { get; set; } = 8;

    /// <summary>
    /// Optional secondary saturation gate. When greater than <c>0</c>, the partition's commit-wait
    /// latency (from <c>IRaft.GetPartitionCommitWaitMs</c>) must also be at or above this (ms).
    /// Off by default: commit-wait is sticky when idle, so it is only ever AND-combined behind the
    /// rate gate. Prefer <see cref="RangeSplitLoadMinQueueDepth"/> (self-clearing) as the primary.
    /// </summary>
    public double RangeSplitLoadMinCommitWaitMs { get; set; }

    /// <summary>
    /// Debounce window. The full rate-AND-saturation predicate must hold continuously for at least
    /// this long before a load split fires, so a single stale or bursty gossiped report cannot trip
    /// one. Must be at least the gossip + EWMA lag (~10s) of the Kommander signals.
    /// </summary>
    public TimeSpan RangeSplitLoadWindow { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Cadence at which the checker polls the cheap per-partition load signals to maintain each
    /// descriptor's "hot since" timestamp, decoupled from the slower full key-count sampling pass.
    /// Polling faster than <see cref="RangeSplitLoadWindow"/> is what makes "sustained for the
    /// window" measurable.
    /// </summary>
    public TimeSpan RangeSplitLoadPollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Indivisibility guard. A range is refused as indivisible when no split key can put each
    /// child below this fraction of the parent's write rate — i.e. essentially all writes hit one
    /// key. Catches the "thousands of keys, ~all writes on one" thrash case the key-count guard misses.
    /// </summary>
    public double RangeSplitLoadImbalanceMax { get; set; } = 0.8;

    /// <summary>
    /// How long to suppress count-branch re-sampling after an indivisibility refusal. During this
    /// window the descriptor is skipped entirely, avoiding an expensive 4096-key scan every
    /// <c>CollectionInterval</c> for a persistently hot-key range. Defaults to five minutes —
    /// roughly five collection passes at the default 60-second cadence.
    /// </summary>
    public TimeSpan RangeSplitIndivisibleCooldown { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Per-descriptor post-split cooldown. A descriptor that just split (parent or either child)
    /// is not re-evaluated for splitting until this elapses, so a still-hot child does not re-split
    /// while its predecessor's leadership transfer is in flight. Defaults to roughly Kommander's
    /// <c>MinLeaderStabilityMs</c> (5s) plus a margin.
    /// </summary>
    public TimeSpan RangeSplitSettleWindow { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Upper bound on how long a range split or merge holds its quiesce while it drains the moving
    /// range's unsettled durable intents before the cutover. The quiesce blocks new prepares, so the
    /// intent set can only shrink: decided intents settle immediately, and undecided ones belong to
    /// in-flight coordinators whose decisions land within this wait. Without the wait a range under
    /// sustained writes always carries a few just-prepared intents and every move attempt is refused.
    /// Writes into the moving range stay refused (retryably) for at most this long per attempt.
    /// Zero or negative disables the wait: one settle pass runs and an unsettled intent refuses the
    /// attempt. Values above half the 30-second quiesce window are clamped to 15 seconds, so the
    /// copy and cutover that follow always run inside the window the drain consumed part of.
    /// </summary>
    public TimeSpan RangeMoveSettleTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Maximum number of keys a KeyRange descriptor may contain before it is no longer
    /// considered an under-min merge candidate. When two adjacent descriptors both have fewer
    /// than this value the auto-merge trigger coalesces them. 0 disables auto-merge.
    /// </summary>
    public int RangeMergeMinSize { get; set; } = 10;

    /// <summary>
    /// Length of the point-in-time recovery window. WAL entries older than
    /// <c>now - PitrWindow</c> may be compacted away. Valid range: (0, 6h].
    /// </summary>
    public TimeSpan PitrWindow { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// How often a new base checkpoint is taken per shard. Must be positive and
    /// no greater than <see cref="PitrWindow"/>; the sliding horizon is anchored
    /// at <c>now - PitrWindow - BaseSnapshotInterval</c>.
    /// </summary>
    public TimeSpan BaseSnapshotInterval { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// How long a partition stays dirty before the background writer advances its WAL retention floor with a
    /// checkpoint. Measured from the moment the partition first went dirty after its previous checkpoint, so
    /// this is the period at which a continuously-written partition checkpoints — and therefore the period at
    /// which its Raft WAL becomes compactable. The receipt and decision snapshots that gate that checkpoint are
    /// written on the same cadence, so a committed receipt/decision stays replayable in the WAL until this
    /// interval elapses. Longer means fewer checkpoints and a larger WAL; shorter means more snapshot rewrites.
    /// </summary>
    public TimeSpan CheckpointInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Declares that the entire Raft group — every voter, witness, and transport — lives inside this
    /// process for the lifetime of the deployment, so no replication proposal can outlive the process:
    /// an in-flight proposal that was not durably committed in the local WAL at a crash is gone forever
    /// (there is no remote replica to resurrect it), and a committed one replays during restore, before
    /// the node serves new work.
    ///
    /// <para><b>What it unlocks.</b> The one-phase durable-commit fast path validates the transaction's
    /// read set <em>before</em> proposing the bundled [record init + prepare + commit decision]; at
    /// apply time only the written keys are re-checked. On a multi-node cluster a stalled bundle can
    /// surface after a leader change with its read validation long out of date — so a read set that
    /// reaches beyond the written keys forces the standard 2PC flow there. In a single-process group
    /// that stall cannot exist: the bundle either applies within the same process incarnation that
    /// validated it (the in-memory write intents that fence the validation window are still live) or it
    /// was already durably committed and replays in log order ahead of any later conflicting write.
    /// With this flag set, read-carrying transactions stay eligible for the one-phase fast path.</para>
    ///
    /// <para><b>Set it only when the topology guarantees the property permanently.</b> The embedded
    /// standalone node (in-process phantom witnesses, no inter-node transport) sets it automatically.
    /// It must stay false for any deployment a remote replica can ever join: the guarantee is
    /// topological, not operational, and a group that grows a real peer while a bundle is in flight
    /// would reopen the stalled-bundle window this flag assumes away.</para>
    /// </summary>
    public bool SingleProcessRaftGroup { get; set; }

    /// <summary>
    /// Milliseconds a transactional staged write's in-memory write intent stays live before other
    /// transactions may treat it as abandoned and write past it. This lease is the window guard between a
    /// transaction staging a write and its durable prepare landing; once it lapses (a paused coordinator,
    /// a stalled client between statements), a competitor can stage and commit the same key — the
    /// interleaving the post-prepare staged-base fence exists to abort. Shortening it increases exposure
    /// to that fence's aborts under slow clients; lengthening it delays writers behind genuinely
    /// abandoned transactions until the reaper clears them. Was a hardcoded 15 s before it was a setting.
    /// </summary>
    public int StagedWriteIntentLeaseMs { get; set; } = 15_000;

    /// <summary>
    /// Milliseconds one range-scan page may keep answering transient (MustRetry/WaitingForReplication)
    /// before the scan fails loudly with the range and cursor named, instead of retrying in silence. The
    /// budget resets on every page that makes progress; genuine settlement lag resolves in milliseconds,
    /// so exhausting it means a page that cannot serve — e.g. a key holding an orphaned write intent that
    /// will never resolve. The failure is retryable, so a false positive under extreme lag costs one
    /// retried scan; the budget never firing costs an unattributed hang.
    ///
    /// <para><b>Ordering constraint:</b> this value must stay comfortably below the smallest client
    /// command deadline in front of the scan, or the client cancels the call first and the named error is
    /// raised into a request nobody is waiting on — the caller then sees only an anonymous cancellation.
    /// The shipped client stack's default command deadline is 10 s, hence the 5 s default. A value
    /// &lt;= 0 falls back to the built-in default.</para>
    /// </summary>
    public int ScanPageRetryBudgetMs { get; set; } = 5_000;

    /// <summary>
    /// Milliseconds a session-owned write intent or range lock — one requested with <c>expiresMs == 0</c>,
    /// so it carries no clock deadline — may stay live without its owning session releasing it. Past this
    /// age the session has been finalized or reaped, so no legitimate transaction can still hold the key and
    /// the record is dropped on next touch. Without the ceiling an intent whose session vanished before its
    /// cleanup ran is immortal at the actor, and a snapshot scan of any page containing that key can never
    /// serve.
    ///
    /// <para>A value of <c>0</c> means derived: <c>MaxTransactionTimeout</c> plus the reaper grace window
    /// plus the maximum participant-effect TTL, which is exactly the span the session machinery already
    /// bounds itself by. An explicit value below <c>MaxTransactionTimeout</c> plus the reaper grace window is
    /// rejected at load, because a ceiling under the session bound could expire a live transaction's lock —
    /// the one outcome this setting must never produce. Prepared intents are exempt at any age; their fate
    /// belongs to the decision machinery.</para>
    /// </summary>
    public int SessionOwnedIntentCeilingMs { get; set; }

    /// <summary>
    /// Milliseconds the prepare-apply staged-base fence remembers a key's last transactionally committed head.
    /// The fence refuses a validated-base prepare's acknowledgement when the base moved — the lost-update
    /// guard — and, because pruned memory is indistinguishable from "no commit happened", it also refuses any
    /// validated-base prepare from a transaction that BEGAN before this horizon. Must therefore comfortably
    /// exceed the longest transaction lifetime the deployment allows (session timeouts, reaper idle
    /// horizons); a value below real lifetimes turns long transactions into spurious conflict aborts. Memory
    /// cost is one small entry per transactionally written key within the horizon.
    /// </summary>
    public int StagedBaseFenceRetentionMs { get; set; } = 600_000;

    /// <summary>
    /// Root directory for PITR backup artifacts and catalog manifests.
    /// When empty, backup operations are disabled.
    /// <para>
    /// Still required with a non-local <see cref="BackupTarget"/>: it is where the local scratch area and
    /// any host-side state live, and it remains the fallback root the default local stores would use.
    /// </para>
    /// </summary>
    public string BackupDir { get; set; } = "";

    /// <summary>
    /// Which storage target holds backups. <c>"local"</c> (the default) keeps them in
    /// <see cref="BackupDir"/> on this host. Any other value requires a matching
    /// <see cref="BackupStorageProvider"/> to be registered by the host, since object-storage targets
    /// live in separate assemblies that <c>Kahuna.Core</c> deliberately does not reference — an embedded
    /// consumer must not inherit a cloud SDK it never asked for.
    /// </summary>
    public string BackupTarget { get; set; } = "local";

    /// <summary>
    /// Local directory backup bytes transit when the target cannot be written to or read from directly by
    /// a persistence backend. A checkpoint is produced by the storage engine through the filesystem and
    /// nothing else, so a remote target must stage it here before upload.
    /// <para>
    /// Mandatory for a target that requires it, and validated at startup rather than at first backup.
    /// Size it for a whole full backup: one transits this directory in its entirety.
    /// </para>
    /// </summary>
    public string BackupScratchDir { get; set; } = "";

    /// <summary>
    /// Host-supplied factory for the backup storage pair, and the seam through which an object-storage
    /// target is installed. Left null the local directory implementations are used, which is why nothing
    /// changes for an existing deployment.
    /// <para>
    /// Not serialized configuration — a host sets this in code (from <c>Kahuna.Server</c>'s DI wiring, or
    /// directly for an embedded node) after deciding which package to depend on.
    /// </para>
    /// </summary>
    public BackupStorageProvider? BackupStorageProvider { get; set; }

    /// <summary>
    /// Operator-assigned identity of this cluster, stamped into every backup manifest. Set the SAME
    /// value on every node of a cluster. It gates backup-chain resolution: a chain must not span
    /// manifests carrying different cluster ids, so a foreign cluster's artifacts can never be chained
    /// or restored here. Empty (the default) leaves it unset — the cross-cluster guard is then dormant
    /// (a null id is treated as "unknown", never forced to match).
    /// </summary>
    public string BackupClusterId { get; set; } = "";

    /// <summary>
    /// Path to a file holding the secret key used to authenticate backup manifests (HMAC-SHA-256). Set the
    /// SAME key file (contents) on every node. When set, each manifest is signed on write and its tag is
    /// verified before restore, so tampering with an artifact and its recorded digest — or stripping the
    /// tag — is detected. Keep the file readable only by the server user and OUTSIDE the backup directory.
    /// Empty disables authentication. Enabling it means backups taken before it was configured are
    /// unsigned and can no longer be restored until re-taken.
    /// </summary>
    public string BackupMacKeyFile { get; set; } = "";

    /// <summary>
    /// Server-owned root directory that restore destinations must be canonically contained within.
    /// When set, a restore <c>targetDir</c> (from any caller, including remote REST/gRPC requests) is
    /// confined under this root after resolving symlinks on every ancestor. When empty, no root
    /// confinement is applied — and remote restore is refused by default (see
    /// <see cref="AllowUnconfinedRemoteRestore"/>): configuring a restore root is the opt-in that
    /// enables remote restore.
    /// </summary>
    public string RestoreRoot { get; set; } = "";

    /// <summary>
    /// Escape hatch to allow remote (REST/gRPC) restore requests even when no <see cref="RestoreRoot"/>
    /// is configured. Default false — restore is an administrative operation, and without a
    /// server-owned root or authentication a remote caller could plant/oversize files at arbitrary
    /// process-writable paths, so remote restore is denied unless explicitly opted in here.
    /// </summary>
    public bool AllowUnconfinedRemoteRestore { get; set; }

    /// <summary>
    /// Backup retention: keep at most this many most-recent backup chains, deleting older chains whole
    /// (root Full + all its incrementals). Zero or negative means unbounded — no count-based deletion.
    /// Retention is OFF by default (all three retention bounds unset), so backups are never reclaimed
    /// unless an operator explicitly opts in; orphaned/leftover artifacts from crashed backups are
    /// always swept regardless.
    /// </summary>
    public int BackupRetentionMaxChains { get; set; }

    /// <summary>
    /// Backup retention: delete any chain whose newest backup is older than this. <see cref="TimeSpan.Zero"/>
    /// means unbounded — no age-based deletion.
    /// </summary>
    public TimeSpan BackupRetentionMaxAge { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Backup retention: keep the most-recent chains whose combined artifact bytes stay within this
    /// budget, deleting older chains beyond it (the single most-recent chain is always kept even if it
    /// alone exceeds the budget). Zero or negative means unbounded — no size-based deletion.
    /// </summary>
    public long BackupRetentionMaxBytes { get; set; }

    /// <summary>
    /// Cadence of the background backup garbage-collection pass (orphan/leftover artifact sweep, plus
    /// retention enforcement when configured). A pass also runs shortly after startup so crash-orphaned
    /// artifacts are reclaimed without waiting for the next backup. <see cref="TimeSpan.Zero"/> or
    /// negative disables the periodic pass entirely (GC then runs only inline after each backup).
    /// </summary>
    public TimeSpan BackupGcInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Throughput budget, in bytes per second, for a restore's bulk checkpoint copy. Caps the copy so a
    /// restore does not saturate the disk and starve foreground traffic. Zero or negative = unlimited.
    /// </summary>
    public long BackupRestoreThrottleBytesPerSec { get; set; }
}