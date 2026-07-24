
namespace Kahuna.Server.Configuration;

public sealed class KahunaConfiguration
{
    public string HttpsCertificate { get; set; } = "";
    
    public string HttpsTrustedThumbprint { get; set; } = "";
    
    public string HttpsCertificatePassword { get; set; } = "";
    
    public int LocksWorkers { get; set; }
    
    public int KeyValueWorkers { get; set; }
    
    public int BackgroundWriterWorkers { get; set; }

    public string Storage { get; set; } = "";
    
    public string StoragePath { get; set; } = "";
    
    public string StorageRevision { get; set; } = "";
    
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
    /// Maximum terminal transaction records the retention GC sweep reclaims in one collection pass, per node.
    /// Bounds the work (and the participant receipt-forget fan-out) a single sweep performs so a large backlog —
    /// e.g. after a restart or a burst — is drained across successive passes instead of monopolizing one tick.
    /// A value &lt;= 0 disables the per-pass cap (drain everything eligible each pass).
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
    /// Minimum age a dirty partition must reach before the background writer advances its WAL retention floor
    /// with a checkpoint. The receipt and decision snapshots that gate that checkpoint are written on the same
    /// cadence, so a committed receipt/decision stays replayable in the WAL until this interval elapses.
    /// </summary>
    public TimeSpan CheckpointInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Root directory for PITR backup artifacts and catalog manifests.
    /// When empty, backup operations are disabled.
    /// </summary>
    public string BackupDir { get; set; } = "";
}