
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