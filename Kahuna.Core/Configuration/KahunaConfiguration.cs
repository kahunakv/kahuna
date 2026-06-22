
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
    
    public int DefaultTransactionTimeout { get; set; }
    
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
    /// Root directory for PITR backup artifacts and catalog manifests.
    /// When empty, backup operations are disabled.
    /// </summary>
    public string BackupDir { get; set; } = "";
}