
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
}