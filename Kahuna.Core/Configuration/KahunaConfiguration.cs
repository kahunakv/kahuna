
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
    
    public int DefaultTransactionTimeout { get; set; }
    
    public int RevisionsToKeepCached { get; set; }
    
    public TimeSpan CacheEntryTtl { get; set; }
    
    public int CacheEntriesToRemove { get; set; }

    public TimeSpan CollectionInterval { get; set; } = TimeSpan.FromSeconds(60);

    public int MaxEntriesPerActor { get; set; } = 50_000;

    public long MaxBytesPerActor { get; set; } = 256L * 1024 * 1024;

    public int CollectBatchMax { get; set; } = 1_000;

    public int RevisionRetention { get; set; } = 16;

    public int LruSampleSize { get; set; } = 5;

    public int LruSampleScanMax { get; set; } = 256;

    /// <summary>
    /// Run metadata trimming every N collection cycles (1 = every cycle). 0 disables trimming.
    /// </summary>
    public int MetadataTrimInterval { get; set; } = 4;
    
    public int DirtyObjectsWriterDelay { get; set; }
}