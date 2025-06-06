
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
    
    public int DirtyObjectsWriterDelay { get; set; }
}