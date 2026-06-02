namespace Kahuna;

/// <summary>
/// Options used to run a single-node Kahuna engine in-process.
/// </summary>
public sealed class EmbeddedKahunaOptions
{
    public string NodeName { get; set; } = "embedded-1";

    public int NodeId { get; set; } = 1;

    public string Host { get; set; } = "localhost";

    public int Port { get; set; }

    public int InitialPartitions { get; set; } = 1;

    /// <summary>
    /// Persistence backend for locks, key-values, and sequences. Supported values: memory, sqlite, rocksdb.
    /// </summary>
    public string Storage { get; set; } = "memory";

    public string StoragePath { get; set; } = "";

    public string StorageRevision { get; set; } = "";

    /// <summary>
    /// Raft WAL backend. Supported values: memory, sqlite, rocksdb.
    /// </summary>
    public string WalStorage { get; set; } = "memory";

    public string WalPath { get; set; } = "";

    public string WalRevision { get; set; } = "";

    public int LocksWorkers { get; set; } = Environment.ProcessorCount;

    public int KeyValueWorkers { get; set; } = Environment.ProcessorCount;

    public int BackgroundWriterWorkers { get; set; } = 1;

    public int DefaultTransactionTimeout { get; set; } = 5000;

    public TimeSpan ScriptCacheExpiration { get; set; } = TimeSpan.FromMinutes(1);

    public int RevisionsToKeepCached { get; set; } = 100;

    public TimeSpan CacheEntryTtl { get; set; } = TimeSpan.FromMinutes(5);

    public int CacheEntriesToRemove { get; set; } = 1000;

    public TimeSpan CollectionInterval { get; set; } = TimeSpan.FromSeconds(60);

    public int MaxEntriesPerActor { get; set; } = 50_000;

    public long MaxBytesPerActor { get; set; } = 256L * 1024 * 1024;

    public int CollectBatchMax { get; set; } = 1_000;

    public int RevisionRetention { get; set; } = 16;

    public int LruSampleSize { get; set; } = 5;

    public int LruSampleScanMax { get; set; } = 256;

    public int MetadataTrimInterval { get; set; } = 4;

    public int DirtyObjectsWriterDelay { get; set; } = 1000;

    public int ReadIOThreads { get; set; } = 8;

    public int WriteIOThreads { get; set; } = 8;

    public int StartElectionTimeout { get; set; } = 500;

    public int EndElectionTimeout { get; set; } = 1500;

    public int CompactEveryOperations { get; set; } = 1000;

    public int CompactNumberEntries { get; set; } = 50;
}
