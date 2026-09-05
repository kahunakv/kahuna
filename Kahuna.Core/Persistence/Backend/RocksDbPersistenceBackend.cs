
using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Runtime.InteropServices;
using System.Text;
using Kahuna.Server.Locks;
using Kahuna.Persistence.Protos;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RocksDbSharp;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.Communication.Grpc;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Provides persistence backend functionality using RocksDB as the underlying database.
/// Implements the <see cref="IPersistenceBackend"/> interface along with IDisposable.
/// </summary>
/// <remarks>
/// This class allows storing and retrieving data categorized into different column families
/// such as key-values and locks. The implementation ensures efficient read-write operations
/// and transactional support using RocksDB capabilities.
/// </remarks>
internal sealed class RocksDbPersistenceBackend : IPersistenceBackend, IDisposable
{
    /// <summary>
    /// Defines a constant string value used as a marker suffix appended to keys
    /// within the database operations. This marker is utilized to represent the
    /// "current" version of an item in RocksDB storage, ensuring unique identification
    /// of the latest state of a record.
    /// </summary>
    private const string CurrentMarker = "~CURRENT";

    // UTF-8 encoding of CurrentMarker for allocation-free key construction. The Debug.Assert
    // below catches any future edit to the const that forgets to update this literal.
    private static ReadOnlySpan<byte> CurrentMarkerUtf8 => "~CURRENT"u8;

    // Per-key no-revision provenance marker. Sorts after "~CURRENT" and after numeric revision
    // suffixes, so it is the last row in a logical key's contiguous group. Its value packs the
    // earliest and latest HLC the key was written with SetNoRevision (6 × int64, little-endian:
    // earliest node/physical/counter then latest node/physical/counter). It never maps to a logical
    // key on read (revision/current scans skip it) and is dropped from as-of images.
    private static ReadOnlySpan<byte> NoRevMarkerUtf8 => "~NOREV"u8;
    
    private const int NoRevProvenanceSize = 6 * sizeof(long);

    // Guard for stackalloc on key buffers. Inputs are caller-controlled, so we must bound the
    // stack frame; anything larger goes through ArrayPool. Real keys are well under this, so the
    // ArrayPool path is rarely taken while the per-call stack zeroing stays cheap.
    private const int KeyStackThreshold = 256;

    /// <summary>
    /// Represents the default write options for write operations in the persistence backend.
    /// Configured with synchronization enabled to ensure durability by flushing changes to disk
    /// before returning control to the calling code.
    /// </summary>
    /// <remarks>
    /// This is a deliberate second fsync: writes already pass through the durable Raft WAL before
    /// reaching this backend. Sync is retained because the checkpoint mechanism can truncate the WAL
    /// once a partition is checkpointed, after which this store becomes the source of truth for that
    /// data — dropping sync here would risk losing checkpointed state on a crash. Relaxing it would
    /// require ordering guarantees between checkpoint/WAL-truncation and this store's flush, which is
    /// out of scope for the table-tuning change.
    /// </remarks>
    private static readonly WriteOptions DefaultWriteOptions = new WriteOptions().SetSync(true);

    /// <summary>
    /// Read options for background maintenance scans (revision pruning / backend-wide sweeps). These walk
    /// large, mostly-cold slices of the keyspace once, so they must not fill the block cache — under direct
    /// reads it is the sole in-RAM read cache, and letting a sweep populate it would evict blocks the
    /// point-read hot path depends on. User-facing scans deliberately keep the default (cache-filling)
    /// behavior; changing those needs workload measurement.
    /// </summary>
    private static readonly ReadOptions MaintenanceScanReadOptions = new ReadOptions().SetFillCache(false);

/// <summary>
    /// Process-wide fallback block cache for the table reader, used when no shared bundle is injected.
    /// Sized once and reused across both column families so hot index/filter/data blocks stay resident
    /// instead of being re-read from disk on every point lookup. Held in a static field to keep it alive
    /// for the lifetime of the process. When a <see cref="RocksDbSharedResources"/> bundle is supplied,
    /// its cache is used instead so this backend and the Raft WAL draw from one unified memory budget.
    /// </summary>
    private static readonly Cache BlockCache = Cache.CreateLru(256 * 1024 * 1024);

    // Mutable only under the swap fence's write side: the storage-failure recovery path closes the
    // wedged instance and reopens it in place (see TryRecoverFromStorageFailure). Every use of
    // these three fields must run between EnterDbFence and ExitDbFence, which guarantees the
    // handles stay alive and stable for the duration of the operation.
    private RocksDb db;

    private ColumnFamilyHandle columnFamilyKeys;

    private ColumnFamilyHandle columnFamilyLocks;

    private readonly string path;

    private readonly string dbRevision;

    // Open-time settings retained so a recovery reopen reproduces the original configuration.
    private readonly RocksDbSharedResources? sharedResources;

    private readonly bool directReads;

    private readonly bool enableStatistics;

    /// <summary>
    /// Swap fence for the close-and-reopen recovery path. Every operation that touches the native
    /// RocksDB handle holds the read side; the recovery swap holds the write side, so it waits for
    /// in-flight operations (including full iterator scans and checkpoint saves) and blocks new
    /// ones for the duration of the swap. Uncontended read acquisition is nanoseconds against
    /// microsecond-scale native calls, so the fence does not tax the hot read path.
    /// </summary>
    private readonly ReaderWriterLockSlim swapFence = new(LockRecursionPolicy.NoRecursion);

    /// <summary>
    /// True while the engine is closed: between a recovery close and a successful reopen, and
    /// after <see cref="Dispose"/>. Guarded by <see cref="swapFence"/>. While set, every
    /// operation fails with a managed <see cref="IOException"/> instead of touching a disposed
    /// native handle.
    /// </summary>
    private bool storageUnavailable;

    private bool disposed;

    /// <summary>
    /// Reports write failures that this backend converts into a <c>false</c> return — see the
    /// store methods' failure contract. Never null (falls back to <see cref="NullLogger"/>).
    /// </summary>
    private readonly ILogger logger;

    // Durable pruned-history floor, stored in the kv column family under a reserved meta key and
    // written in the SAME WriteBatch as the revision deletes that advance it — so a crash can never
    // leave the floor trailing the deleted history. The '\0'-prefixed key never collides with a user
    // key and is inert to the ~CURRENT / numeric-revision scans (which never see a '\0' prefix).
    private static ReadOnlySpan<byte> PrunedFloorKeyUtf8 => "\0pitr_pruned_history_floor"u8;

    // The floor's key before the '\0' prefix: a leading space. A store that pruned under an older
    // build holds its floor only here, and an absent floor reads as "never pruned" — the value that
    // licenses a backup to trust already-deleted history. So when the current key is absent, the
    // load falls back to this key and copies the value forward. The legacy key is left in place so
    // a rollback to an older build still reads a valid floor.
    private static ReadOnlySpan<byte> LegacyPrunedFloorKeyUtf8 => " pitr_pruned_history_floor"u8;

    // A floor that refuses every real cut: returned when the durable floor is unreadable/corrupt for a
    // store that may have pruned, so backups fail closed until integrity is re-established.
    private static readonly HLCTimestamp FailClosedFloor = new(int.MaxValue, long.MaxValue, uint.MaxValue);

    private readonly Lock _floorLock = new();
    
    private HLCTimestamp? _prunedFloorCache; // null until first loaded from the DB
    
    private bool _prunedFloorCorrupt;

    /// <summary>
    /// Raw key bytes at which the next backend-wide revision sweep should resume, or <c>null</c> to
    /// start from the beginning of the column family. Carried across sweep passes so each pass scans
    /// a bounded slice of the keyspace instead of the whole column family every interval.
    /// </summary>
    private byte[]? sweepCursor;

    /// <summary>
    /// RocksDbPersistenceBackend is a persistence backend implementation based on RocksDB.
    /// It provides methods for storing and retrieving key-value pairs and locks, categorized
    /// into distinct column families for better organization and efficiency.
    /// </summary>
    /// <remarks>
    /// The class offers support for efficient data management, including transactional
    /// operations, prefix-based queries, and version-specific retrievals. It integrates
    /// RocksDB's advanced features, ensuring high-performance persistent storage.
    /// </remarks>
    /// <param name="sharedResources">
    /// Optional shared-memory bundle (one block cache + one WriteBufferManager) created and owned by the
    /// composition root and shared with Kommander's Raft WAL so both RocksDB instances draw from a single
    /// unified memory budget. When non-null, its block cache replaces the static fallback and its WBM is
    /// attached to this backend's <see cref="DbOptions"/>. This backend <b>borrows</b> the bundle and must
    /// never dispose it — the composition root disposes it after both databases are closed. When null,
    /// behavior is byte-for-byte identical to the prior default (static 256 MB cache, no WBM).
    /// </param>
    public RocksDbPersistenceBackend(string path = ".", string dbRevision = "v1", RocksDbSharedResources? sharedResources = null, bool directReads = true, bool enableStatistics = false, ILogger? logger = null)
    {
        this.path = path;
        this.dbRevision = dbRevision;
        this.sharedResources = sharedResources;
        this.directReads = directReads;
        this.enableStatistics = enableStatistics;
        this.logger = logger ?? NullLogger.Instance;

        db = OpenStore(out columnFamilyKeys, out columnFamilyLocks);
    }

    /// <summary>
    /// Opens the store at its configured path with the retained open-time settings. Used by the
    /// constructor and by the storage-failure recovery reopen
    /// (<see cref="TryRecoverFromStorageFailure"/>).
    /// </summary>
    private RocksDb OpenStore(out ColumnFamilyHandle kvFamily, out ColumnFamilyHandle locksFamily)
    {
        string fullPath = $"{path}/{dbRevision}";

        DbOptions dbOptions = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            // A torn record at the tail of RocksDB's own log is the normal residue of an unclean
            // stop: a SIGKILL between the write syscall and its completion, or a WAL append cut
            // short by ENOSPC. Recovery.AbsoluteConsistency refuses to open on it, which turns
            // that single event into a permanently unopenable store — at process start (crash
            // loop) and equally on the in-place recovery reopen after a storage failure.
            //
            // TolerateCorruptedTailRecords (RocksDB's own default) drops that one short record and
            // opens. Dropping it loses nothing that was promised: every write here is synced
            // (DefaultWriteOptions), so a record that is short was never acknowledged — it belongs
            // to a failed batch the background writer still retains, or to state whose Raft WAL
            // entries the durability floor keeps replayable. Real damage — a checksum mismatch
            // with whole records after it — still refuses to open in this mode.
            .SetWalRecoveryMode(Recovery.TolerateCorruptedTailRecords)
            // Use available cores for background flush/compaction so writes don't stall behind a
            // single-threaded compactor under sustained load.
            .IncreaseParallelism(Math.Max(2, Environment.ProcessorCount))
            .SetMaxBackgroundFlushes(2)
            .SetMaxBackgroundCompactions(Math.Max(2, Environment.ProcessorCount / 2));

        // Statistics carry a measurable per-operation cost, so they are opt-in for tuning/diagnosis only.
        // When enabled, RocksDB tracks counters (block-cache hit rate, write stalls, compaction bytes,
        // per-level file counts, ...) and periodically dumps them to its LOG file under the storage path,
        // where they can be read without any additional export surface.
        if (enableStatistics)
        {
            dbOptions.EnableStatistics();
            // Dump the accumulated stats to the RocksDB LOG on this cadence (seconds).
            Native.Instance.rocksdb_options_set_stats_dump_period_sec(dbOptions.Handle, 60);
        }

        // With direct reads, SST reads bypass the OS page cache and go straight to disk, so the only
        // in-RAM read cache is RocksDB's own block cache. This removes the double-caching where a hot
        // block lives both uncompressed in the block cache and compressed in the page cache, giving a
        // predictable, block-cache-bounded read memory footprint. The trade-off is that every
        // block-cache miss becomes a physical read instead of possibly being served from the page
        // cache, so it relies on the block cache being sized for the hot working set. Left on by
        // default; disable it to fall back to buffered reads backed by the page cache.
        if (directReads)
            dbOptions.SetUseDirectReads(true);

        // When sharing resources, attach the WriteBufferManager to the DbOptions before opening so this
        // backend's memtables count against the same unified budget as the WAL's. Must be set before
        // RocksDb.Open — it cannot be changed afterward.
        if (sharedResources is not null)
            Native.Instance.rocksdb_options_set_write_buffer_manager(
                dbOptions.Handle, sharedResources.WriteBufferManagerHandle);

        // Bloom filters + a block cache make point lookups (GetKeyValue/GetLock, the read hot path)
        // avoid touching SSTs on a miss and keep hot blocks resident across reads. Use the injected
        // shared cache when present so both column families here — and the WAL's — share one budget;
        // otherwise fall back to the process-wide static cache.
        // Index and filter blocks live in the block cache (so they count against one bounded budget).
        // Pinning the L0 filter/index blocks keeps them resident instead of evictable: with direct reads
        // the page cache no longer backs a re-read, so an evicted filter/index block would cost a
        // synchronous disk read on the point-lookup hot path, and L0 is consulted on every lookup.
        BlockBasedTableOptions tableOptions = new BlockBasedTableOptions()
            .SetBlockCache(sharedResources?.BlockCache ?? BlockCache)
            .SetFilterPolicy(BloomFilterPolicy.Create(10, false))
            .SetCacheIndexAndFilterBlocks(true)
            .SetPinL0FilterAndIndexBlocksInCache(true)
            .SetWholeKeyFiltering(true);

        // Per-column-family memtable sizing. These are column-family options in RocksDB, so they must be
        // set on each ColumnFamilyOptions — setting them on DbOptions does not reach the "kv"/"locks"
        // families (they would stay at the max_write_buffer_number=2 default). A larger memtable absorbs
        // more writes before flushing (less write amplification); up to three lets writes continue into a
        // fresh memtable while two earlier ones await flush.
        ColumnFamilyOptions kvOptions = new ColumnFamilyOptions()
            .SetBlockBasedTableFactory(tableOptions)
            .SetWriteBufferSize(64 * 1024 * 1024)
            .SetMaxWriteBufferNumber(3)
            .SetMinWriteBufferNumberToMerge(1);
        
        // Locks are small (~hundreds of bytes), short-lived, and written at a fraction of kv volume,
        // so their memtables get a much smaller ceiling: 8 MB x 2 (16 MB worst case) instead of the
        // kv family's 64 MB x 3 (192 MB). Memtable arenas grow lazily, so this bounds the standalone
        // worst case rather than shrinking resident memory of an idle family; an 8 MB memtable still
        // holds tens of thousands of lock writes before flushing.
        ColumnFamilyOptions locksOptions = new ColumnFamilyOptions()
            .SetBlockBasedTableFactory(tableOptions)
            .SetWriteBufferSize(8 * 1024 * 1024)
            .SetMaxWriteBufferNumber(2)
            .SetMinWriteBufferNumberToMerge(1);

        ColumnFamilies columnFamilies = new()
        {
            { "kv", kvOptions },
            { "locks", locksOptions }
        };

        RocksDb database = RocksDb.Open(dbOptions, fullPath, columnFamilies);

        kvFamily = database.GetColumnFamily("kv");
        locksFamily = database.GetColumnFamily("locks");

        return database;
    }

    /// <summary>
    /// Enters the swap fence's read side. Throws a managed <see cref="IOException"/> while the
    /// engine is closed (mid-recovery after a failed reopen, or after Dispose) instead of letting
    /// the caller touch a disposed native handle. Callers must pair this with
    /// <see cref="ExitDbFence"/> in a <c>finally</c> block.
    /// </summary>
    private void EnterDbFence()
    {
        swapFence.EnterReadLock();

        if (storageUnavailable)
        {
            swapFence.ExitReadLock();
            throw new IOException(
                $"The RocksDB store at '{path}/{dbRevision}' is closed after a storage failure; recovery has not reopened it yet.");
        }
    }

    private void ExitDbFence() => swapFence.ExitReadLock();

    /// <summary>
    /// Reopen preflight: a reopen while the volume is still (nearly) full cannot succeed — RocksDB
    /// needs room for a fresh WAL and manifest — and closing the wedged instance would take the
    /// read path down with it. Requires a modest floor of free space before any swap is attempted.
    /// When the free space cannot be determined, the reopen is allowed rather than blocked.
    /// </summary>
    private const long MinFreeBytesForReopen = 64L * 1024 * 1024;

    private bool HasFreeSpaceForReopen()
    {
        try
        {
            string? root = Path.GetPathRoot(Path.GetFullPath(path));
            return root is null || new DriveInfo(root).AvailableFreeSpace >= MinFreeBytesForReopen;
        }
        catch
        {
            return true;
        }
    }

    private const int RecoveryReopenAttempts = 5;

    /// <summary>
    /// Storage-failure recovery: closes the RocksDB instance and reopens it at the same path.
    /// <para>
    /// RocksDB latches a background error after a failed WAL append (for example ENOSPC): every
    /// later write returns the cached error without new I/O, and with no <c>SstFileManager</c>
    /// installed the engine never re-checks the disk on its own. The binding exposes no resume
    /// call, so a close-and-reopen is the only way to clear the latch in-process. The write-behind
    /// design makes the swap safe: the background writer retains failed batches, and the
    /// durability floor advances only after data lands, so nothing acknowledged is lost.
    /// </para>
    /// <para>
    /// Concurrent readers, iterators, and checkpoint calls block on the swap fence for the
    /// duration of the swap and resume against the fresh instance. The reopen uses the same
    /// tail-tolerant WAL recovery mode as every open (see <see cref="OpenStore"/>): a WAL append
    /// cut short by ENOSPC leaves a torn tail record, and that record belongs to the failed,
    /// never-acknowledged batch the writer still retains and will rewrite.
    /// </para>
    /// Returns <c>true</c> when the engine is open again; <c>false</c> when the volume is still
    /// full (nothing was closed) or every bounded reopen attempt failed (the store then reports
    /// <see cref="IOException"/> until a later recovery call succeeds).
    /// </summary>
    public bool TryRecoverFromStorageFailure()
    {
        if (!HasFreeSpaceForReopen())
        {
            logger.LogWarning(
                "RocksDb store at {Path}/{Revision} cannot be reopened yet: the volume still has less than {MinFree} bytes free",
                path, dbRevision, MinFreeBytesForReopen);
            return false;
        }

        swapFence.EnterWriteLock();

        try
        {
            if (disposed)
                return false;

            if (!storageUnavailable)
            {
                // Closing flushes what the engine still can and releases the path's LOCK file; a
                // second instance cannot open the path before this one is closed.
                db.Dispose();
                storageUnavailable = true;
            }

            for (int attempt = 1; attempt <= RecoveryReopenAttempts; attempt++)
            {
                try
                {
                    db = OpenStore(out columnFamilyKeys, out columnFamilyLocks);
                    storageUnavailable = false;

                    logger.LogWarning(
                        "RocksDb store at {Path}/{Revision} was closed and reopened after a storage failure (attempt {Attempt})",
                        path, dbRevision, attempt);

                    return true;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex,
                        "RocksDb store at {Path}/{Revision} failed reopen attempt {Attempt}/{Max}",
                        path, dbRevision, attempt, RecoveryReopenAttempts);

                    if (attempt < RecoveryReopenAttempts)
                        Thread.Sleep(200 * attempt);
                }
            }

            return false;
        }
        finally
        {
            swapFence.ExitWriteLock();
        }
    }

    public HLCTimestamp GetPrunedHistoryFloor()
    {
        EnterDbFence();
        try
        {
            lock (_floorLock)
                return CurrentPrunedFloorLocked();
        }
        finally
        {
            ExitDbFence();
        }
    }

    private const int PrunedFloorSize = 24;

    // Must hold _floorLock. Lazily loads the durable floor from the DB; a corrupt/unreadable value for
    // a store that may have pruned yields FailClosedFloor so backups refuse every cut until repaired.
    private HLCTimestamp CurrentPrunedFloorLocked()
    {
        if (_prunedFloorCorrupt)
            return FailClosedFloor;
        
        if (_prunedFloorCache is null)
        {
            try
            {
                byte[]? data = db.Get(PrunedFloorKeyUtf8, cf: columnFamilyKeys);

                // Absent under the current key: consult the legacy key before concluding "never
                // pruned" (see LegacyPrunedFloorKeyUtf8). A valid legacy floor is adopted and
                // copied to the current key; a wrong-length legacy value fails closed below, the
                // same as a wrong-length current one.
                if (data is null)
                {
                    data = db.Get(LegacyPrunedFloorKeyUtf8, cf: columnFamilyKeys);

                    if (data is not null && data.Length == PrunedFloorSize)
                    {
                        // Best-effort migration: a lost write only means the next load falls back
                        // to the legacy key again, so a Put failure is not a floor failure.
                        try { db.Put(PrunedFloorKeyUtf8, data, cf: columnFamilyKeys); }
                        catch { /* re-migrated on the next load */ }
                    }
                }

                if (data is null)
                    _prunedFloorCache = HLCTimestamp.Zero; // never pruned
                else if (data.Length == PrunedFloorSize)
                    _prunedFloorCache = UnpackPrunedFloor(data);
                else
                    _prunedFloorCorrupt = true;
            }
            catch
            {
                _prunedFloorCorrupt = true;
            }
        }
        return _prunedFloorCorrupt ? FailClosedFloor : _prunedFloorCache!.Value;
    }

    // Adds a floor Put to the delete batch when the candidate advances the floor, so the floor lands
    // atomically with the deletes. Returns the value to commit to the in-memory cache after the batch
    // is durably written, or Zero when nothing was staged.
    private HLCTimestamp StagePrunedFloor(WriteBatch batch, HLCTimestamp candidate)
    {
        lock (_floorLock)
        {
            HLCTimestamp current = CurrentPrunedFloorLocked();
            
            if (_prunedFloorCorrupt || candidate.CompareTo(current) <= 0)
                return HLCTimestamp.Zero;
            
            batch.Put(PrunedFloorKeyUtf8, PackPrunedFloor(candidate), cf: columnFamilyKeys);
            return candidate;
        }
    }

    private void CommitPrunedFloor(HLCTimestamp value)
    {
        lock (_floorLock)
        {
            if (!_prunedFloorCorrupt && (_prunedFloorCache is null || value.CompareTo(_prunedFloorCache.Value) > 0))
                _prunedFloorCache = value;
        }
    }

    private static byte[] PackPrunedFloor(HLCTimestamp t)
    {
        byte[] b = new byte[PrunedFloorSize];
        BinaryPrimitives.WriteInt64LittleEndian(b.AsSpan(0, 8), t.N);
        BinaryPrimitives.WriteInt64LittleEndian(b.AsSpan(8, 8), t.L);
        BinaryPrimitives.WriteInt64LittleEndian(b.AsSpan(16, 8), t.C);
        return b;
    }

    private static HLCTimestamp UnpackPrunedFloor(ReadOnlySpan<byte> b) =>
        new((int)BinaryPrimitives.ReadInt64LittleEndian(b[..8]),
            BinaryPrimitives.ReadInt64LittleEndian(b.Slice(8, 8)),
            (uint)BinaryPrimitives.ReadInt64LittleEndian(b.Slice(16, 8)));

    /// <summary>
    /// Metadata-key prefix for per-partition application-durability floors. The leading space sorts
    /// the rows before every real key, keeping them out of prefix/range scans. (The PITR
    /// pruned-history floor used this idiom too before it moved to a '\0' prefix; these rows keep
    /// the space so existing stores read their floors unchanged.)
    /// </summary>
    private const string DurabilityFloorKeyPrefix = " durability_floor_";

    public bool StoreDurabilityFloors(IReadOnlyList<(int PartitionId, long Floor)> floors)
    {
        EnterDbFence();
        try
        {
            return StoreDurabilityFloorsFenced(floors);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private bool StoreDurabilityFloorsFenced(IReadOnlyList<(int PartitionId, long Floor)> floors)
    {
        using WriteBatch batch = new();

        Span<byte> value = stackalloc byte[8];

        foreach ((int partitionId, long floor) in floors)
        {
            byte[] key = Encoding.UTF8.GetBytes(DurabilityFloorKeyPrefix + partitionId);

            // Monotonic guard (parity with the SQLite backend's MAX() upsert): the caller's
            // advanced-only filter resets with the process, so the first write after a restart can
            // carry a floor below the persisted one — replay re-registers from the checkpoint, and
            // the fresh watermark starts under the previous run's floor. Regressing the persisted
            // floor is stale-low-safe but widens the next restart's replay for nothing.
            byte[]? current = db.Get(key, cf: columnFamilyKeys);
            if (current is { Length: 8 } && BinaryPrimitives.ReadInt64LittleEndian(current) >= floor)
                continue;

            BinaryPrimitives.WriteInt64LittleEndian(value, floor);
            batch.Put(key, value, cf: columnFamilyKeys);
        }

        // The store contract is "false on failure", never a throw: the background writer's
        // failure path re-parks failed batches for retry, and it keys off the return value.
        // A RocksDbException here (e.g. ENOSPC) escaping instead of returning false is what
        // silently dropped batches and froze durability floors under a full disk.
        try
        {
            db.Write(batch, DefaultWriteOptions);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RocksDb write of {Count} durability floors failed", floors.Count);
            return false;
        }

        return true;
    }

    public long GetDurabilityFloor(int partitionId)
    {
        EnterDbFence();
        try
        {
            byte[]? data = db.Get(Encoding.UTF8.GetBytes(DurabilityFloorKeyPrefix + partitionId), cf: columnFamilyKeys);

            return data is { Length: 8 } ? BinaryPrimitives.ReadInt64LittleEndian(data) : -1;
        }
        finally
        {
            ExitDbFence();
        }
    }

    public bool RemoveDurabilityFloor(int partitionId)
    {
        EnterDbFence();
        try
        {
            db.Remove(Encoding.UTF8.GetBytes(DurabilityFloorKeyPrefix + partitionId), cf: columnFamilyKeys);
            return true;
        }
        finally
        {
            ExitDbFence();
        }
    }

    /// <summary>
    /// Stores a batch of lock-related items into the RocksDB database within the designated column family.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> representing the locks and their associated metadata to be stored.</param>
    /// <returns>Returns <c>true</c> if the operation completes successfully.</returns>
    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        EnterDbFence();
        try
        {
            return StoreLocksFenced(items);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private bool StoreLocksFenced(List<PersistenceRequestItem> items)
    {
        using WriteBatch batch = new();

        // The list is not mutated while it is being written out, so iterate its backing storage by
        // reference — each item is a wide struct and a by-value foreach would copy every one.
        // One message shell serves every serialization in the batch; the reset restores every
        // field so an owner-less record cannot inherit the prior record's owner.
        RocksDbLockMessage kvm = new();

        foreach (ref readonly PersistenceRequestItem item in CollectionsMarshal.AsSpan(items))
        {
            ResetLockMessage(kvm);

            kvm.ExpiresPhysical = item.ExpiresPhysical;
            kvm.ExpiresCounter = item.ExpiresCounter;
            kvm.LastUsedPhysical = item.LastUsedPhysical;
            kvm.LastUsedCounter = item.LastUsedCounter;
            kvm.LastModifiedPhysical = item.LastModifiedPhysical;
            kvm.LastModifiedCounter = item.LastModifiedCounter;
            kvm.FencingToken = item.Revision;
            kvm.State = item.State;

            if (item.Value != null)
                kvm.Owner = UnsafeByteOperations.UnsafeWrap(item.Value);

            PutLocksItems(batch, in item, kvm, columnFamilyLocks);
        }

        // "false on failure" contract — see StoreDurabilityFloors for the reasoning.
        try
        {
            db.Write(batch, DefaultWriteOptions);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RocksDb write of {Count} locks failed", items.Count);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Inserts lock items into the RocksDB database using the specified write batch, item data,
    /// lock message, and column family handle.
    /// </summary>
    /// <param name="batch">The write batch used for batching multiple write operations.</param>
    /// <param name="item">The lock item containing key, revision, and additional metadata.</param>
    /// <param name="kvm">The RocksDB lock message to be serialized and persisted.</param>
    /// <param name="columnFamily">The column family handle identifying the column family where the data is stored.</param>
    private static void PutLocksItems(WriteBatch batch, in PersistenceRequestItem item, RocksDbLockMessage kvm, ColumnFamilyHandle columnFamily)
    {
        int serializedSize = kvm.CalculateSize();
        int keyLen = Encoding.UTF8.GetByteCount(item.Key);
        // The ~<revision> key is the larger of the two indices; one bounded buffer serves both.
        // 21 = '~'(1) + 20 digits (long.MinValue = -9223372036854775808 is 20 chars including '-').
        int maxLen = keyLen + 21;

        byte[]? rentedKey = null;
        byte[] rentedSer = ArrayPool<byte>.Shared.Rent(serializedSize);
        
        Span<byte> buffer = maxLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedKey = ArrayPool<byte>.Shared.Rent(maxLen));
        
        try
        {
            using (CodedOutputStream cos = new(rentedSer))
                kvm.WriteTo(cos);

            ReadOnlySpan<byte> serialized = rentedSer.AsSpan(0, serializedSize);

            // ~CURRENT key: encode key + CurrentMarker suffix without an intermediate string.
            System.Diagnostics.Debug.Assert(CurrentMarker.Length == CurrentMarkerUtf8.Length,
                "CurrentMarker string and CurrentMarkerUtf8 span have drifted — update the u8 literal");
            
            Encoding.UTF8.GetBytes(item.Key, buffer);
            CurrentMarkerUtf8.CopyTo(buffer[keyLen..]);
            batch.Put(buffer[..(keyLen + CurrentMarkerUtf8.Length)], serialized, cf: columnFamily);

            // ~<revision> key: reuse the key bytes already at buffer[0..keyLen), overwrite the suffix.
            buffer[keyLen] = (byte)'~';
            bool formatted = Utf8Formatter.TryFormat(item.Revision, buffer[(keyLen + 1)..], out int revLen);
            System.Diagnostics.Debug.Assert(formatted, "Utf8Formatter.TryFormat failed for revision key");
            batch.Put(buffer[..(keyLen + 1 + revLen)], serialized, cf: columnFamily);
        }
        finally
        {
            if (rentedKey is not null)
                ArrayPool<byte>.Shared.Return(rentedKey);
            
            ArrayPool<byte>.Shared.Return(rentedSer);
        }
    }

    /// <summary>
    /// Stores a collection of key-value pairs and their associated metadata into the persistence backend.
    /// The current row per key is advanced monotonically by (revision, commit HLC): the same committed
    /// mutation is queued independently by the owning actor and the Raft consumer, so a delayed older
    /// duplicate can arrive after a newer head — in the same batch or a later one — and must never
    /// regress what a read serves as current. Older records still land as retained history.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> objects, each representing key-value pairs and metadata to be stored.</param>
    /// <returns>Returns <c>true</c> if the operation is successfully completed.</returns>
    public bool StoreKeyValues(List<PersistenceRequestItem> items)
    {
        EnterDbFence();
        try
        {
            return StoreKeyValuesFenced(items);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private bool StoreKeyValuesFenced(List<PersistenceRequestItem> items)
    {
        using WriteBatch batch = new();

        // Batch-local no-revision provenance so multiple no-revision writes to the same key within one
        // batch merge correctly (a db.Get would not see this batch's own not-yet-committed puts).
        Dictionary<string, (HLCTimestamp Earliest, HLCTimestamp Latest)>? pendingNoRev = null;

        Span<PersistenceRequestItem> span = CollectionsMarshal.AsSpan(items);

        // The newest candidate per logical key competes for the current row; the newest candidate per
        // (key, revision) owns that retained-history row. Both orderings are revision-first with the
        // commit HLC as the same-revision tiebreak, because delete and extend records legitimately
        // reuse a revision number with a newer commit HLC.
        Dictionary<string, int> currentCandidates = new(span.Length, StringComparer.Ordinal);
        Dictionary<(string Key, long Revision), int> historyCandidates = new(span.Length);

        for (int i = 0; i < span.Length; i++)
        {
            ref readonly PersistenceRequestItem item = ref span[i];

            if (!currentCandidates.TryGetValue(item.Key, out int currentIdx)
                || IsNewerCandidate(in item, in span[currentIdx]))
                currentCandidates[item.Key] = i;

            // No-revision writes keep no history row.
            if (!item.NoRevision)
            {
                (string Key, long Revision) historyKey = (item.Key, item.Revision);
                if (!historyCandidates.TryGetValue(historyKey, out int historyIdx)
                    || CompareLastModified(in item, in span[historyIdx]) > 0)
                    historyCandidates[historyKey] = i;
            }
        }

        // One native point read per distinct key resolves the durable heads for the batch.
        // All flush-path stores are serialized on the writer queue and the install/restore callers
        // run quiesced, so the read-compare-write below is not racing another store for these keys.
        Dictionary<string, StoredKeyValueOrdering> durableHeads;
        try
        {
            durableHeads = ReadCurrentHeads(currentCandidates.Keys);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RocksDb read of {Count} current heads failed", currentCandidates.Count);
            return false;
        }

        // Row plan: which physical rows each item writes. Decided before serialization so each
        // written item is serialized exactly once even when it owns both of its rows.
        bool[] writeCurrent = new bool[span.Length];
        bool[] writeHistory = new bool[span.Length];

        foreach (KeyValuePair<string, int> candidate in currentCandidates)
        {
            ref readonly PersistenceRequestItem item = ref span[candidate.Value];

            if (!durableHeads.TryGetValue(candidate.Key, out StoredKeyValueOrdering head)
                || IsNewerThanStored(in item, in head))
                writeCurrent[candidate.Value] = true;
        }

        // History rows above the durable head cannot exist yet (the head is at or above every row
        // ever written), so they are written unconditionally. A record at or below the head may
        // collide with a durably newer same-revision row (set superseded by delete/extend reusing
        // the revision), so those few are read — ordering fields only, in place over native
        // memory — before being written.
        byte[]? rentedHistoryKey = null;
        Span<byte> historyKeyBuffer = stackalloc byte[KeyStackThreshold];

        try
        {
            foreach (KeyValuePair<(string Key, long Revision), int> candidate in historyCandidates)
            {
                if (!durableHeads.TryGetValue(candidate.Key.Key, out StoredKeyValueOrdering head)
                    || candidate.Key.Revision > head.Revision)
                {
                    writeHistory[candidate.Value] = true;
                    continue;
                }

                int keyLen = Encoding.UTF8.GetByteCount(candidate.Key.Key);
                // 21 = '~'(1) + 20 digits (long.MinValue = -9223372036854775808 is 20 chars including '-').
                int maxLen = keyLen + 21;

                Span<byte> rowKey = historyKeyBuffer;
                if (maxLen > rowKey.Length)
                {
                    if (rentedHistoryKey is null || rentedHistoryKey.Length < maxLen)
                    {
                        if (rentedHistoryKey is not null)
                            ArrayPool<byte>.Shared.Return(rentedHistoryKey);
                        rentedHistoryKey = ArrayPool<byte>.Shared.Rent(maxLen);
                    }

                    rowKey = rentedHistoryKey;
                }

                Encoding.UTF8.GetBytes(candidate.Key.Key, rowKey);
                rowKey[keyLen] = (byte)'~';
                bool formatted = Utf8Formatter.TryFormat(candidate.Key.Revision, rowKey[(keyLen + 1)..], out int revLen);
                System.Diagnostics.Debug.Assert(formatted, "Utf8Formatter.TryFormat failed for guarded history row key");

                StoredKeyValueOrderingLookup stored = db.Get(
                    rowKey[..(keyLen + 1 + revLen)], StoredKeyValueOrderingDeserializer.Instance, cf: columnFamilyKeys);

                StoredKeyValueOrdering storedOrdering = stored.Ordering;
                if (!stored.Found || CompareLastModifiedToStored(in span[candidate.Value], in storedOrdering) > 0)
                    writeHistory[candidate.Value] = true;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RocksDb read of guarded history rows failed");
            return false;
        }
        finally
        {
            if (rentedHistoryKey is not null)
                ArrayPool<byte>.Shared.Return(rentedHistoryKey);
        }

        // One message shell serves every serialization in the batch; PutStoreItems refills it fully.
        RocksDbKeyValueMessage reusableMessage = new();

        for (int i = 0; i < span.Length; i++)
        {
            ref readonly PersistenceRequestItem item = ref span[i];

            if (writeCurrent[i] || writeHistory[i])
                PutStoreItems(batch, in item, reusableMessage, writeCurrent[i], writeHistory[i], columnFamilyKeys);

            if (item.NoRevision)
                RecordNoRevisionWrite(batch, item.Key,
                    new(item.LastModifiedNode, item.LastModifiedPhysical, (uint)item.LastModifiedCounter),
                    pendingNoRev ??= new(StringComparer.Ordinal));
        }

        // "false on failure" contract — see StoreDurabilityFloors for the reasoning.
        try
        {
            db.Write(batch, DefaultWriteOptions);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RocksDb write of {Count} key-values failed", items.Count);
            return false;
        }

        return true;
    }

    /// <summary>Batch-candidate ordering for the current row: revision first, commit HLC tiebreak.</summary>
    private static bool IsNewerCandidate(in PersistenceRequestItem a, in PersistenceRequestItem b) =>
        a.Revision != b.Revision ? a.Revision > b.Revision : CompareLastModified(in a, in b) > 0;

    /// <summary>Commit-HLC comparison between two batch items, in physical → counter → node order.</summary>
    private static int CompareLastModified(in PersistenceRequestItem a, in PersistenceRequestItem b)
    {
        if (a.LastModifiedPhysical != b.LastModifiedPhysical)
            return a.LastModifiedPhysical < b.LastModifiedPhysical ? -1 : 1;

        if (a.LastModifiedCounter != b.LastModifiedCounter)
            return a.LastModifiedCounter < b.LastModifiedCounter ? -1 : 1;

        return a.LastModifiedNode.CompareTo(b.LastModifiedNode);
    }

    /// <summary>
    /// Whether a candidate advances a stored row by (revision, commit HLC). Rows written before the
    /// node component was persisted decode it as zero; the node is only the final tiebreak, so at
    /// worst such a row is re-written once with identical logical content.
    /// </summary>
    private static bool IsNewerThanStored(in PersistenceRequestItem item, in StoredKeyValueOrdering stored) =>
        item.Revision != stored.Revision
            ? item.Revision > stored.Revision
            : CompareLastModifiedToStored(in item, in stored) > 0;

    /// <summary>Commit-HLC comparison between a batch item and a stored row, in physical → counter → node order.</summary>
    private static int CompareLastModifiedToStored(in PersistenceRequestItem item, in StoredKeyValueOrdering stored)
    {
        if (item.LastModifiedPhysical != stored.LastModifiedPhysical)
            return item.LastModifiedPhysical < stored.LastModifiedPhysical ? -1 : 1;

        if (item.LastModifiedCounter != stored.LastModifiedCounter)
            return item.LastModifiedCounter < stored.LastModifiedCounter ? -1 : 1;

        return item.LastModifiedNode.CompareTo(stored.LastModifiedNode);
    }

    /// <summary>
    /// Reads the ordering fields of the current row for every key in <paramref name="keys"/>.
    /// Keys with no current row are absent from the result. Each lookup is a native point get
    /// whose deserializer decodes revision and commit HLC in place over the native value memory:
    /// no row key, payload, or scaffolding array is allocated. A MultiGet batch is deliberately
    /// not used here — its managed wrapper marshals every value into a fresh array, which is the
    /// exact copy these compares never need.
    /// </summary>
    private Dictionary<string, StoredKeyValueOrdering> ReadCurrentHeads(IReadOnlyCollection<string> keys)
    {
        Dictionary<string, StoredKeyValueOrdering> heads = new(keys.Count, StringComparer.Ordinal);

        if (keys.Count == 0)
            return heads;

        byte[]? rented = null;
        Span<byte> rowKeyBuffer = stackalloc byte[KeyStackThreshold];

        try
        {
            foreach (string key in keys)
            {
                int keyLen = Encoding.UTF8.GetByteCount(key);
                int rowKeyLen = keyLen + CurrentMarkerUtf8.Length;

                Span<byte> rowKey = rowKeyBuffer;
                if (rowKeyLen > rowKey.Length)
                {
                    if (rented is null || rented.Length < rowKeyLen)
                    {
                        if (rented is not null)
                            ArrayPool<byte>.Shared.Return(rented);
                        rented = ArrayPool<byte>.Shared.Rent(rowKeyLen);
                    }

                    rowKey = rented;
                }

                Encoding.UTF8.GetBytes(key, rowKey);
                CurrentMarkerUtf8.CopyTo(rowKey[keyLen..]);

                StoredKeyValueOrderingLookup lookup = db.Get(
                    rowKey[..rowKeyLen], StoredKeyValueOrderingDeserializer.Instance, cf: columnFamilyKeys);

                if (lookup.Found)
                    heads[key] = lookup.Ordering;
            }
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }

        return heads;
    }


    // Merges a no-revision write's HLC into the key's persisted (earliest, latest) provenance and
    // queues the updated marker onto the batch.
    private void RecordNoRevisionWrite(
        WriteBatch batch, string key, HLCTimestamp writeHlc,
        Dictionary<string, (HLCTimestamp Earliest, HLCTimestamp Latest)> pending)
    {
        if (pending.TryGetValue(key, out (HLCTimestamp Earliest, HLCTimestamp Latest) span))
        {
            // already have a merged value for this key in this batch
        }
        else
        {
            byte[] markerKey = BuildNoRevKey(key);
            byte[]? existing = db.Get(markerKey, cf: columnFamilyKeys);
            span = TryUnpackNoRev(existing, out HLCTimestamp e, out HLCTimestamp l) ? (e, l) : (HLCTimestamp.Zero, HLCTimestamp.Zero);
        }

        HLCTimestamp earliest = span.Earliest == HLCTimestamp.Zero || writeHlc.CompareTo(span.Earliest) < 0 ? writeHlc : span.Earliest;
        HLCTimestamp latest = writeHlc.CompareTo(span.Latest) > 0 ? writeHlc : span.Latest;
        pending[key] = (earliest, latest);

        batch.Put(BuildNoRevKey(key), PackNoRev(earliest, latest), cf: columnFamilyKeys);
    }

    private static byte[] BuildNoRevKey(string logicalKey)
    {
        int keyLen = Encoding.UTF8.GetByteCount(logicalKey);
        byte[] buffer = new byte[keyLen + NoRevMarkerUtf8.Length];
        Encoding.UTF8.GetBytes(logicalKey, buffer);
        NoRevMarkerUtf8.CopyTo(buffer.AsSpan(keyLen));
        return buffer;
    }

    private static byte[] PackNoRev(HLCTimestamp earliest, HLCTimestamp latest)
    {
        byte[] buf = new byte[NoRevProvenanceSize];
        Span<byte> s = buf;
        BinaryPrimitives.WriteInt64LittleEndian(s[..8], earliest.N);
        BinaryPrimitives.WriteInt64LittleEndian(s[8..16], earliest.L);
        BinaryPrimitives.WriteInt64LittleEndian(s[16..24], earliest.C);
        BinaryPrimitives.WriteInt64LittleEndian(s[24..32], latest.N);
        BinaryPrimitives.WriteInt64LittleEndian(s[32..40], latest.L);
        BinaryPrimitives.WriteInt64LittleEndian(s[40..48], latest.C);
        return buf;
    }

    private static bool TryUnpackNoRev(byte[]? data, out HLCTimestamp earliest, out HLCTimestamp latest)
    {
        earliest = HLCTimestamp.Zero;
        latest = HLCTimestamp.Zero;
        
        if (data is null || data.Length < NoRevProvenanceSize)
            return false;

        ReadOnlySpan<byte> s = data;
        
        earliest = new(
            (int)BinaryPrimitives.ReadInt64LittleEndian(s[..8]),
            BinaryPrimitives.ReadInt64LittleEndian(s[8..16]),
            (uint)BinaryPrimitives.ReadInt64LittleEndian(s[16..24]));
        
        latest = new(
            (int)BinaryPrimitives.ReadInt64LittleEndian(s[24..32]),
            BinaryPrimitives.ReadInt64LittleEndian(s[32..40]),
            (uint)BinaryPrimitives.ReadInt64LittleEndian(s[40..48]));
        
        return true;
    }

    /// <summary>
    /// Serializes one key-value record and adds its requested physical rows to the WriteBatch:
    /// the ~CURRENT marker row when the record won the batch's monotonic current-head advance,
    /// and the ~&lt;revision&gt; history row when the record owns that retained revision. The
    /// caller decides both flags; at least one must be set.
    /// </summary>
    /// <param name="batch">
    /// The WriteBatch used to batch operations for writing into RocksDB.
    /// </param>
    /// <param name="item">
    /// The persistence request item containing the key and revision data used for creating the entry.
    /// </param>
    /// <param name="kvm">Caller-owned reusable message shell; fully reset and refilled per item.</param>
    /// <param name="putCurrent">Whether to write the ~CURRENT marker row.</param>
    /// <param name="putHistory">Whether to write the ~&lt;revision&gt; history row.</param>
    /// <param name="columnFamily">
    /// The RocksDB column family handle where the key-value pair will be stored.
    /// </param>
    private static void PutStoreItems(WriteBatch batch, in PersistenceRequestItem item, RocksDbKeyValueMessage kvm, bool putCurrent, bool putHistory, ColumnFamilyHandle columnFamily)
    {
        // The shell arrives dirty from the previous item; the reset restores every field so a
        // value-less record cannot inherit the prior record's payload.
        ResetKeyValueMessage(kvm);

        kvm.ExpiresNode = item.ExpiresNode;
        kvm.ExpiresPhysical = item.ExpiresPhysical;
        kvm.ExpiresCounter = item.ExpiresCounter;
        kvm.LastUsedNode = item.LastUsedNode;
        kvm.LastUsedPhysical = item.LastUsedPhysical;
        kvm.LastUsedCounter = item.LastUsedCounter;
        kvm.LastModifiedNode = item.LastModifiedNode;
        kvm.LastModifiedPhysical = item.LastModifiedPhysical;
        kvm.LastModifiedCounter = item.LastModifiedCounter;
        kvm.Revision = item.Revision;
        kvm.State = item.State;

        if (item.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(item.Value);

        int serializedSize = kvm.CalculateSize();
        int keyLen = Encoding.UTF8.GetByteCount(item.Key);
        // The ~<revision> key is the larger of the two indices; one bounded buffer serves both.
        // 21 = '~'(1) + 20 digits (long.MinValue = -9223372036854775808 is 20 chars including '-').
        int maxLen = keyLen + 21;

        byte[]? rentedKey = null;
        byte[] rentedSer = ArrayPool<byte>.Shared.Rent(serializedSize);

        Span<byte> buffer = maxLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedKey = ArrayPool<byte>.Shared.Rent(maxLen));
        try
        {
            using (CodedOutputStream cos = new(rentedSer))
                kvm.WriteTo(cos);

            ReadOnlySpan<byte> serialized = rentedSer.AsSpan(0, serializedSize);

            Encoding.UTF8.GetBytes(item.Key, buffer);

            // ~CURRENT key: encode key + CurrentMarker suffix without an intermediate string.
            if (putCurrent)
            {
                System.Diagnostics.Debug.Assert(CurrentMarker.Length == CurrentMarkerUtf8.Length,
                    "CurrentMarker string and CurrentMarkerUtf8 span have drifted — update the u8 literal");
                CurrentMarkerUtf8.CopyTo(buffer[keyLen..]);
                batch.Put(buffer[..(keyLen + CurrentMarkerUtf8.Length)], serialized, cf: columnFamily);
            }

            // ~<revision> key: never written for no-revision writes — only the current value is kept.
            if (putHistory)
            {
                System.Diagnostics.Debug.Assert(!item.NoRevision, "a no-revision write must not own a history row");
                buffer[keyLen] = (byte)'~';
                bool formatted = Utf8Formatter.TryFormat(item.Revision, buffer[(keyLen + 1)..], out int revLen);
                System.Diagnostics.Debug.Assert(formatted, "Utf8Formatter.TryFormat failed for revision key");
                batch.Put(buffer[..(keyLen + 1 + revLen)], serialized, cf: columnFamily);
            }
        }
        finally
        {
            if (rentedKey is not null)
                ArrayPool<byte>.Shared.Return(rentedKey);
            
            ArrayPool<byte>.Shared.Return(rentedSer);
        }
    }

    /// <summary>
    /// Retrieves the lock context associated with the specified resource.
    /// </summary>
    /// <param name="resource">The unique identifier of the resource for which the lock context is being retrieved.</param>
    /// <returns>
    /// A <see cref="LockEntry"/> instance containing details about the lock if it exists;
    /// otherwise, null if no lock is associated with the specified resource.
    /// </returns>
    public LockEntry? GetLock(string resource)
    {
        EnterDbFence();
        try
        {
            return GetLockFenced(resource);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private LockEntry? GetLockFenced(string resource)
    {
        int keyLen = Encoding.UTF8.GetByteCount(resource);
        int totalLen = keyLen + CurrentMarkerUtf8.Length;

        byte[]? rented = null;
        
        Span<byte> buffer = totalLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(totalLen));
        
        try
        {
            buffer = buffer[..totalLen];
            Encoding.UTF8.GetBytes(resource.AsSpan(), buffer);
            CurrentMarkerUtf8.CopyTo(buffer[keyLen..]);

            byte[]? value = db.Get(buffer, cf: columnFamilyLocks);
            if (value is null)
                return null;

            RocksDbLockMessage message = UnserializeLockMessageThreadCached(value);

            byte[]? owner;

            if (MemoryMarshal.TryGetArray(message.Owner.Memory, out ArraySegment<byte> segment))
                owner = segment.Array;
            else
                owner = message.Owner.ToByteArray();

            return new()
            {
                Owner = owner,
                FencingToken = message.FencingToken,
                Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
                LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
                LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
                State = (LockState)message.State
            };
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Retrieves the key-value context associated with the specified key name. If the key does not exist,
    /// the method returns null.
    /// </summary>
    /// <param name="keyName">The name of the key to retrieve the associated key-value context.</param>
    /// <returns>
    /// A <see cref="KeyValueEntry"/> object containing the value, revision, expiration details, and other metadata
    /// associated with the key, or null if the key does not exist.
    /// </returns>
    public KeyValueEntry? GetKeyValue(string keyName)
    {
        EnterDbFence();
        try
        {
            return GetKeyValueFenced(keyName);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private KeyValueEntry? GetKeyValueFenced(string keyName)
    {
        int keyLen = Encoding.UTF8.GetByteCount(keyName);
        int totalLen = keyLen + CurrentMarkerUtf8.Length;

        byte[]? rented = null;
        Span<byte> buffer = totalLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(totalLen));
        try
        {
            buffer = buffer[..totalLen];
            Encoding.UTF8.GetBytes(keyName.AsSpan(), buffer);
            CurrentMarkerUtf8.CopyTo(buffer[keyLen..]);

            byte[]? value = db.Get(buffer, cf: columnFamilyKeys);
            if (value is null)
                return null;

            return DecodeCurrentKeyValue(value);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Decodes a serialized current-marker row from the keys column family into a
    /// <see cref="KeyValueEntry"/>. Shared by <see cref="GetKeyValue"/> and
    /// <see cref="GetKeyValues"/> so single and batched point reads cannot drift.
    /// </summary>
    private static KeyValueEntry DecodeCurrentKeyValue(byte[] value)
    {
        RocksDbKeyValueMessage message = UnserializeKeyValueMessageThreadCached(value);

        // The writer leaves the value field unset for a key that holds no value, so presence is what
        // separates that from a key holding zero bytes. The SQLite backend keeps the two apart (it stores
        // NULL), and a reopen must not be the step that quietly merges them.
        byte[]? messageValue = ByteStringPayload.GetArrayOrNull(message.HasValue, message.Value);

        return new()
        {
            Value = messageValue,
            Revision = message.Revision,
            Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
            LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
            LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
            State = (KeyValueState)message.State,
        };
    }

    /// <summary>
    /// Batched point lookup via a single RocksDB <c>MultiGet</c>: one native call resolves every
    /// key in <paramref name="keyNames"/> (bloom-filter and block-cache probes are batched inside
    /// RocksDB), instead of N individual <c>Get</c>s. Results are index-aligned; a missing key
    /// yields <c>null</c> at its slot.
    /// </summary>
    public KeyValueEntry?[] GetKeyValues(string[] keyNames)
    {
        EnterDbFence();
        try
        {
            return GetKeyValuesFenced(keyNames);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private KeyValueEntry?[] GetKeyValuesFenced(string[] keyNames)
    {
        byte[][] keys = new byte[keyNames.Length][];

        for (int i = 0; i < keyNames.Length; i++)
        {
            string keyName = keyNames[i];
            int keyLen = Encoding.UTF8.GetByteCount(keyName);

            byte[] key = new byte[keyLen + CurrentMarkerUtf8.Length];
            Encoding.UTF8.GetBytes(keyName.AsSpan(), key);
            CurrentMarkerUtf8.CopyTo(key.AsSpan(keyLen));

            keys[i] = key;
        }

        ColumnFamilyHandle[] families = new ColumnFamilyHandle[keys.Length];
        Array.Fill(families, columnFamilyKeys);

        // RocksDbSharp returns one pair per input key, in input order; a missing key carries a
        // null Value.
        KeyValuePair<byte[], byte[]>[] values = db.MultiGet(keys, families);

        KeyValueEntry?[] results = new KeyValueEntry?[keyNames.Length];

        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].Value is not null)
                results[i] = DecodeCurrentKeyValue(values[i].Value);
        }

        return results;
    }

    /// <summary>
    /// Retrieves a key-value revision based on the specified key name and revision number.
    /// </summary>
    /// <param name="keyName">The name of the key to retrieve.</param>
    /// <param name="revision">The specific revision number of the key to retrieve.</param>
    /// <returns>
    /// A <see cref="KeyValueEntry"/> object containing the key-value pair metadata and value,
    /// or <c>null</c> if the key or revision is not found.
    /// </returns>
    public KeyValueEntry? GetKeyValueRevision(string keyName, long revision)
    {
        EnterDbFence();
        try
        {
            return GetKeyValueRevisionFenced(keyName, revision);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private KeyValueEntry? GetKeyValueRevisionFenced(string keyName, long revision)
    {
        int keyLen = Encoding.UTF8.GetByteCount(keyName);
        int maxLen = keyLen + 21; // '~'(1) + up to 20 decimal digits

        byte[]? rented = null;
        Span<byte> buffer = maxLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(maxLen));
        try
        {
            Encoding.UTF8.GetBytes(keyName, buffer);
            buffer[keyLen] = (byte)'~';
            bool formatted = Utf8Formatter.TryFormat(revision, buffer[(keyLen + 1)..], out int revLen);
            System.Diagnostics.Debug.Assert(formatted, "Utf8Formatter.TryFormat failed for revision lookup key");

            byte[]? value = db.Get(buffer[..(keyLen + 1 + revLen)], cf: columnFamilyKeys);
            if (value is null)
                return null;

            RocksDbKeyValueMessage message = UnserializeKeyValueMessageThreadCached(value);

            byte[]? messageValue = ByteStringPayload.GetArrayOrNull(message.HasValue, message.Value);

            return new()
            {
                Value = messageValue,
                Revision = message.Revision,
                Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
                LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
                LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
                State = (KeyValueState)message.State,
            };
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Scans the <c>keyName~{decimal}</c> revision rows for <paramref name="keyName"/> in one
    /// forward pass and returns the entry with the highest revision that satisfies both
    /// <c>revision ≤ maxRevision</c> and <c>LastModified ≤ readTimestamp</c>.
    /// Returns <c>null</c> when no qualifying retained revision exists.
    /// </summary>
    public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
    {
        EnterDbFence();
        try
        {
            return GetKeyValueRevisionAtOrBeforeFenced(keyName, maxRevision, readTimestamp);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private KeyValueEntry? GetKeyValueRevisionAtOrBeforeFenced(string keyName, long maxRevision, HLCTimestamp readTimestamp)
    {
        int keyLen = Encoding.UTF8.GetByteCount(keyName);
        int prefixLen = keyLen + 1; // keyName + '~'

        byte[]? rented = null;
        Span<byte> prefixBuffer = prefixLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(prefixLen));

        try
        {
            prefixBuffer = prefixBuffer[..prefixLen];
            Encoding.UTF8.GetBytes(keyName, prefixBuffer);
            prefixBuffer[keyLen] = (byte)'~';

            using Iterator? iterator = db.NewIterator(cf: columnFamilyKeys);
            iterator.Seek(prefixBuffer);

            KeyValueEntry? best = null;
            long bestRevision = -1;

            // One parse shell serves every row of the scan; fields are copied out per row.
            RocksDbKeyValueMessage shell = new();

            while (iterator.Valid())
            {
                ReadOnlySpan<byte> rawKey = iterator.GetKeySpan();

                if (!rawKey.StartsWith(prefixBuffer))
                    break;

                ReadOnlySpan<byte> revSuffix = rawKey[prefixLen..];

                // keyName's own current-marker row terminates the scan: all of keyName's revision rows
                // are decimal-suffixed and sort before "CURRENT" ('0'..'9' < 'C'). Keys may contain '~',
                // so sibling keys named "keyName~…" also share this prefix and contribute their own
                // "~CURRENT"/revision rows; only keyName's *exact* "CURRENT" suffix is the terminator —
                // a sibling's "~CURRENT" row must be skipped, not break the scan (it can sort before
                // keyName's own revisions and would otherwise drop them).
                if (revSuffix.SequenceEqual(CurrentMarkerUtf8[1..]))
                    break;

                // Only a suffix that is *entirely* a decimal number is one of keyName's own revision
                // rows. A partial parse — e.g. "2024~5" from a sibling key "keyName~2024" — must be
                // rejected, otherwise the sibling's row would be mis-attributed to keyName.
                if (!Utf8Parser.TryParse(revSuffix, out long revision, out int consumed) || consumed != revSuffix.Length)
                {
                    iterator.Next();
                    continue;
                }

                if (revision <= maxRevision)
                {
                    RocksDbKeyValueMessage message = UnserializeKeyValueMessage(iterator.GetValueSpan(), shell);
                    HLCTimestamp lastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter);

                    if (lastModified.CompareTo(readTimestamp) <= 0 && revision > bestRevision)
                    {
                        byte[]? messageValue = ByteStringPayload.GetArrayOrNull(message.HasValue, message.Value);

                        best = new()
                        {
                            Value = messageValue,
                            Revision = message.Revision,
                            Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
                            LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
                            LastModified = lastModified,
                            State = (KeyValueState)message.State,
                        };
                        bestRevision = revision;
                    }
                }

                iterator.Next();
            }

            return best;
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Retrieves a list of key-value pairs that match the specified prefix key.
    /// </summary>
    /// <param name="prefixKeyName">The prefix string used to filter and retrieve matching key-value pairs.</param>
    /// <returns>A list of tuples where each tuple contains a string key and a corresponding <see cref="ReadOnlyKeyValueEntry"/> value.</returns>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
    {
        EnterDbFence();
        try
        {
            return GetKeyValueByPrefixFenced(prefixKeyName);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefixFenced(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueEntry)> result = [];

        int prefixLen = Encoding.UTF8.GetByteCount(prefixKeyName);
        byte[]? rented = null;
        Span<byte> prefixBytes = prefixLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(prefixLen));
        try
        {
            prefixBytes = prefixBytes[..prefixLen];
            Encoding.UTF8.GetBytes(prefixKeyName, prefixBytes);

            using Iterator? iterator = db.NewIterator(cf: columnFamilyKeys);
            iterator.Seek(prefixBytes);

            // One parse shell serves every row of the scan; fields are copied out per row.
            RocksDbKeyValueMessage shell = new();

            while (iterator.Valid() && result.Count < KeyValueScanLimits.MaxPrefixScanResults)
            {
                // GetKeySpan returns a span directly over native memory — no byte[] copy.
                // Valid only until the next iterator move; consumed entirely before Next().
                ReadOnlySpan<byte> rawKey = iterator.GetKeySpan();

                if (!rawKey.StartsWith(prefixBytes))
                    break;

                if (!rawKey.EndsWith(CurrentMarkerUtf8))
                {
                    iterator.Next();
                    continue;
                }

                // Decode only keys that pass both filters.
                string keyWithoutMarker = Encoding.UTF8.GetString(rawKey[..^CurrentMarkerUtf8.Length]);

                RocksDbKeyValueMessage message = UnserializeKeyValueMessage(iterator.GetValueSpan(), shell);

                result.Add((keyWithoutMarker, DecodeReadOnlyKeyValue(message)));

                iterator.Next();
            }

            return result;
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Single-pass snapshot prefix scan. Current-head rows and revision-history rows share one
    /// physical key range in the keys column family, so the interface's default composition
    /// (prefix scan, then one <see cref="GetKeyValueRevisionAtOrBefore"/> per stale key) re-reads
    /// that range once per stale key — under deep revision chains the disk reads grow with
    /// chain depth times key count per scan. This override walks the range exactly once and
    /// resolves every key's as-of image from the rows of that same pass.
    /// </summary>
    public List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> GetKeyValueByPrefixAtOrBefore(
        string prefixKeyName, HLCTimestamp readTimestamp, Func<bool>? shouldAbort = null)
    {
        EnterDbFence();
        try
        {
            return GetKeyValueByPrefixAtOrBeforeFenced(prefixKeyName, readTimestamp, shouldAbort);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> GetKeyValueByPrefixAtOrBeforeFenced(
        string prefixKeyName, HLCTimestamp readTimestamp, Func<bool>? shouldAbort)
    {
        Dictionary<string, PrefixAsOfAccumulator> accumulators = new(StringComparer.Ordinal);

        int prefixLen = Encoding.UTF8.GetByteCount(prefixKeyName);
        byte[]? rented = null;
        Span<byte> prefixBytes = prefixLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(prefixLen));

        long rowsExamined = 0;
        bool aborted = false;

        try
        {
            prefixBytes = prefixBytes[..prefixLen];
            Encoding.UTF8.GetBytes(prefixKeyName, prefixBytes);

            using Iterator? iterator = db.NewIterator(cf: columnFamilyKeys);
            iterator.Seek(prefixBytes);

            // Rows of one logical key are usually adjacent, so cache the last decoded logical key
            // and reuse the string while consecutive rows still belong to it.
            byte[] lastLogicalKeyBytes = [];
            string lastLogicalKey = "";
            int currentRowsSeen = 0;

            // One parse shell serves every row of the scan; fields are copied out per row.
            RocksDbKeyValueMessage shell = new();

            while (iterator.Valid())
            {
                if ((rowsExamined & 1023) == 0 && shouldAbort is not null && shouldAbort())
                {
                    aborted = true;
                    break;
                }

                // GetKeySpan returns a span directly over native memory — no byte[] copy.
                // Valid only until the next iterator move; consumed entirely before Next().
                ReadOnlySpan<byte> rawKey = iterator.GetKeySpan();

                if (!rawKey.StartsWith(prefixBytes))
                    break;

                rowsExamined++;

                if (rawKey.EndsWith(CurrentMarkerUtf8))
                {
                    ReadOnlySpan<byte> logicalKeyBytes = rawKey[..^CurrentMarkerUtf8.Length];
                    PrefixAsOfAccumulator accumulator = ResolveAccumulator(
                        accumulators, logicalKeyBytes, ref lastLogicalKeyBytes, ref lastLogicalKey);

                    accumulator.Current = DecodeReadOnlyKeyValue(UnserializeKeyValueMessage(iterator.GetValueSpan(), shell));

                    // Result cap: the non-snapshot prefix scan returns the first
                    // MaxPrefixScanResults heads in key order and truncates the rest; mirror that.
                    if (++currentRowsSeen >= KeyValueScanLimits.MaxPrefixScanResults)
                        break;

                    iterator.Next();
                    continue;
                }

                // A revision-history row is "<logicalKey>~<decimal>". The suffix after the LAST
                // '~' must be entirely decimal: keys may themselves contain '~', and only the
                // last separator splits the owning key from its revision. "~NOREV" provenance
                // rows and any non-numeric suffix are skipped.
                int lastTilde = rawKey.LastIndexOf((byte)'~');
                if (lastTilde <= 0)
                {
                    iterator.Next();
                    continue;
                }

                ReadOnlySpan<byte> revSuffix = rawKey[(lastTilde + 1)..];
                if (!Utf8Parser.TryParse(revSuffix, out long revision, out int consumed) || consumed != revSuffix.Length)
                {
                    iterator.Next();
                    continue;
                }

                PrefixAsOfAccumulator revisionAccumulator = ResolveAccumulator(
                    accumulators, rawKey[..lastTilde], ref lastLogicalKeyBytes, ref lastLogicalKey);

                // The as-of pick may need the runner-up: when the head reuses the best row's
                // revision number (delete/extend reuse revisions) the best row is excluded by the
                // strictly-below-head bound and the second-best is the answer. A row that cannot
                // enter the top two by revision number can be skipped before deserialization.
                if (revisionAccumulator.Best is not null && revision <= revisionAccumulator.SecondRevision)
                {
                    iterator.Next();
                    continue;
                }

                RocksDbKeyValueMessage message = UnserializeKeyValueMessage(iterator.GetValueSpan(), shell);
                HLCTimestamp lastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter);

                if (lastModified.CompareTo(readTimestamp) <= 0)
                {
                    if (revisionAccumulator.Best is null || revision > revisionAccumulator.BestRevision)
                    {
                        revisionAccumulator.Second = revisionAccumulator.Best;
                        revisionAccumulator.SecondRevision = revisionAccumulator.BestRevision;
                        revisionAccumulator.Best = DecodeReadOnlyKeyValue(message);
                        revisionAccumulator.BestRevision = revision;
                    }
                    else if (revisionAccumulator.Second is null || revision > revisionAccumulator.SecondRevision)
                    {
                        revisionAccumulator.Second = DecodeReadOnlyKeyValue(message);
                        revisionAccumulator.SecondRevision = revision;
                    }
                }

                iterator.Next();
            }
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }

        List<(string, ReadOnlyKeyValueEntry, ReadOnlyKeyValueEntry?)> result = new(accumulators.Count);

        foreach ((string key, PrefixAsOfAccumulator accumulator) in accumulators)
        {
            // History rows without a head row: either the pass was truncated before reaching the
            // key's head, or the key's rows were physically removed mid-scan. The key is not part
            // of the head page in either case, matching the non-snapshot scan's truncation.
            if (accumulator.Current is null)
                continue;

            result.Add((key, accumulator.Current, ResolveSnapshot(accumulator, readTimestamp)));
        }

        result.Sort(static (a, b) => string.CompareOrdinal(a.Item1, b.Item1));

        KeyValueScanMetrics.SnapshotPrefixRowsExamined.Add(rowsExamined);
        KeyValueScanMetrics.SnapshotPrefixEntriesReturned.Add(result.Count);
        if (aborted)
            KeyValueScanMetrics.ScansAbandonedCancelled.Add(1);

        // Loud amplification signal: a scan that examines far more physical rows than it returns
        // is paying for revision history, and that cost grows with chain depth. Surfacing it here
        // makes the condition diagnosable without host-level disk counters.
        if (rowsExamined > 65_536 && rowsExamined > 16L * Math.Max(1, result.Count))
            logger.LogWarning(
                "Snapshot prefix scan over {Prefix} examined {Rows} physical rows to return {Results} entries; revision chains under this prefix are deep",
                prefixKeyName, rowsExamined, result.Count);

        return result;
    }

    /// <summary>
    /// Per-logical-key state for the single-pass as-of prefix scan: the head row plus the two
    /// highest-revision history rows at-or-before the read timestamp seen so far.
    /// </summary>
    private sealed class PrefixAsOfAccumulator
    {
        internal ReadOnlyKeyValueEntry? Current;
        internal ReadOnlyKeyValueEntry? Best;
        internal long BestRevision = -1;
        internal ReadOnlyKeyValueEntry? Second;
        internal long SecondRevision = -1;
    }

    /// <summary>
    /// Returns the accumulator for the logical key encoded in <paramref name="logicalKeyBytes"/>,
    /// reusing the previously decoded key string when consecutive rows belong to the same key.
    /// </summary>
    private static PrefixAsOfAccumulator ResolveAccumulator(
        Dictionary<string, PrefixAsOfAccumulator> accumulators,
        ReadOnlySpan<byte> logicalKeyBytes,
        ref byte[] lastLogicalKeyBytes,
        ref string lastLogicalKey)
    {
        if (!logicalKeyBytes.SequenceEqual(lastLogicalKeyBytes))
        {
            lastLogicalKeyBytes = logicalKeyBytes.ToArray();
            lastLogicalKey = Encoding.UTF8.GetString(logicalKeyBytes);
        }

        if (accumulators.TryGetValue(lastLogicalKey, out PrefixAsOfAccumulator? accumulator))
            return accumulator;

        accumulator = new();
        accumulators[lastLogicalKey] = accumulator;
        return accumulator;
    }

    /// <summary>
    /// Resolves a key's as-of image from its accumulated rows: the head itself when it is
    /// at-or-before the timestamp, otherwise the highest qualifying history row strictly below
    /// the head revision, or null when no committed version existed at the timestamp.
    /// </summary>
    private static ReadOnlyKeyValueEntry? ResolveSnapshot(PrefixAsOfAccumulator accumulator, HLCTimestamp readTimestamp)
    {
        ReadOnlyKeyValueEntry current = accumulator.Current!;

        if (current.LastModified.CompareTo(readTimestamp) <= 0)
            return current;

        long maxRevision = current.Revision - 1;

        ReadOnlyKeyValueEntry? snapshot = accumulator.BestRevision <= maxRevision
            ? accumulator.Best
            : (accumulator.SecondRevision <= maxRevision ? accumulator.Second : null);

        if (snapshot is null || snapshot.State is KeyValueState.Undefined)
            return null;

        return snapshot;
    }

    /// <summary>
    /// Decodes a deserialized keys-column-family row into a <see cref="ReadOnlyKeyValueEntry"/>.
    /// </summary>
    private static ReadOnlyKeyValueEntry DecodeReadOnlyKeyValue(RocksDbKeyValueMessage message)
    {
        byte[]? messageValue = ByteStringPayload.GetArrayOrNull(message.HasValue, message.Value);

        return new(
            messageValue,
            message.Revision,
            new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
            new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
            new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
            (KeyValueState)message.State);
    }

    /// <summary>
    /// Retrieves a bounded, ordered page of key-value pairs whose keys start with <paramref name="prefix"/>,
    /// beginning at <paramref name="startKey"/> (or the prefix start if null), up to <paramref name="limit"/> entries.
    /// </summary>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
    {
        EnterDbFence();
        try
        {
            return GetKeyValueByRangeFenced(prefix, startKey, limit);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRangeFenced(string prefix, string? startKey, int limit)
    {
        List<(string, ReadOnlyKeyValueEntry)> result = [];

        int prefixLen = Encoding.UTF8.GetByteCount(prefix);
        string seekStr = startKey ?? prefix;
        int seekLen = Encoding.UTF8.GetByteCount(seekStr);

        byte[]? rentedPrefix = null;
        Span<byte> prefixBytes = prefixLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedPrefix = ArrayPool<byte>.Shared.Rent(prefixLen));

        byte[]? rentedSeek = null;
        Span<byte> seekBytes = seekLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedSeek = ArrayPool<byte>.Shared.Rent(seekLen));

        try
        {
            prefixBytes = prefixBytes[..prefixLen];
            Encoding.UTF8.GetBytes(prefix, prefixBytes);

            seekBytes = seekBytes[..seekLen];
            Encoding.UTF8.GetBytes(seekStr, seekBytes);

            using Iterator iterator = db.NewIterator(cf: columnFamilyKeys);
            iterator.Seek(seekBytes);

            // One parse shell serves every row of the scan; fields are copied out per row.
            RocksDbKeyValueMessage shell = new();

            while (iterator.Valid() && result.Count < limit)
            {
                // GetKeySpan returns a span directly over native memory — no byte[] copy.
                // Valid only until the next iterator move; consumed entirely before Next().
                ReadOnlySpan<byte> rawKey = iterator.GetKeySpan();

                if (!rawKey.StartsWith(prefixBytes))
                    break;

                if (!rawKey.EndsWith(CurrentMarkerUtf8))
                {
                    iterator.Next();
                    continue;
                }

                // Decode only keys that pass both filters.
                string keyWithoutMarker = Encoding.UTF8.GetString(rawKey[..^CurrentMarkerUtf8.Length]);

                RocksDbKeyValueMessage message = UnserializeKeyValueMessage(iterator.GetValueSpan(), shell);

                result.Add((keyWithoutMarker, DecodeReadOnlyKeyValue(message)));

                iterator.Next();
            }

            return result;
        }
        finally
        {
            if (rentedPrefix is not null)
                ArrayPool<byte>.Shared.Return(rentedPrefix);
            if (rentedSeek is not null)
                ArrayPool<byte>.Shared.Return(rentedSeek);
        }
    }


    /// <summary>
    /// Whole-family paged scan of the current key-value rows: iterates the <c>kv</c> column family
    /// keeping only <c>~CURRENT</c> rows (revision, no-revision-provenance and internal cursor rows
    /// are skipped), returning keys strictly greater than the cursor — which is simply the last key
    /// of the previous page — in ordinal order.
    /// </summary>
    public KeyValueScanPage ScanKeyValues(string? cursor, int limit)
    {
        EnterDbFence();
        try
        {
            return ScanKeyValuesFenced(cursor, limit);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private KeyValueScanPage ScanKeyValuesFenced(string? cursor, int limit)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];

        using Iterator iterator = db.NewIterator(cf: columnFamilyKeys);

        if (cursor is null)
            iterator.SeekToFirst();
        else
            iterator.Seek(Encoding.UTF8.GetBytes(cursor));

        // One parse shell serves every row of the scan; fields are copied out per row.
        RocksDbKeyValueMessage shell = new();

        while (iterator.Valid() && items.Count < limit)
        {
            ReadOnlySpan<byte> rawKey = iterator.GetKeySpan();

            if (!rawKey.EndsWith(CurrentMarkerUtf8))
            {
                iterator.Next();
                continue;
            }

            string key = Encoding.UTF8.GetString(rawKey[..^CurrentMarkerUtf8.Length]);

            // The seek lands inside the cursor key's row block (its revision rows sort before the
            // marker row), so the cursor key itself comes back once more — skip it.
            if (cursor is not null && string.CompareOrdinal(key, cursor) <= 0)
            {
                iterator.Next();
                continue;
            }

            RocksDbKeyValueMessage message = UnserializeKeyValueMessage(iterator.GetValueSpan(), shell);

            items.Add((key, DecodeReadOnlyKeyValue(message)));

            iterator.Next();
        }

        return new(items, items.Count == limit ? items[^1].Item1 : null);
    }

    /// <summary>
    /// Whole-family paged scan of the lock rows: same marker filtering and cursor semantics as
    /// <see cref="ScanKeyValues"/>, over the <c>locks</c> column family.
    /// </summary>
    public LockScanPage ScanLocks(string? cursor, int limit)
    {
        EnterDbFence();
        try
        {
            return ScanLocksFenced(cursor, limit);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private LockScanPage ScanLocksFenced(string? cursor, int limit)
    {
        List<(string, LockEntry)> items = [];

        using Iterator iterator = db.NewIterator(cf: columnFamilyLocks);

        if (cursor is null)
            iterator.SeekToFirst();
        else
            iterator.Seek(Encoding.UTF8.GetBytes(cursor));

        while (iterator.Valid() && items.Count < limit)
        {
            ReadOnlySpan<byte> rawKey = iterator.GetKeySpan();

            if (!rawKey.EndsWith(CurrentMarkerUtf8))
            {
                iterator.Next();
                continue;
            }

            string resource = Encoding.UTF8.GetString(rawKey[..^CurrentMarkerUtf8.Length]);

            if (cursor is not null && string.CompareOrdinal(resource, cursor) <= 0)
            {
                iterator.Next();
                continue;
            }

            RocksDbLockMessage message = UnserializeLockMessageThreadCached(iterator.GetValueSpan());

            byte[]? owner;
            if (MemoryMarshal.TryGetArray(message.Owner.Memory, out ArraySegment<byte> segment))
                owner = segment.Array;
            else
                owner = message.Owner.ToByteArray();

            items.Add((resource, new()
            {
                Owner = owner,
                FencingToken = message.FencingToken,
                Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
                LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
                LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
                State = (LockState)message.State
            }));

            iterator.Next();
        }

        return new(items, items.Count == limit ? items[^1].Item1 : null);
    }

    /// <summary>
    /// Physically removes each key's rows — <c>~CURRENT</c>, every <c>~&lt;revision&gt;</c> history
    /// row and the <c>~NOREV</c> provenance marker all share the <c>key~</c> raw prefix, so one
    /// bounded prefix walk per key collects them all — in a single write batch.
    /// </summary>
    public bool DeleteKeyValues(IReadOnlyList<string> keys)
    {
        // The column-family handle is read under the fence: read outside, it could be a stale
        // handle from before a recovery swap.
        EnterDbFence();
        try
        {
            return DeleteFamilyRows(keys, columnFamilyKeys);
        }
        finally
        {
            ExitDbFence();
        }
    }

    /// <summary>Physically removes each lock resource's rows (same layout as the kv family).</summary>
    public bool DeleteLocks(IReadOnlyList<string> resources)
    {
        EnterDbFence();
        try
        {
            return DeleteFamilyRows(resources, columnFamilyLocks);
        }
        finally
        {
            ExitDbFence();
        }
    }

    private bool DeleteFamilyRows(IReadOnlyList<string> keys, ColumnFamilyHandle columnFamily)
    {
        using WriteBatch batch = new();
        using Iterator iterator = db.NewIterator(cf: columnFamily);

        foreach (string key in keys)
        {
            byte[] prefix = Encoding.UTF8.GetBytes(key + "~");

            iterator.Seek(prefix);

            while (iterator.Valid())
            {
                ReadOnlySpan<byte> rawKey = iterator.GetKeySpan();

                if (!rawKey.StartsWith(prefix))
                    break;

                // Copy the key out of native memory: it must outlive the iterator for batch.Delete.
                batch.Delete(rawKey.ToArray(), cf: columnFamily);
                iterator.Next();
            }
        }

        db.Write(batch, DefaultWriteOptions);
        return true;
    }

    /// <summary>Shell reused by <see cref="UnserializeKeyValueMessageThreadCached"/> on this thread.</summary>
    [ThreadStatic]
    private static RocksDbKeyValueMessage? threadCachedKeyValueMessage;

    /// <summary>Shell reused by <see cref="UnserializeLockMessageThreadCached"/> on this thread.</summary>
    [ThreadStatic]
    private static RocksDbLockMessage? threadCachedLockMessage;

    /// <summary>
    /// Parses a key-value row into a thread-cached shell, for single-row point reads. The result
    /// is only valid until the next parse on the same thread: the caller must copy every field it
    /// needs out before then and must not retain the instance. Extracted byte payloads are safe to
    /// keep — each parse creates a fresh ByteString; only the shell is reused.
    /// </summary>
    private static RocksDbKeyValueMessage UnserializeKeyValueMessageThreadCached(ReadOnlySpan<byte> serializedData) =>
        UnserializeKeyValueMessage(serializedData, threadCachedKeyValueMessage ??= new());

    /// <summary>Lock-row equivalent of <see cref="UnserializeKeyValueMessageThreadCached"/>; same contract.</summary>
    private static RocksDbLockMessage UnserializeLockMessageThreadCached(ReadOnlySpan<byte> serializedData)
    {
        RocksDbLockMessage shell = threadCachedLockMessage ??= new();

        ResetLockMessage(shell);
        shell.MergeFrom(serializedData);

        return shell;
    }

    private static RocksDbKeyValueMessage UnserializeKeyValueMessage(ReadOnlySpan<byte> serializedData) =>
        RocksDbKeyValueMessage.Parser.ParseFrom(serializedData);

    /// <summary>
    /// Parses a key-value row into a caller-owned reusable message instead of a fresh allocation,
    /// for per-row scan loops. The result is only valid until the next parse into the same shell:
    /// the caller must copy every field it needs out before then and must not retain the instance.
    /// Byte payloads extracted from it are safe to keep — each parse creates a fresh ByteString;
    /// only the shell is reused.
    /// </summary>
    private static RocksDbKeyValueMessage UnserializeKeyValueMessage(ReadOnlySpan<byte> serializedData, RocksDbKeyValueMessage shell)
    {
        ResetKeyValueMessage(shell);
        shell.MergeFrom(serializedData);
        return shell;
    }

    /// <summary>
    /// Restores every field of a reused <see cref="RocksDbKeyValueMessage"/> to its default before
    /// a merge or a refill. The reset must be complete: proto3 omits default-valued fields from the
    /// wire and <c>MergeFrom</c> leaves omitted fields untouched, so any field missed here would
    /// leak the previous row's value into the next one. A new proto field must be added here too —
    /// the reset-completeness unit test fails when one is missed.
    /// </summary>
    internal static void ResetKeyValueMessage(RocksDbKeyValueMessage message)
    {
        message.Key = string.Empty;
        message.ClearValue();
        message.ExpiresNode = 0;
        message.ExpiresPhysical = 0;
        message.ExpiresCounter = 0;
        message.LastUsedNode = 0;
        message.LastUsedPhysical = 0;
        message.LastUsedCounter = 0;
        message.LastModifiedNode = 0;
        message.LastModifiedPhysical = 0;
        message.LastModifiedCounter = 0;
        message.Revision = 0;
        message.State = 0;
    }

    /// <summary>
    /// Restores every field of a reused <see cref="RocksDbLockMessage"/> to its default. The same
    /// completeness rule as <see cref="ResetKeyValueMessage"/> applies.
    /// </summary>
    internal static void ResetLockMessage(RocksDbLockMessage message)
    {
        message.Resource = string.Empty;
        message.ClearOwner();
        message.ExpiresNode = 0;
        message.ExpiresPhysical = 0;
        message.ExpiresCounter = 0;
        message.LastUsedNode = 0;
        message.LastUsedPhysical = 0;
        message.LastUsedCounter = 0;
        message.LastModifiedNode = 0;
        message.LastModifiedPhysical = 0;
        message.LastModifiedCounter = 0;
        message.FencingToken = 0;
        message.State = 0;
    }

    /// <summary>
    /// The ordering fields of a stored key-value row: the revision plus the commit HLC.
    /// Rows are compared on these fields alone in the head-advance and prune paths.
    /// </summary>
    internal readonly struct StoredKeyValueOrdering
    {
        public long Revision { get; init; }
        public int LastModifiedNode { get; init; }
        public long LastModifiedPhysical { get; init; }
        public uint LastModifiedCounter { get; init; }
    }

    /// <summary>
    /// Result of a native ordering lookup. <see cref="Found"/> distinguishes a missing row from a
    /// present row whose fields are all defaults: the span-deserializer Get returns
    /// <c>default</c> for a missing key without invoking the deserializer, so Found is false
    /// exactly when no row exists.
    /// </summary>
    private readonly struct StoredKeyValueOrderingLookup
    {
        public bool Found { get; init; }
        public StoredKeyValueOrdering Ordering { get; init; }
    }

    /// <summary>
    /// Decodes the ordering fields of a row directly from the RocksDB-owned native value memory.
    /// The payload bytes are skipped in place and never copied into managed memory — unlike a
    /// MultiGet, whose managed wrapper marshals every value into a fresh array before the caller
    /// can look at it.
    /// </summary>
    private sealed class StoredKeyValueOrderingDeserializer : ISpanDeserializer<StoredKeyValueOrderingLookup>
    {
        public static readonly StoredKeyValueOrderingDeserializer Instance = new();

        public StoredKeyValueOrderingLookup Deserialize(ReadOnlySpan<byte> buffer) =>
            new() { Found = true, Ordering = DecodeKeyValueOrdering(buffer) };
    }

    /// <summary>
    /// Decodes only the ordering fields of a serialized key-value row. The full protobuf parse
    /// copies the value payload into a fresh array plus a ByteString plus a message object; the
    /// compare paths throw all three away. This walk skips the payload bytes without a copy.
    /// Falls back to the full parser on any wire shape it does not handle, so malformed data
    /// surfaces the same exception as before.
    /// </summary>
    internal static StoredKeyValueOrdering DecodeKeyValueOrdering(ReadOnlySpan<byte> serializedData)
    {
        if (TryDecodeKeyValueOrdering(serializedData, out StoredKeyValueOrdering ordering))
            return ordering;

        RocksDbKeyValueMessage message = UnserializeKeyValueMessage(serializedData);
        return new()
        {
            Revision = message.Revision,
            LastModifiedNode = message.LastModifiedNode,
            LastModifiedPhysical = message.LastModifiedPhysical,
            LastModifiedCounter = message.LastModifiedCounter,
        };
    }

    /// <summary>
    /// Allocation-free wire-format walk over a serialized <see cref="RocksDbKeyValueMessage"/>.
    /// Extracts fields 9–12 (commit HLC and revision) and skips every other field, including the
    /// value payload. Returns false on any malformed or unsupported wire shape; the caller then
    /// falls back to the full parser. A later field occurrence overrides an earlier one, which
    /// matches protobuf merge semantics.
    /// </summary>
    internal static bool TryDecodeKeyValueOrdering(ReadOnlySpan<byte> data, out StoredKeyValueOrdering ordering)
    {
        long revision = 0;
        int lastModifiedNode = 0;
        long lastModifiedPhysical = 0;
        uint lastModifiedCounter = 0;

        int pos = 0;

        while (pos < data.Length)
        {
            if (!TryReadVarint(data, ref pos, out ulong tag) || tag >> 3 == 0)
            {
                ordering = default;
                return false;
            }

            int fieldNumber = (int)(tag >> 3);

            switch ((int)(tag & 7))
            {
                case 0: // varint
                    if (!TryReadVarint(data, ref pos, out ulong value))
                    {
                        ordering = default;
                        return false;
                    }

                    switch (fieldNumber)
                    {
                        case 9:
                            lastModifiedNode = (int)(long)value;
                            break;
                        case 10:
                            lastModifiedPhysical = (long)value;
                            break;
                        case 11:
                            lastModifiedCounter = (uint)value;
                            break;
                        case 12:
                            revision = (long)value;
                            break;
                    }
                    break;

                case 1: // fixed64
                    if (data.Length - pos < 8)
                    {
                        ordering = default;
                        return false;
                    }
                    pos += 8;
                    break;

                case 2: // length-delimited: skip the bytes without materializing them
                    if (!TryReadVarint(data, ref pos, out ulong length) || (ulong)(data.Length - pos) < length)
                    {
                        ordering = default;
                        return false;
                    }
                    pos += (int)length;
                    break;

                case 5: // fixed32
                    if (data.Length - pos < 4)
                    {
                        ordering = default;
                        return false;
                    }
                    pos += 4;
                    break;

                default: // groups and reserved wire types: let the full parser decide
                    ordering = default;
                    return false;
            }
        }

        ordering = new()
        {
            Revision = revision,
            LastModifiedNode = lastModifiedNode,
            LastModifiedPhysical = lastModifiedPhysical,
            LastModifiedCounter = lastModifiedCounter,
        };
        return true;
    }

    /// <summary>
    /// Reads one base-128 varint. Accepts at most 10 bytes — the protobuf maximum for a 64-bit
    /// value — and, like the protobuf reader, discards bits shifted beyond 64.
    /// </summary>
    private static bool TryReadVarint(ReadOnlySpan<byte> data, ref int pos, out ulong value)
    {
        value = 0;

        for (int shift = 0; shift < 70; shift += 7)
        {
            if (pos >= data.Length)
                return false;

            byte b = data[pos++];

            if (shift < 64)
                value |= (ulong)(b & 0x7F) << shift;

            if ((b & 0x80) == 0)
                return true;
        }

        // Continuation bit still set after 10 bytes: malformed.
        return false;
    }

    public bool PruneKeyValueRevisions(
        IReadOnlyCollection<string>? keys,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        HLCTimestamp floorTimestamp,
        out RevisionPruneResult result)
    {
        EnterDbFence();
        try
        {
            return PruneKeyValueRevisionsFenced(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        }
        finally
        {
            ExitDbFence();
        }
    }

    // Must run under the swap fence (EnterDbFence).
    private bool PruneKeyValueRevisionsFenced(
        IReadOnlyCollection<string>? keys,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        HLCTimestamp floorTimestamp,
        out RevisionPruneResult result)
    {
        int keysVisited = 0;
        int deleted = 0;
        int floorViolations = 0;
        bool batchLimitReached = false;
        List<string>? remaining = null;

        if (keys is not null)
        {
            IList<string> keyList = keys as IList<string> ?? keys.ToList();

            for (int i = 0; i < keyList.Count; i++)
            {
                if (deleted >= batchSize)
                {
                    // Batch full before reaching this key — everything from here on still needs work.
                    batchLimitReached = true;
                    for (int j = i; j < keyList.Count; j++)
                        (remaining ??= []).Add(keyList[j]);
                    break;
                }

                string key = keyList[i];
                PruneRevisionsForKey(key, retentionCount, retentionAge, batchSize, floorTimestamp, ref deleted, ref floorViolations, out bool keyLimitReached);
                keysVisited++;

                if (keyLimitReached)
                {
                    // This key was only partially pruned, and the batch is now full.
                    batchLimitReached = true;
                    (remaining ??= []).Add(key);
                    for (int j = i + 1; j < keyList.Count; j++)
                        (remaining ??= []).Add(keyList[j]);
                    break;
                }
            }
        }
        else
        {
            // Backend-wide sweep: visit each logical key via its ~CURRENT entry, resuming from the
            // cursor left by the previous pass so each pass scans only a bounded slice (at most
            // batchSize keys or batchSize deletes) instead of the whole column family.
            int keyBudget = batchSize;
            bool paused = false;

            using Iterator iterator = db.NewIterator(readOptions: MaintenanceScanReadOptions, cf: columnFamilyKeys);

            if (sweepCursor is null)
                iterator.SeekToFirst();
            else
                iterator.Seek(sweepCursor);

            while (iterator.Valid())
            {
                if (deleted >= batchSize || keysVisited >= keyBudget)
                {
                    // Pause here; resume from the current (unprocessed) entry next pass.
                    sweepCursor = iterator.GetKeySpan().ToArray();
                    batchLimitReached = true;
                    paused = true;
                    break;
                }

                // Only ~CURRENT rows map to a logical key to prune. Test the suffix on the native
                // span and decode just the logical key for those — revision rows skip the decode.
                ReadOnlySpan<byte> rawKeySpan = iterator.GetKeySpan();

                if (rawKeySpan.EndsWith(CurrentMarkerUtf8))
                {
                    string logicalKey = Encoding.UTF8.GetString(rawKeySpan[..^CurrentMarkerUtf8.Length]);
                    PruneRevisionsForKey(logicalKey, retentionCount, retentionAge, batchSize, floorTimestamp, ref deleted, ref floorViolations, out bool keyLimitReached);
                    keysVisited++;

                    if (keyLimitReached)
                    {
                        // Key only partially pruned — resume at this same key next pass.
                        sweepCursor = rawKeySpan.ToArray(); // ToArray only on pause path
                        batchLimitReached = true;
                        paused = true;
                        break;
                    }
                }

                iterator.Next();
            }

            // Reached the end of the column family without pausing: full scan complete, wrap around.
            if (!paused)
                sweepCursor = null;
        }

        result = new(keysVisited, deleted, batchLimitReached, remaining, floorViolations);
        return true;
    }

    /// <summary>
    /// Prunes old revision records for a single logical key, keeping the current revision
    /// and any revisions within the configured count/age retention window.
    /// When <paramref name="floorTimestamp"/> is non-zero the floor-boundary revision
    /// (the highest revision whose LastModified ≤ floorTimestamp) and everything newer
    /// are also protected from deletion.
    /// </summary>
    private void PruneRevisionsForKey(
        string key,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        HLCTimestamp floorTimestamp,
        ref int deleted,
        ref int floorViolations,
        out bool batchLimitReached)
    {
        batchLimitReached = false;

        int keyLen = Encoding.UTF8.GetByteCount(key);
        // ~CURRENT is the longer suffix; one buffer serves both the marker lookup and the ~ prefix.
        int maxLen = keyLen + CurrentMarkerUtf8.Length;

        byte[]? rentedKey = null;
        Span<byte> keyBuffer = maxLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedKey = ArrayPool<byte>.Shared.Rent(maxLen));
        try
        {
            // Encode the key once; both RocksDB lookups reuse these bytes.
            Encoding.UTF8.GetBytes(key.AsSpan(), keyBuffer);

            // Determine the current revision so we never delete its historical record.
            CurrentMarkerUtf8.CopyTo(keyBuffer[keyLen..]);
            byte[]? currentData = db.Get(keyBuffer[..(keyLen + CurrentMarkerUtf8.Length)], cf: columnFamilyKeys);
            if (currentData is null)
                return;

            StoredKeyValueOrdering currentOrdering = DecodeKeyValueOrdering(currentData);
            long currentRevision = currentOrdering.Revision;

            // Collect all revision entries for this key by seeking to "<key>~".
            // Revision entries have numeric suffixes; ~CURRENT is skipped.
            // Reuse key bytes already in keyBuffer[0..keyLen); overwrite suffix to just "~".
            keyBuffer[keyLen] = (byte)'~';
            ReadOnlySpan<byte> prefixBytes = keyBuffer[..(keyLen + 1)];

            bool needAge = retentionAge > TimeSpan.Zero;
            bool needFloor = floorTimestamp != HLCTimestamp.Zero;
            long cutoffPhysical = needAge
                ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - (long)retentionAge.TotalMilliseconds
                : long.MinValue;

            List<(long Revision, long LastModifiedPhysical, HLCTimestamp LastModified, byte[] RawKeyBytes)> revisions = [];

            using (Iterator iterator = db.NewIterator(readOptions: MaintenanceScanReadOptions, cf: columnFamilyKeys))
            {
                iterator.Seek(prefixBytes);

                while (iterator.Valid())
                {
                    // Filter on the native key span — no per-row string decode. Only rows we keep are
                    // materialised (their key bytes are retained below for the later batch.Delete).
                    ReadOnlySpan<byte> entryKey = iterator.GetKeySpan();

                    if (!entryKey.StartsWith(prefixBytes))
                        break;

                    // Skip the ~CURRENT sentinel — never a candidate for deletion.
                    if (entryKey.EndsWith(CurrentMarkerUtf8))
                    {
                        iterator.Next();
                        continue;
                    }

                    // The suffix after "<key>~" must be a numeric revision; reject any trailing junk
                    // so "<key>~123abc" is ignored exactly as long.TryParse would.
                    ReadOnlySpan<byte> suffix = entryKey[prefixBytes.Length..];
                    if (!Utf8Parser.TryParse(suffix, out long revisionNum, out int consumed) || consumed != suffix.Length)
                    {
                        iterator.Next();
                        continue;
                    }

                    // Decode LastModified for every revision (not just when age/floor need it): the
                    // pruned-history floor is derived from the oldest surviving revision's timestamp,
                    // which must be known even for count-only retention.
                    StoredKeyValueOrdering rowOrdering = DecodeKeyValueOrdering(iterator.GetValueSpan());
                    long lastModifiedPhysical = rowOrdering.LastModifiedPhysical;
                    HLCTimestamp lastModified = new(rowOrdering.LastModifiedNode, rowOrdering.LastModifiedPhysical, rowOrdering.LastModifiedCounter);

                    // Copy the key out of native memory: it must outlive the iterator for batch.Delete.
                    revisions.Add((revisionNum, lastModifiedPhysical, lastModified, entryKey.ToArray()));
                    iterator.Next();
                }
            }

            if (revisions.Count == 0)
                return;

            // Sort descending so index 0 is the newest revision.
            revisions.Sort(static (a, b) => b.Revision.CompareTo(a.Revision));

            // Compute the floor revision: the highest revision whose LastModified ≤ floorTimestamp.
            // Revisions >= floorRevision are protected from deletion.
            long floorRevision = -1;
            if (needFloor)
            {
                foreach ((long rev, _, HLCTimestamp lm, _) in revisions)
                {
                    if (lm <= floorTimestamp && rev > floorRevision)
                        floorRevision = rev;
                }
            }

            using WriteBatch batch = new();
            int deletedInBatch = 0;
            HashSet<long>? deletedRevNums = null;

            for (int i = 0; i < revisions.Count; i++)
            {
                (long revNum, long lastModifiedPhysical, _, byte[] rawKeyBytes) = revisions[i];

                // Always protect the current revision's historical record.
                if (revNum == currentRevision)
                    continue;

                // Floor protection: when a floor is active, protect the boundary revision and
                // everything newer.  When no revision exists at-or-before the floor (floorRevision
                // < 0), the key was created entirely after the floor — all its revisions are
                // protected by skipping this key entirely.
                if (needFloor && (floorRevision < 0 || revNum >= floorRevision))
                    continue;

                bool deleteByCount = retentionCount > 0 && i >= retentionCount;
                bool deleteByAge = needAge && lastModifiedPhysical < cutoffPhysical;

                if (!deleteByCount && !deleteByAge)
                    continue;

                if (deleted + deletedInBatch >= batchSize)
                {
                    batchLimitReached = true;
                    break;
                }

                // Independent floor-protection audit: the floor clamp above (the `continue` guarding
                // revNum >= floorRevision / floorRevision < 0) must prevent any floor-protected
                // revision from reaching this delete. Re-deriving the protected condition here, at
                // the delete site, catches a regression in that clamp — a correct clamp keeps this
                // at 0; observing one means a protected revision is being deleted.
                if (needFloor && (floorRevision < 0 || revNum >= floorRevision))
                    floorViolations++;

                batch.Delete(rawKeyBytes, cf: columnFamilyKeys);
                deletedInBatch++;
                (deletedRevNums ??= []).Add(revNum);
            }

            // This key lost history: raise the durable pruned-history floor to its oldest surviving
            // revision (or the current no-revision row's HLC when none survives), staging the floor
            // Put into the SAME batch as the deletes so a crash cannot leave the floor trailing the
            // deleted history. As-of reads at a cut below it can no longer find the key's boundary.
            HLCTimestamp stagedFloor = HLCTimestamp.Zero;
            if (deletedRevNums is not null)
            {
                HLCTimestamp oldestSurviving = HLCTimestamp.Zero;
                foreach ((long revNum, _, HLCTimestamp lastModified, _) in revisions)
                {
                    if (deletedRevNums.Contains(revNum))
                        continue;
                    if (oldestSurviving == HLCTimestamp.Zero || lastModified.CompareTo(oldestSurviving) < 0)
                        oldestSurviving = lastModified;
                }

                // No revision row survives — the current value is a SetNoRevision write with no history
                // row of its own, so a cut below its timestamp cannot reconstruct the deleted boundary.
                if (oldestSurviving == HLCTimestamp.Zero)
                    oldestSurviving = new HLCTimestamp(
                        currentOrdering.LastModifiedNode, currentOrdering.LastModifiedPhysical, currentOrdering.LastModifiedCounter);

                stagedFloor = StagePrunedFloor(batch, oldestSurviving);
            }

            if (deletedInBatch > 0 || stagedFloor != HLCTimestamp.Zero)
                db.Write(batch, DefaultWriteOptions);

            // Commit the floor to the in-memory cache only after it is durably written with its deletes.
            if (stagedFloor != HLCTimestamp.Zero)
                CommitPrunedFloor(stagedFloor);

            deleted += deletedInBatch;
        }
        finally
        {
            if (rentedKey is not null) ArrayPool<byte>.Shared.Return(rentedKey);
        }
    }

    public bool SupportsExactAsOfCheckpoint => true;

    public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime)
        => CreateCheckpointCore(destinationPath, appliedIndex, appliedTime, cut: null);

    /// <summary>
    /// Exact as-of-<paramref name="cut"/> checkpoint: takes a native checkpoint, then trims the copy
    /// so each key is left at its newest revision with <c>LastModified ≤ cut</c> (or removed if its
    /// whole history is newer). The live database is never modified — only the checkpoint copy.
    /// </summary>
    public CheckpointResult CreateCheckpointAsOf(
        string destinationPath, long appliedIndex, HLCTimestamp cut, CancellationToken ct = default)
        => CreateCheckpointCore(destinationPath, appliedIndex, cut, cut: cut, ct: ct);

    private CheckpointResult CreateCheckpointCore(
        string destinationPath, long appliedIndex, HLCTimestamp appliedTime, HLCTimestamp? cut, CancellationToken ct = default)
    {
        // The whole checkpoint runs under the swap fence: a recovery swap must never dispose the
        // native handle while a checkpoint save is walking it. A long as-of trim delays a pending
        // recovery by its own duration; the writer keeps retaining batches meanwhile.
        EnterDbFence();
        try
        {
            return CreateCheckpointFenced(destinationPath, appliedIndex, appliedTime, cut, ct);
        }
        finally
        {
            ExitDbFence();
        }
    }

    private CheckpointResult CreateCheckpointFenced(
        string destinationPath, long appliedIndex, HLCTimestamp appliedTime, HLCTimestamp? cut, CancellationToken ct)
    {
        // rocksdb_checkpoint_create requires the leaf to NOT exist — it creates the directory
        // itself. Use a temp sibling so a failure before the rename can never leave a partial
        // checkpoint at destinationPath.
        string? parent = Path.GetDirectoryName(destinationPath);
        if (parent is not null)
            Directory.CreateDirectory(parent);

        string tmpPath = destinationPath + ".tmp_" + Guid.NewGuid().ToString("N")[..8];

        try
        {
            using (Checkpoint cp = db.Checkpoint())
                cp.Save(tmpPath, logSizeForFlush: 0); // RocksDB creates tmpPath

            if (cut.HasValue)
                TrimCheckpointAsOf(tmpPath, cut.Value, ct);

            CheckpointManifest manifest = CheckpointManifest.From(appliedIndex, appliedTime);
            manifest.WriteTo(tmpPath);

            Directory.Move(tmpPath, destinationPath);

            return new(destinationPath, manifest);
        }
        catch
        {
            if (Directory.Exists(tmpPath))
                Directory.Delete(tmpPath, recursive: true);
            throw;
        }
    }

    private sealed class KeyTrimState
    {
        public long BestRevision = -1;
        public byte[]? BestValue;
        public HLCTimestamp BestLastModified;
        public bool HasAnyRevision;
        public byte[]? CurrentKey;
        public HLCTimestamp CurrentLastModified;
        public readonly List<byte[]> FutureRevisionKeys = [];

        // No-revision provenance for this key (earliest/latest SetNoRevision write), and the marker
        // row itself, which is dropped from the as-of image.
        public HLCTimestamp NoRevEarliest;
        public HLCTimestamp NoRevLatest;
        public byte[]? NoRevKey;
    }

    // Flush the write batch to the copy after this many buffered operations, so neither the batch
    // nor managed state grows with the store size.
    private const int TrimBatchFlushThreshold = 4096;

    /// <summary>
    /// Opens the checkpoint copy at <paramref name="checkpointPath"/> and removes all state newer than
    /// <paramref name="cut"/> from the <c>kv</c> column family: future revision rows are deleted, each
    /// key's <c>~CURRENT</c> row is reset to its newest surviving revision (or deleted if none survive),
    /// historyless keys newer than the cut fail closed, and the <c>locks</c> family is emptied. A final
    /// full compaction physically rewrites the SSTs so no post-cut payload remains in the artifact.
    /// <para>Streams one logical key's rows at a time (they are contiguous in sorted order) and flushes
    /// bounded write batches, so peak memory and batch size are independent of the store size.</para>
    /// </summary>
    private static void TrimCheckpointAsOf(string checkpointPath, HLCTimestamp cut, CancellationToken ct)
    {
        DbOptions options = new DbOptions()
            .SetCreateIfMissing(false)
            .SetCreateMissingColumnFamilies(false);

        ColumnFamilies columnFamilies = new()
        {
            { "kv", new ColumnFamilyOptions() },
            { "locks", new ColumnFamilyOptions() }
        };

        using RocksDb copy = RocksDb.Open(options, checkpointPath, columnFamilies);
        ColumnFamilyHandle kv = copy.GetColumnFamily("kv");
        ColumnFamilyHandle locksCf = copy.GetColumnFamily("locks");

        WriteBatch batch = new();
        int batchOps = 0;

        void FlushIfLarge()
        {
            if (batchOps < TrimBatchFlushThreshold)
                return;
            copy.Write(batch);
            batch.Dispose();
            batch = new WriteBatch();
            batchOps = 0;
        }

        try
        {
            string? currentLogical = null;
            KeyTrimState st = new();

            void FlushGroup()
            {
                if (currentLogical is null)
                    return;

                // Drop the internal provenance marker from the as-of image.
                if (st.NoRevKey is not null) { batch.Delete(st.NoRevKey, cf: kv); batchOps++; }

                if (st.CurrentLastModified.CompareTo(cut) <= 0)
                {
                    // The current value is at or before the cut, so it is the exact as-of state
                    // regardless of how it was written — keep it, never reset to an older revision.
                    // (This is the revisioned→no-revision fix.)
                }
                else
                {
                    // The current value is after the cut. Fail closed when a no-revision write in the
                    // boundary window (earliest ≤ cut, latest newer than the surviving revision
                    // boundary) was overwritten and cannot be reconstructed. (no-revision→revisioned fix.)
                    HLCTimestamp boundaryHlc = st.BestRevision >= 0 ? st.BestLastModified : HLCTimestamp.Zero;
                    if (st.NoRevEarliest != HLCTimestamp.Zero
                        && st.NoRevEarliest.CompareTo(cut) <= 0
                        && st.NoRevLatest.CompareTo(boundaryHlc) > 0)
                        throw new ExactCheckpointUnavailableException(
                            $"Key '{currentLogical}' has a SetNoRevision write in its as-of-{cut} boundary " +
                            "window whose value was overwritten and cannot be reconstructed; the cut cannot be produced exactly.");

                    if (st.BestRevision >= 0)
                    {
                        // Roll the current row back to the newest surviving revision (its boundary).
                        batch.Put(st.CurrentKey ?? BuildCurrentKey(currentLogical), st.BestValue!, cf: kv);
                        batchOps++;
                    }
                    else if (st.CurrentKey is not null)
                    {
                        // No revision at/before the cut and no lost no-revision boundary → the key did
                        // not exist at the cut. Omit it.
                        batch.Delete(st.CurrentKey, cf: kv);
                        batchOps++;
                    }
                }

                foreach (byte[] futureRev in st.FutureRevisionKeys) { batch.Delete(futureRev, cf: kv); batchOps++; }
            }

            using (Iterator it = copy.NewIterator(cf: kv))
            {
                it.SeekToFirst();
                while (it.Valid())
                {
                    ct.ThrowIfCancellationRequested();

                    ReadOnlySpan<byte> rawKey = it.GetKeySpan();
                    int lastTilde = rawKey.LastIndexOf((byte)'~');
                    if (lastTilde < 0) { it.Next(); continue; }

                    ReadOnlySpan<byte> suffix = rawKey[(lastTilde + 1)..];
                    string logicalKey = Encoding.UTF8.GetString(rawKey[..lastTilde]);

                    // All rows of one logical key are contiguous in sorted order — a change of logical
                    // key closes the previous group and starts a new one, keeping memory bounded.
                    if (logicalKey != currentLogical)
                    {
                        FlushGroup();
                        FlushIfLarge();
                        currentLogical = logicalKey;
                        st = new KeyTrimState();
                    }

                    // The provenance marker's value is a packed HLC span, not a protobuf message —
                    // handle it before any attempt to deserialize a RocksDbKeyValueMessage.
                    if (suffix.SequenceEqual(NoRevMarkerUtf8[1..]))
                    {
                        if (TryUnpackNoRev(it.GetValueSpan().ToArray(), out HLCTimestamp e, out HLCTimestamp l))
                        {
                            st.NoRevEarliest = e;
                            st.NoRevLatest = l;
                        }
                        st.NoRevKey = rawKey.ToArray();
                        it.Next();
                        continue;
                    }

                    StoredKeyValueOrdering rowOrdering = DecodeKeyValueOrdering(it.GetValueSpan());
                    HLCTimestamp lm = new(rowOrdering.LastModifiedNode, rowOrdering.LastModifiedPhysical, rowOrdering.LastModifiedCounter);

                    if (suffix.SequenceEqual(CurrentMarkerUtf8[1..]))
                    {
                        st.CurrentKey = rawKey.ToArray();
                        st.CurrentLastModified = lm;
                    }
                    else if (Utf8Parser.TryParse(suffix, out long revision, out int consumed) && consumed == suffix.Length)
                    {
                        st.HasAnyRevision = true;
                        if (lm.CompareTo(cut) > 0)
                            st.FutureRevisionKeys.Add(rawKey.ToArray()); // committed after the cut → drop
                        else if (revision > st.BestRevision)
                        {
                            st.BestRevision = revision;
                            st.BestValue = it.GetValueSpan().ToArray();
                            st.BestLastModified = lm;
                        }
                    }

                    it.Next();
                }
            }

            FlushGroup();

            // Exclude locks from the as-of image — volatile lease state re-established at runtime.
            using (Iterator lockIt = copy.NewIterator(cf: locksCf))
            {
                lockIt.SeekToFirst();
                while (lockIt.Valid())
                {
                    ct.ThrowIfCancellationRequested();
                    batch.Delete(lockIt.GetKeySpan().ToArray(), cf: locksCf);
                    batchOps++;
                    FlushIfLarge();
                    lockIt.Next();
                }
            }

            copy.Write(batch);
        }
        finally
        {
            batch.Dispose();
        }

        // Physically purge: a full compaction rewrites the SSTs, dropping the deleted/overwritten
        // post-cut values so no artifact byte discloses state newer than the cut. Null bounds compact
        // the entire column family.
        copy.CompactRange((byte[]?)null, (byte[]?)null, kv);
        copy.CompactRange((byte[]?)null, (byte[]?)null, locksCf);
    }

    private static byte[] BuildCurrentKey(string logicalKey)
    {
        int keyLen = Encoding.UTF8.GetByteCount(logicalKey);
        byte[] buffer = new byte[keyLen + CurrentMarkerUtf8.Length];
        Encoding.UTF8.GetBytes(logicalKey, buffer);
        CurrentMarkerUtf8.CopyTo(buffer.AsSpan(keyLen));
        return buffer;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        // The write side of the fence waits for in-flight operations, so the native handle is
        // never disposed under an active reader. A straggler that arrives later gets the managed
        // IOException from EnterDbFence instead of a native use-after-free.
        swapFence.EnterWriteLock();
        try
        {
            if (disposed)
                return;

            disposed = true;

            if (!storageUnavailable)
            {
                db.Dispose();
                storageUnavailable = true;
            }
        }
        finally
        {
            swapFence.ExitWriteLock();
        }

        // The fence itself is deliberately not disposed: a late caller must reach the managed
        // "store is closed" refusal in EnterDbFence, not a disposed-lock exception, and the
        // backend instance lives for the process lifetime anyway.
    }
}
