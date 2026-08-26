
using Kahuna.Server.Locks;
using Kommander;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Kahuna.Server.Locks.Data;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Represents a persistence backend implementation using SQLite for storing key-value pairs
/// and lock data. This class provides functionality for managing and retrieving stored
/// data, enabling flexible data management in applications.
/// </summary>
/// <remarks>
/// This class implements the <see cref="IPersistenceBackend"/> interface, providing
/// methods for storing and retrieving key-value and lock data, as well as querying
/// data by prefix. It is designed for use cases requiring persistence in SQLite-based
/// storage for high-performance scenarios.
/// The class also implements IDisposable to ensure proper handling and release of resources
/// like database connections and locks.
/// </remarks>
internal sealed class SqlitePersistenceBackend : IPersistenceBackend, IDisposable
{
    /// <summary>
    /// Represents the maximum number of shards used in the SQLite persistence backend.
    /// </summary>
    /// <remarks>
    /// The <c>MaxShards</c> value determines the number of partitioned storage segments
    /// within the SQLite persistence system. This value is used to calculate shard indices
    /// for distributing and managing data effectively. It ensures performance scaling
    /// by allowing resource segregation based on shard calculations.
    /// </remarks>
    private const int MaxShards = 8;
       
    private readonly SemaphoreSlim semaphore = new(1, 1);

    /// <summary>
    /// Manages a collection of SQLite database connections, organized by shard identifiers.
    /// </summary>
    /// <remarks>
    /// The <c>connections</c> dictionary stores tuples containing a <see cref="ReaderWriterLock"/>
    /// and a <see cref="SqliteConnection"/> instance for each shard. This structure allows for
    /// efficient concurrent access and connection management to ensure thread-safe operations
    /// within the persistence backend.
    /// </remarks>
    private readonly Dictionary<int, (ReaderWriterLock, SqliteConnection)> connections = new();
    
    private readonly string path;

    private readonly string dbRevision;

    private readonly ILogger logger;

    // Durable pruned-history floor, stored per shard in the pitr_meta table and written (write-ahead)
    // before the revision deletes that advance it, so a crash cannot leave the floor trailing the
    // deleted history. A corrupt/unreadable floor for a store that may have pruned yields
    // FailClosedFloor so backups refuse every cut until repaired.
    private static readonly HLCTimestamp FailClosedFloor = new(int.MaxValue, long.MaxValue, uint.MaxValue);
    
    private readonly Lock _floorLock = new();
    
    private HLCTimestamp? _prunedFloorCache; // null until first loaded from the shards
    
    private bool _prunedFloorCorrupt;

    /// <summary>
    /// Shard at which the next backend-wide revision sweep should resume.
    /// </summary>
    private int sweepShardCursor;

    /// <summary>
    /// Exclusive lower-bound key within <see cref="sweepShardCursor"/> at which the next sweep pass
    /// should resume, or <c>null</c> to start from the first key in the shard. Together with
    /// <see cref="sweepShardCursor"/> this lets each sweep pass scan only a bounded slice of the
    /// keyspace instead of every revision row on every interval.
    /// </summary>
    private string? sweepKeyCursor;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="path"></param>
    /// <param name="dbRevision"></param>
    public SqlitePersistenceBackend(string path = ".", string dbRevision = "v1", ILogger? logger = null)
    {
        this.path = path;
        this.dbRevision = dbRevision;
        this.logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public HLCTimestamp GetPrunedHistoryFloor()
    {
        lock (_floorLock)
        {
            if (_prunedFloorCorrupt)
                return FailClosedFloor;
            
            if (_prunedFloorCache is null)
                LoadPrunedFloorLocked();
            
            return _prunedFloorCorrupt ? FailClosedFloor : _prunedFloorCache!.Value;
        }
    }

    private const string PrunedFloorKey = "pruned_history_floor";

    // Must hold _floorLock. The durable floor is the max of every existing shard's pitr_meta row.
    private void LoadPrunedFloorLocked()
    {
        HLCTimestamp max = HLCTimestamp.Zero;
        
        try
        {
            for (int shard = 0; shard < MaxShards; shard++)
            {
                if (!File.Exists(Path.Combine(path, $"kahuna{shard}_{dbRevision}.db")))
                    continue;

                (ReaderWriterLock rwLock, SqliteConnection connection) = TryOpenDatabaseByShard(shard);
                rwLock.AcquireReaderLock(TimeSpan.FromSeconds(5));
                try
                {
                    using SqliteCommand cmd = new(
                        "SELECT node, physical, counter FROM pitr_meta WHERE k = @k", connection);
                    cmd.Parameters.AddWithValue("@k", PrunedFloorKey);
                    using SqliteDataReader reader = cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        HLCTimestamp v = new(reader.GetInt32(0), reader.GetInt64(1), (uint)reader.GetInt64(2));
                        if (v.CompareTo(max) > 0)
                            max = v;
                    }
                }
                finally
                {
                    rwLock.ReleaseReaderLock();
                }
            }
            
            _prunedFloorCache = max;
        }
        catch
        {
            _prunedFloorCorrupt = true;
        }
    }

    // Write-ahead upsert of a shard's pitr_meta floor to max(existing, candidate), by HLC order.
    // Durable (auto-commit / WAL) and issued BEFORE the delete that produced the candidate.
    private static void UpsertPrunedFloor(SqliteConnection connection, HLCTimestamp candidate)
    {
        const string sql = """
            INSERT INTO pitr_meta (k, node, physical, counter)
            VALUES (@k, @n, @p, @c)
            ON CONFLICT(k) DO UPDATE SET
              node     = CASE WHEN @p > physical OR (@p = physical AND @c > counter) OR (@p = physical AND @c = counter AND @n > node) THEN @n ELSE node END,
              counter  = CASE WHEN @p > physical OR (@p = physical AND @c > counter) OR (@p = physical AND @c = counter AND @n > node) THEN @c ELSE counter END,
              physical = CASE WHEN @p > physical OR (@p = physical AND @c > counter) OR (@p = physical AND @c = counter AND @n > node) THEN @p ELSE physical END;
            """;
        using SqliteCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@k", PrunedFloorKey);
        cmd.Parameters.AddWithValue("@n", candidate.N);
        cmd.Parameters.AddWithValue("@p", candidate.L);
        cmd.Parameters.AddWithValue("@c", (long)candidate.C);
        cmd.ExecuteNonQuery();
    }

    private void CommitPrunedFloorCache(HLCTimestamp value)
    {
        lock (_floorLock)
        {
            if (_prunedFloorCorrupt) return;
            if (_prunedFloorCache is null || value.CompareTo(_prunedFloorCache.Value) > 0)
                _prunedFloorCache = value;
        }
    }

    /// <summary>
    /// Attempts to open the database connection and acquire a reader-writer lock
    /// for the specified resource.
    /// </summary>
    /// <param name="resource">The resource for which the database should be accessed. This is used to determine the appropriate shard.</param>
    /// <returns>
    /// A tuple containing a <see cref="ReaderWriterLock"/> for controlling concurrent access
    /// and a <see cref="SqliteConnection"/> representing the database connection.
    /// </returns>
    private (ReaderWriterLock readerWriterLock, SqliteConnection connection) TryOpenDatabase(string resource)
    {
        int shard = (int)HashUtils.InversePrefixedHash(resource, '/', MaxShards);
        
        return TryOpenDatabaseByShard(shard);
    }

    /// <summary>
    /// Attempts to open the database connection and acquire a reader-writer lock
    /// for the specified shard.
    /// </summary>
    /// <param name="shard">The shard identifier used to locate the appropriate database.</param>
    /// <returns>
    /// A tuple containing a <see cref="ReaderWriterLock"/> for managing concurrent access
    /// and a <see cref="SqliteConnection"/> representing the database connection for the specified shard.
    /// </returns>
    private (ReaderWriterLock readerWriterLock, SqliteConnection connection) TryOpenDatabaseByShard(int shard)
    {
        if (connections.TryGetValue(shard, out (ReaderWriterLock readerWriterLock, SqliteConnection connection) sqlConnection))
            return sqlConnection;
        
        try
        {
            semaphore.Wait();

            if (connections.TryGetValue(shard, out sqlConnection))
                return sqlConnection;
            
            // Pooling=False ensures that SqliteConnection.Dispose() physically closes the
            // underlying file handle rather than returning it to the ADO.NET connection pool.
            // Without this, pool entries accumulate over the process lifetime — every
            // embedded node disposal leaves an open file descriptor.  Since
            // SqlitePersistenceBackend already manages connection lifetime explicitly
            // (connections are kept open for the lifetime of this instance and shared
            // across all callers via the `connections` dict), ADO.NET pooling adds no
            // benefit and only causes FD leaks.
            string connectionString = $"Data Source={path}/kahuna{shard}_{dbRevision}.db;Pooling=False";
            SqliteConnection connection = new(connectionString);

            connection.Open();

            const string createTableQuery = """
            CREATE TABLE IF NOT EXISTS locks (
                resource STRING PRIMARY KEY, 
                owner BLOB, 
                expiresNode INT,
                expiresPhysical INT,
                expiresCounter INT, 
                fencingToken INT,
                lastUsedNode INT, 
                lastUsedPhysical INT,
                lastUsedCounter INT,
                lastModifiedNode INT,
                lastModifiedPhysical INT,
                lastModifiedCounter INT,
                state INT
            );
            """;
            
            using SqliteCommand command1 = new(createTableQuery, connection);
            command1.ExecuteNonQuery();
            
            const string createTableQuery2 = """
            CREATE TABLE IF NOT EXISTS keys (
                key STRING,
                revision INT, 
                value BLOB, 
                expiresNode INT,
                expiresPhysical INT, 
                expiresCounter INT, 
                lastUsedNode INT, 
                lastUsedPhysical INT,
                lastUsedCounter INT,
                lastModifiedNode INT,
                lastModifiedPhysical INT,
                lastModifiedCounter INT,
                state INT,
                PRIMARY KEY (key)
            );
            """;
            
            using SqliteCommand command2 = new(createTableQuery2, connection);
            command2.ExecuteNonQuery();
            
            const string createTableQuery3 = """
             CREATE TABLE IF NOT EXISTS keys_revisions (
                 key STRING,
                 revision INT,
                 value BLOB,
                 expiresNode INT,
                 expiresPhysical INT,
                 expiresCounter INT,
                 lastUsedNode INT,
                 lastUsedPhysical INT,
                 lastUsedCounter INT,
                 lastModifiedNode INT,
                 lastModifiedPhysical INT,
                 lastModifiedCounter INT,
                 state INT,
                 PRIMARY KEY (key, revision)
             );
             """;

            using SqliteCommand command3 = new(createTableQuery3, connection);
            command3.ExecuteNonQuery();

            const string createRevisionKeyIndexQuery = """
                CREATE INDEX IF NOT EXISTS idx_keys_revisions_key_revision
                ON keys_revisions(key, revision DESC);
                """;

            using (SqliteCommand commandIndex1 = new(createRevisionKeyIndexQuery, connection))
                commandIndex1.ExecuteNonQuery();

            const string createRevisionModifiedIndexQuery = """
                CREATE INDEX IF NOT EXISTS idx_keys_revisions_last_modified
                ON keys_revisions(lastModifiedPhysical);
                """;

            using (SqliteCommand commandIndex2 = new(createRevisionModifiedIndexQuery, connection))
                commandIndex2.ExecuteNonQuery();

            // Per-key provenance for SetNoRevision writes: the earliest and latest HLC the key was
            // written without a retained revision row. A no-revision value that is later overwritten
            // cannot be reconstructed, so the as-of trim uses this to fail closed when a cut's boundary
            // could be such a lost value. A new table (not new columns), so it is created on existing
            // databases too.
            const string createNoRevTableQuery = """
                CREATE TABLE IF NOT EXISTS keys_norev (
                    key STRING,
                    earliestNode INT, earliestPhysical INT, earliestCounter INT,
                    latestNode INT, latestPhysical INT, latestCounter INT,
                    PRIMARY KEY (key)
                );
                """;
            using (SqliteCommand commandNoRev = new(createNoRevTableQuery, connection))
                commandNoRev.ExecuteNonQuery();

            // Durable pruned-history floor for this shard (write-ahead-coupled with revision deletes).
            const string createPitrMetaQuery = """
                CREATE TABLE IF NOT EXISTS pitr_meta (
                    k STRING,
                    node INT, physical INT, counter INT,
                    PRIMARY KEY (k)
                );
                """;
            using (SqliteCommand commandPitrMeta = new(createPitrMetaQuery, connection))
                commandPitrMeta.ExecuteNonQuery();

            // Per-partition application-durability floors (only shard 0 is ever written/read — the
            // floor is partition-scoped, not key-scoped). A new table, so it is created on existing
            // databases too.
            const string createDurabilityFloorQuery = """
                CREATE TABLE IF NOT EXISTS durability_floor (
                    partition INTEGER PRIMARY KEY,
                    floor INTEGER
                );
                """;
            using (SqliteCommand commandDurabilityFloor = new(createDurabilityFloorQuery, connection))
                commandDurabilityFloor.ExecuteNonQuery();

            const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;";
            using SqliteCommand command4 = new(pragmasQuery, connection);
            command4.ExecuteNonQuery();

            ReaderWriterLock readerWriterLock = new();
            
            connections.Add(shard, (readerWriterLock, connection));

            return (readerWriterLock, connection);
        }
        finally
        {
            semaphore.Release();
        }
    }

    /// <summary>
    /// Persists a collection of lock-related request items to the database.
    /// </summary>
    /// <param name="items">The list of <see cref="PersistenceRequestItem"/> objects representing lock data to be stored.</param>
    /// <returns>
    /// A boolean value indicating whether the lock data was successfully stored.
    /// </returns>
    public bool StoreDurabilityFloors(IReadOnlyList<(int PartitionId, long Floor)> floors)
    {
        try
        {
            const string upsert = """
                INSERT INTO durability_floor (partition, floor) VALUES (@partition, @floor)
                ON CONFLICT(partition) DO UPDATE SET floor = MAX(floor, @floor);
                """;

            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(0);

            readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

            try
            {
                using SqliteTransaction transaction = connection.BeginTransaction();

                using SqliteCommand command = new(upsert, connection);
                command.Transaction = transaction;

                SqliteParameter paramPartition = command.Parameters.Add("@partition", SqliteType.Integer);
                SqliteParameter paramFloor = command.Parameters.Add("@floor", SqliteType.Integer);
                command.Prepare();

                foreach ((int partitionId, long floor) in floors)
                {
                    paramPartition.Value = partitionId;
                    paramFloor.Value = floor;
                    command.ExecuteNonQuery();
                }

                transaction.Commit();
            }
            finally
            {
                readerWriterLock.ReleaseWriterLock();
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError("StoreDurabilityFloors: {Type} {Message}", ex.GetType().Name, ex.Message);
            return false;
        }
    }

    public bool RemoveDurabilityFloor(int partitionId)
    {
        try
        {
            // Floors live in shard 0 (see StoreDurabilityFloors); a never-created shard has no row.
            if (!File.Exists(Path.Combine(path, $"kahuna0_{dbRevision}.db")))
                return true;

            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(0);

            readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

            try
            {
                using SqliteCommand command = new("DELETE FROM durability_floor WHERE partition = @partition", connection);
                command.Parameters.AddWithValue("@partition", partitionId);
                command.ExecuteNonQuery();
            }
            finally
            {
                readerWriterLock.ReleaseWriterLock();
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError("RemoveDurabilityFloor: {Type} {Message}", ex.GetType().Name, ex.Message);
            return false;
        }
    }

    public long GetDurabilityFloor(int partitionId)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(0);

            readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

            try
            {
                using SqliteCommand command = new("SELECT floor FROM durability_floor WHERE partition = @partition", connection);
                command.Parameters.AddWithValue("@partition", partitionId);

                object? result = command.ExecuteScalar();
                return result is long floor ? floor : -1;
            }
            finally
            {
                readerWriterLock.ReleaseReaderLock();
            }
        }
        catch (Exception ex)
        {
            logger.LogError("GetDurabilityFloor: {Type} {Message}", ex.GetType().Name, ex.Message);
            return -1;
        }
    }

    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        try
        {
            const string insert = """
              INSERT INTO locks (resource, owner, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter, lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, fencingToken, state) 
              VALUES (@resource, @owner, @expiresNode, @expiresPhysical, @expiresCounter, @lastUsedNode, @lastUsedPhysical, @lastUsedCounter, @lastModifiedNode, @lastModifiedPhysical, @lastModifiedCounter, @fencingToken, @state) 
              ON CONFLICT(resource) DO UPDATE SET 
              owner=@owner,
              expiresNode=@expiresNode,
              expiresPhysical=@expiresPhysical, 
              expiresCounter=@expiresCounter,
              lastUsedNode=@lastUsedNode,
              lastUsedPhysical=@lastUsedPhysical, 
              lastUsedCounter=@lastUsedCounter,
              lastModifiedNode=@lastModifiedNode,
              lastModifiedPhysical=@lastModifiedPhysical, 
              lastModifiedCounter=@lastModifiedCounter,
              fencingToken=@fencingToken, 
              state=@state;
              """;
            
            // Group items by shard so each shard's connection is locked once and its rows are
            // written under a single prepared command + transaction instead of re-parsing the
            // INSERT for every row.
            Dictionary<int, List<PersistenceRequestItem>> plan = new();

            // Source list is not mutated while planning; iterate its backing storage by reference to
            // avoid copying every wide struct into the loop variable.
            foreach (ref readonly PersistenceRequestItem item in CollectionsMarshal.AsSpan(items))
            {
                int shard = (int)HashUtils.InversePrefixedHash(item.Key, '/', MaxShards);

                if (plan.TryGetValue(shard, out List<PersistenceRequestItem>? itemsPerShard))
                    itemsPerShard.Add(item);
                else
                    plan.Add(shard, [item]);
            }

            foreach (KeyValuePair<int, List<PersistenceRequestItem>> kv in plan)
            {
                (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(kv.Key);

                try
                {
                    readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

                    using SqliteTransaction transaction = connection.BeginTransaction();

                    try
                    {
                        using SqliteCommand command = new(insert, connection);
                        command.Transaction = transaction;

                        // Parameters are created once and rebound per row; the statement is parsed
                        // and planned a single time via Prepare().
                        SqliteParameter pResource = command.Parameters.Add("@resource", SqliteType.Text);
                        SqliteParameter pOwner = command.Parameters.Add("@owner", SqliteType.Blob);
                        SqliteParameter pExpiresNode = command.Parameters.Add("@expiresNode", SqliteType.Integer);
                        SqliteParameter pExpiresPhysical = command.Parameters.Add("@expiresPhysical", SqliteType.Integer);
                        SqliteParameter pExpiresCounter = command.Parameters.Add("@expiresCounter", SqliteType.Integer);
                        SqliteParameter pLastUsedNode = command.Parameters.Add("@lastUsedNode", SqliteType.Integer);
                        SqliteParameter pLastUsedPhysical = command.Parameters.Add("@lastUsedPhysical", SqliteType.Integer);
                        SqliteParameter pLastUsedCounter = command.Parameters.Add("@lastUsedCounter", SqliteType.Integer);
                        SqliteParameter pLastModifiedNode = command.Parameters.Add("@lastModifiedNode", SqliteType.Integer);
                        SqliteParameter pLastModifiedPhysical = command.Parameters.Add("@lastModifiedPhysical", SqliteType.Integer);
                        SqliteParameter pLastModifiedCounter = command.Parameters.Add("@lastModifiedCounter", SqliteType.Integer);
                        SqliteParameter pFencingToken = command.Parameters.Add("@fencingToken", SqliteType.Integer);
                        SqliteParameter pState = command.Parameters.Add("@state", SqliteType.Integer);

                        command.Prepare();

                        foreach (ref readonly PersistenceRequestItem item in CollectionsMarshal.AsSpan(kv.Value))
                        {
                            pResource.Value = item.Key;
                            pOwner.Value = item.Value is null ? DBNull.Value : item.Value;
                            pExpiresNode.Value = item.ExpiresNode;
                            pExpiresPhysical.Value = item.ExpiresPhysical;
                            pExpiresCounter.Value = item.ExpiresCounter;
                            pLastUsedNode.Value = item.LastUsedNode;
                            pLastUsedPhysical.Value = item.LastUsedPhysical;
                            pLastUsedCounter.Value = item.LastUsedCounter;
                            pLastModifiedNode.Value = item.LastModifiedNode;
                            pLastModifiedPhysical.Value = item.LastModifiedPhysical;
                            pLastModifiedCounter.Value = item.LastModifiedCounter;
                            pFencingToken.Value = item.Revision;
                            pState.Value = item.State;

                            command.ExecuteNonQuery();
                        }

                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
                finally
                {
                    readerWriterLock.ReleaseWriterLock();
                }
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "StoreLock failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
        }

        return false;
    }

    // Upserts a key's no-revision provenance, keeping the earliest (min) and latest (max) write HLC in
    // physical→counter→node order. SQLite evaluates every CASE against the pre-update row, so the
    // interdependent SET clauses are consistent.
    private const string UpsertNoRevProvenanceSql = """
        INSERT INTO keys_norev (key, earliestNode, earliestPhysical, earliestCounter, latestNode, latestPhysical, latestCounter)
        VALUES (@key, @n, @p, @c, @n, @p, @c)
        ON CONFLICT(key) DO UPDATE SET
          earliestNode     = CASE WHEN @p < earliestPhysical OR (@p = earliestPhysical AND @c < earliestCounter) OR (@p = earliestPhysical AND @c = earliestCounter AND @n < earliestNode) THEN @n ELSE earliestNode END,
          earliestCounter  = CASE WHEN @p < earliestPhysical OR (@p = earliestPhysical AND @c < earliestCounter) OR (@p = earliestPhysical AND @c = earliestCounter AND @n < earliestNode) THEN @c ELSE earliestCounter END,
          earliestPhysical = CASE WHEN @p < earliestPhysical OR (@p = earliestPhysical AND @c < earliestCounter) OR (@p = earliestPhysical AND @c = earliestCounter AND @n < earliestNode) THEN @p ELSE earliestPhysical END,
          latestNode       = CASE WHEN @p > latestPhysical OR (@p = latestPhysical AND @c > latestCounter) OR (@p = latestPhysical AND @c = latestCounter AND @n > latestNode) THEN @n ELSE latestNode END,
          latestCounter    = CASE WHEN @p > latestPhysical OR (@p = latestPhysical AND @c > latestCounter) OR (@p = latestPhysical AND @c = latestCounter AND @n > latestNode) THEN @c ELSE latestCounter END,
          latestPhysical   = CASE WHEN @p > latestPhysical OR (@p = latestPhysical AND @c > latestCounter) OR (@p = latestPhysical AND @c = latestCounter AND @n > latestNode) THEN @p ELSE latestPhysical END;
        """;

    /// <summary>
    /// Stores a collection of key-value pairs in the database, ensuring persistent storage for the specified items.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> objects representing the key-value pairs to be stored.</param>
    /// <returns>
    /// A boolean value indicating whether the operation to store the key-value pairs was successful.
    /// </returns>
    public bool StoreKeyValues(List<PersistenceRequestItem> items)
    {
        try
        {
            // The current row only ever advances by (revision, commit HLC in physical→counter→node
            // order): the same committed mutation is queued independently by the owning actor and the
            // Raft consumer, so a delayed older duplicate can land after a newer head — in the same
            // batch or a later one — and must never regress what a read serves as current. In the
            // DO UPDATE predicate, unqualified column names refer to the existing row.
            const string insertKeys = """
              INSERT INTO keys (key, revision, value, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter, lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, state)
              VALUES (@key, @revision, @value, @expiresNode, @expiresPhysical, @expiresCounter, @lastUsedNode, @lastUsedPhysical, @lastUsedCounter, @lastModifiedNode, @lastModifiedPhysical, @lastModifiedCounter, @state)
              ON CONFLICT(key) DO UPDATE SET
              revision=@revision,
              value=@value,
              expiresNode=@expiresNode,
              expiresPhysical=@expiresPhysical,
              expiresCounter=@expiresCounter,
              lastUsedNode=@lastUsedNode,
              lastUsedPhysical=@lastUsedPhysical,
              lastUsedCounter=@lastUsedCounter,
              lastModifiedNode=@lastModifiedNode,
              lastModifiedPhysical=@lastModifiedPhysical,
              lastModifiedCounter=@lastModifiedCounter,
              state=@state
              WHERE @revision > keys.revision
                 OR (@revision = keys.revision
                     AND (@lastModifiedPhysical > keys.lastModifiedPhysical
                          OR (@lastModifiedPhysical = keys.lastModifiedPhysical
                              AND (@lastModifiedCounter > keys.lastModifiedCounter
                                   OR (@lastModifiedCounter = keys.lastModifiedCounter
                                       AND @lastModifiedNode > keys.lastModifiedNode)))));
              """;

            // A retained-history row is keyed by (key, revision); delete and extend records
            // legitimately reuse a revision number with a newer commit HLC, so the row only ever
            // advances by commit HLC — a delayed older same-revision duplicate must not regress it.
            const string insertKeyRevisions = """
              INSERT INTO keys_revisions (key, revision, value, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter, lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, state)
              VALUES (@key, @revision, @value, @expiresNode, @expiresPhysical, @expiresCounter, @lastUsedNode, @lastUsedPhysical, @lastUsedCounter, @lastModifiedNode, @lastModifiedPhysical, @lastModifiedCounter, @state)
              ON CONFLICT(key, revision) DO UPDATE SET
              value=@value,
              expiresNode=@expiresNode,
              expiresPhysical=@expiresPhysical,
              expiresCounter=@expiresCounter,
              lastUsedNode=@lastUsedNode,
              lastUsedPhysical=@lastUsedPhysical,
              lastUsedCounter=@lastUsedCounter,
              lastModifiedNode=@lastModifiedNode,
              lastModifiedPhysical=@lastModifiedPhysical,
              lastModifiedCounter=@lastModifiedCounter,
              state=@state
              WHERE @lastModifiedPhysical > keys_revisions.lastModifiedPhysical
                 OR (@lastModifiedPhysical = keys_revisions.lastModifiedPhysical
                     AND (@lastModifiedCounter > keys_revisions.lastModifiedCounter
                          OR (@lastModifiedCounter = keys_revisions.lastModifiedCounter
                              AND @lastModifiedNode > keys_revisions.lastModifiedNode)));
              """;

            Dictionary<int, List<PersistenceRequestItem>> plan = new();

            // Source list is not mutated while planning; iterate its backing storage by reference to
            // avoid copying every wide struct into the loop variable.
            foreach (ref readonly PersistenceRequestItem item in CollectionsMarshal.AsSpan(items))
            {
                int shard = (int)HashUtils.InversePrefixedHash(item.Key, '/', MaxShards);
                
                if (plan.TryGetValue(shard, out List<PersistenceRequestItem>? itemsPerShard))
                    itemsPerShard.Add(item);
                else
                    plan.Add(shard, [item]);
            }

            foreach (KeyValuePair<int, List<PersistenceRequestItem>> kv in plan)
            {
                (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(kv.Key);

                try
                {
                    readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));
                    
                    using SqliteTransaction transaction = connection.BeginTransaction();
                                        
                    try
                    {
                        // Both statements are parsed/planned once per shard via Prepare(); each row
                        // only rebinds the reused parameter objects instead of re-parsing the SQL.
                        using SqliteCommand revisionsCommand = new(insertKeyRevisions, connection);
                        revisionsCommand.Transaction = transaction;
                        ShardInsertParameters revisionsParams = ShardInsertParameters.Create(revisionsCommand);
                        revisionsCommand.Prepare();

                        using SqliteCommand keysCommand = new(insertKeys, connection);
                        keysCommand.Transaction = transaction;
                        ShardInsertParameters keysParams = ShardInsertParameters.Create(keysCommand);
                        keysCommand.Prepare();

                        // Records the earliest/latest no-revision write HLC per key, taking min/max in
                        // HLC (physical→counter→node) order against the existing row.
                        using SqliteCommand noRevCommand = new(UpsertNoRevProvenanceSql, connection);
                        noRevCommand.Transaction = transaction;
                        SqliteParameter nrKey = noRevCommand.Parameters.Add("@key", SqliteType.Text);
                        SqliteParameter nrN = noRevCommand.Parameters.Add("@n", SqliteType.Integer);
                        SqliteParameter nrP = noRevCommand.Parameters.Add("@p", SqliteType.Integer);
                        SqliteParameter nrC = noRevCommand.Parameters.Add("@c", SqliteType.Integer);
                        noRevCommand.Prepare();

                        foreach (ref readonly PersistenceRequestItem item in CollectionsMarshal.AsSpan(kv.Value))
                        {
                            if (!item.NoRevision)
                            {
                                revisionsParams.Bind(in item);
                                revisionsCommand.ExecuteNonQuery();
                            }
                            else
                            {
                                nrKey.Value = item.Key;
                                nrN.Value = item.LastModifiedNode;
                                nrP.Value = item.LastModifiedPhysical;
                                nrC.Value = (long)item.LastModifiedCounter;
                                noRevCommand.ExecuteNonQuery();
                            }

                            keysParams.Bind(in item);
                            keysCommand.ExecuteNonQuery();
                        }

                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        throw;
                    }                                       
                }
                finally
                {
                    readerWriterLock.ReleaseWriterLock();
                }    
            }
            
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "StoreKeyValue failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
        }

        return false;
    }

    /// <summary>
    /// Attempts to obtain data for a lock in the specified resource and manage concurrent access.
    /// </summary>
    /// <param name="resource">The resource for which the lock data will be queried. This is used to identify the corresponding database shard.</param>
    /// <returns>
    /// A <see cref="LockEntry"/> instance representing the acquired lock context or null if the lock could not be acquired.
    /// </returns>
    public LockEntry? GetLock(string resource)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(resource);
            
            try
            {
                readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

                const string query = """
                SELECT owner, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter, 
                       lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, fencingToken, state                               
                FROM locks
                WHERE resource = @resource
                """;
                
                using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@resource", resource);

                using SqliteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                    return new()
                    {
                        Owner = reader.IsDBNull(0) ? null : (byte[])reader[0],
                        Expires = new(
                            reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                            reader.IsDBNull(2) ? 0 : reader.GetInt64(2), 
                            reader.IsDBNull(3) ? 0 : (uint)reader.GetInt64(3)
                        ),
                        LastUsed = new(
                            reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                            reader.IsDBNull(5) ? 0 : reader.GetInt64(5), 
                            reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6)
                        ),
                        LastModified = new(
                            reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                            reader.IsDBNull(8) ? 0 : reader.GetInt64(8), 
                            reader.IsDBNull(9) ? 0 : (uint)reader.GetInt64(9)
                        ),
                        FencingToken = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                        State = reader.IsDBNull(11) ? LockState.Locked : (LockState)reader.GetInt32(11)
                    };
            }
            finally
            {
                readerWriterLock.ReleaseReaderLock();
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GetLock failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
        }
        
        return null;
    }

    /// <summary>
    /// Retrieves the key-value context associated with the specified key name.
    /// </summary>
    /// <param name="keyName">The name of the key for which to retrieve the associated KeyValueContext.</param>
    /// <returns>An instance of <see cref="KeyValueEntry"/> if the key exists, or null if no context is found.</returns>
    public KeyValueEntry? GetKeyValue(string keyName)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(keyName);

            try
            {
                readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

                //const string query = "SELECT value, revision, expiresPhysical, expiresCounter, state FROM keys WHERE key = @key AND revision = -1";
                
                const string query = """
                 SELECT value, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter, 
                        lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, revision, state                               
                 FROM keys
                 WHERE key = @key
                 """;
                
                using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@key", keyName);

                using SqliteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                    return new()
                    {
                        Value = reader.IsDBNull(0) ? null : (byte[])reader[0],
                        Expires = new(
                            reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                            reader.IsDBNull(2) ? 0 : reader.GetInt64(2), 
                            reader.IsDBNull(3) ? 0 : (uint)reader.GetInt64(3)
                        ),
                        LastUsed = new(
                            reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                            reader.IsDBNull(5) ? 0 : reader.GetInt64(5), 
                            reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6)
                        ),
                        LastModified = new(
                            reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                            reader.IsDBNull(8) ? 0 : reader.GetInt64(8), 
                            reader.IsDBNull(9) ? 0 : (uint)reader.GetInt64(9)
                        ),
                        Revision = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                        State = reader.IsDBNull(11) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(11)
                    };
            }
            finally
            {
                readerWriterLock.ReleaseReaderLock();
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GetKeyValue failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
        }
        
        return null;
    }

    /// <summary>
    /// Retrieves the version-specific key-value pair context from persistent storage.
    /// </summary>
    /// <param name="keyName">The name of the key to retrieve the context for.</param>
    /// <param name="revision">The specific revision of the key-value pair to retrieve.</param>
    /// <returns>
    /// A <see cref="KeyValueEntry"/> representing the key-value pair context for the specified key and revision,
    /// or null if the key or specific revision does not exist.
    /// </returns>
    public KeyValueEntry? GetKeyValueRevision(string keyName, long revision)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(keyName);

            try
            {
                readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

                const string query = """
                 SELECT value, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter, 
                        lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, revision, state                               
                 FROM keys_revisions
                 WHERE key = @key AND revision = @revision
                 """;
                
                using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@key", keyName);
                command.Parameters.AddWithValue("@revision", revision);

                using SqliteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                    return new()
                    {
                        Value = reader.IsDBNull(0) ? null : (byte[])reader[0],
                        Expires = new(
                            reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                            reader.IsDBNull(2) ? 0 : reader.GetInt64(2), 
                            reader.IsDBNull(3) ? 0 : (uint)reader.GetInt64(3)
                        ),
                        LastUsed = new(
                            reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                            reader.IsDBNull(5) ? 0 : reader.GetInt64(5), 
                            reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6)
                        ),
                        LastModified = new(
                            reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                            reader.IsDBNull(8) ? 0 : reader.GetInt64(8), 
                            reader.IsDBNull(9) ? 0 : (uint)reader.GetInt64(9)
                        ),
                        Revision = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                        State = reader.IsDBNull(11) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(11)
                    };
            }
            finally
            {
                readerWriterLock.ReleaseReaderLock();
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GetKeyValueRevision failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
        }

        return null;
    }

    public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(keyName);

            try
            {
                readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

                // HLC comparison encodes (L, C, N) lexicographic order: physical first, then counter, then node.
                const string query = """
                 SELECT value, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter,
                        lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, revision, state
                 FROM keys_revisions
                 WHERE key = @key
                   AND revision <= @maxRevision
                   AND (
                       lastModifiedPhysical < @tsPhysical
                       OR (lastModifiedPhysical = @tsPhysical AND lastModifiedCounter < @tsCounter)
                       OR (lastModifiedPhysical = @tsPhysical AND lastModifiedCounter = @tsCounter AND lastModifiedNode <= @tsNode)
                   )
                 ORDER BY revision DESC
                 LIMIT 1
                 """;

                using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@key", keyName);
                command.Parameters.AddWithValue("@maxRevision", maxRevision);
                command.Parameters.AddWithValue("@tsPhysical", readTimestamp.L);
                command.Parameters.AddWithValue("@tsCounter", (long)readTimestamp.C);
                command.Parameters.AddWithValue("@tsNode", readTimestamp.N);

                using SqliteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                    return new()
                    {
                        Value = reader.IsDBNull(0) ? null : (byte[])reader[0],
                        Expires = new(
                            reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                            reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                            reader.IsDBNull(3) ? 0 : (uint)reader.GetInt64(3)
                        ),
                        LastUsed = new(
                            reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                            reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                            reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6)
                        ),
                        LastModified = new(
                            reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                            reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                            reader.IsDBNull(9) ? 0 : (uint)reader.GetInt64(9)
                        ),
                        Revision = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                        State = reader.IsDBNull(11) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(11)
                    };
            }
            finally
            {
                readerWriterLock.ReleaseReaderLock();
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GetKeyValueRevisionAtOrBefore failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
        }

        return null;
    }

    /// <summary>
    /// Retrieves a list of key-value pairs whose keys match the specified prefix.
    /// </summary>
    /// <param name="prefixKeyName">The prefix of the keys to filter and retrieve.</param>
    /// <returns>
    /// A list of tuples, where each tuple contains a key as a string and its associated
    /// <see cref="ReadOnlyKeyValueEntry"/> representing the value.
    /// </returns>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueEntry)> results = [];

        // Keys of the form "{prefix}/{x}" are stored on shard ConsistentHash(prefix) because
        // StoreKeyValues uses InversePrefixedHash(fullKey) = ConsistentHash(fullKey[..lastSlash]) = ConsistentHash(prefix).
        // TryOpenDatabase(prefix) would compute InversePrefixedHash(prefix) = ConsistentHash(prefix[..lastSlash]),
        // which is one level too high and maps to the wrong shard.
        int shard = HashUtils.ConsistentHash(prefixKeyName, MaxShards);
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(shard);
        
        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

            const string query = """
             SELECT key, value, revision, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter,
                    lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, state
             FROM keys
             WHERE key LIKE @key
             LIMIT @limit
             """;

            using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@key", prefixKeyName + "%");
            command.Parameters.AddWithValue("@limit", KeyValueScanLimits.MaxPrefixScanResults);

            using SqliteDataReader reader = command.ExecuteReader();

            while (reader.Read())
                results.Add((reader.IsDBNull(0) ? "" : reader.GetString(0), new(
                    value: reader.IsDBNull(1) ? null : (byte[])reader[1],
                    revision: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                    expires: new(
                        reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                        reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                        reader.IsDBNull(5) ? 0 : (uint)reader.GetInt64(5)
                    ),
                    lastUsed: new(
                        reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                        reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                        reader.IsDBNull(8) ? 0 : (uint)reader.GetInt64(8)
                    ),
                    lastModified: new(
                        reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                        reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                        reader.IsDBNull(11) ? 0 : (uint)reader.GetInt64(11)
                    ),
                    state: reader.IsDBNull(12) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(12)
                )));
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }

        return results;
    }

    /// <summary>
    /// Retrieves a bounded, ordered page of key-value pairs whose keys start with <paramref name="prefix"/>,
    /// beginning at <paramref name="startKey"/> (or the prefix start if null), up to <paramref name="limit"/> entries.
    /// </summary>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
    {
        List<(string, ReadOnlyKeyValueEntry)> results = [];

        int shard = HashUtils.ConsistentHash(prefix, MaxShards);
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(shard);

        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

            string seek = startKey ?? prefix;
            string? upper = GetPrefixUpperBound(prefix);

            // Collation note: SQLite's default TEXT collation for >= / < / ORDER BY is BINARY,
            // which compares UTF-8 bytes.  string.CompareOrdinal (used by the C# merge layer)
            // compares UTF-16 code units.  The two orderings are identical for code points U+0000–
            // U+007F (the full ASCII range used by all Camus key formats).  They can diverge for
            // keys containing multi-byte UTF-8 sequences (non-ASCII), where UTF-8 and UTF-16
            // byte orders may differ for characters above U+07FF.  If non-ASCII keys are ever
            // needed, add COLLATE BINARY explicitly and document the encoding assumption, or
            // switch the C# layer to UTF-8 byte comparison to stay in sync.
            string query = upper is not null
                ? """
                  SELECT key, value, revision, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter,
                         lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, state
                  FROM keys
                  WHERE key >= @start AND key < @upper
                  ORDER BY key
                  LIMIT @limit
                  """
                : """
                  SELECT key, value, revision, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter,
                         lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, state
                  FROM keys
                  WHERE key >= @start
                  ORDER BY key
                  LIMIT @limit
                  """;

            using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@start", seek);
            command.Parameters.AddWithValue("@limit", limit);
            if (upper is not null)
                command.Parameters.AddWithValue("@upper", upper);

            using SqliteDataReader reader = command.ExecuteReader();

            while (reader.Read())
                results.Add((reader.IsDBNull(0) ? "" : reader.GetString(0), new(
                    value: reader.IsDBNull(1) ? null : (byte[])reader[1],
                    revision: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                    expires: new(
                        reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                        reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                        reader.IsDBNull(5) ? 0 : (uint)reader.GetInt64(5)
                    ),
                    lastUsed: new(
                        reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                        reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                        reader.IsDBNull(8) ? 0 : (uint)reader.GetInt64(8)
                    ),
                    lastModified: new(
                        reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                        reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                        reader.IsDBNull(11) ? 0 : (uint)reader.GetInt64(11)
                    ),
                    state: reader.IsDBNull(12) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(12)
                )));
        }
        finally
        {
            readerWriterLock.ReleaseReaderLock();
        }

        return results;
    }

    /// <summary>
    /// Whole-family paged scan of the current key-value rows. Rows are sharded across up to
    /// <see cref="MaxShards"/> database files by key-space hash, with no global order across them,
    /// so the scan walks the shards sequentially and the cursor encodes
    /// <c>"&lt;shard&gt;:&lt;lastKey&gt;"</c> (the key part may contain any character; the shard is
    /// everything before the first colon). A page draws from a single shard; when the shard is
    /// exhausted the returned cursor advances to the next one, so a short or empty page with a
    /// non-null cursor is normal.
    /// </summary>
    public KeyValueScanPage ScanKeyValues(string? cursor, int limit) =>
        ScanFamily(cursor, limit,
            """
            SELECT key, value, revision, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter,
                   lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, state
            FROM keys
            WHERE (@after IS NULL OR key > @after)
            ORDER BY key
            LIMIT @limit
            """,
            static reader => (reader.IsDBNull(0) ? "" : reader.GetString(0), new ReadOnlyKeyValueEntry(
                value: reader.IsDBNull(1) ? null : (byte[])reader[1],
                revision: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                expires: new(
                    reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                    reader.IsDBNull(5) ? 0 : (uint)reader.GetInt64(5)
                ),
                lastUsed: new(
                    reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                    reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    reader.IsDBNull(8) ? 0 : (uint)reader.GetInt64(8)
                ),
                lastModified: new(
                    reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                    reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                    reader.IsDBNull(11) ? 0 : (uint)reader.GetInt64(11)
                ),
                state: reader.IsDBNull(12) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(12)
            )),
            static page => new KeyValueScanPage(page.Items, page.NextCursor));

    /// <summary>
    /// Whole-family paged scan of the lock rows. Sharding and cursor semantics are identical to
    /// <see cref="ScanKeyValues"/>.
    /// </summary>
    public LockScanPage ScanLocks(string? cursor, int limit) =>
        ScanFamily(cursor, limit,
            """
            SELECT resource, owner, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter,
                   lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, fencingToken, state
            FROM locks
            WHERE (@after IS NULL OR resource > @after)
            ORDER BY resource
            LIMIT @limit
            """,
            static reader => (reader.IsDBNull(0) ? "" : reader.GetString(0), new LockEntry
            {
                Owner = reader.IsDBNull(1) ? null : (byte[])reader[1],
                Expires = new(
                    reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                    reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                    reader.IsDBNull(4) ? 0 : (uint)reader.GetInt64(4)
                ),
                LastUsed = new(
                    reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                    reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    reader.IsDBNull(7) ? 0 : (uint)reader.GetInt64(7)
                ),
                LastModified = new(
                    reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
                    reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                    reader.IsDBNull(10) ? 0 : (uint)reader.GetInt64(10)
                ),
                FencingToken = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                State = reader.IsDBNull(12) ? LockState.Locked : (LockState)reader.GetInt32(12)
            }),
            static page => new LockScanPage(page.Items, page.NextCursor));

    /// <summary>
    /// Shared shard-walking core of the whole-family scans: parses the cursor, skips shard files
    /// that were never created, queries one shard per page, and advances the cursor to the next
    /// shard once the current one is exhausted.
    /// </summary>
    private TPage ScanFamily<TEntry, TPage>(
        string? cursor,
        int limit,
        string query,
        Func<SqliteDataReader, (string, TEntry)> map,
        Func<(List<(string, TEntry)> Items, string? NextCursor), TPage> wrap)
    {
        int shard = 0;
        string? afterKey = null;

        if (cursor is not null)
        {
            int separator = cursor.IndexOf(':');
            shard = int.Parse(cursor[..separator]);
            afterKey = separator == cursor.Length - 1 ? null : cursor[(separator + 1)..];
        }

        List<(string, TEntry)> items = [];

        for (; shard < MaxShards; shard++, afterKey = null)
        {
            // Skip shards whose database file was never created — opening them here would
            // materialize empty databases as a scan side effect.
            if (!File.Exists(Path.Combine(path, $"kahuna{shard}_{dbRevision}.db")))
                continue;

            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(shard);

            try
            {
                readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

                using SqliteCommand command = new(query, connection);
                command.Parameters.AddWithValue("@after", (object?)afterKey ?? DBNull.Value);
                command.Parameters.AddWithValue("@limit", limit);

                using SqliteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                    items.Add(map(reader));
            }
            finally
            {
                readerWriterLock.ReleaseReaderLock();
            }

            if (items.Count == limit)
                return wrap((items, $"{shard}:{items[^1].Item1}"));

            // Shard exhausted: resume at the start of the next shard (possibly an empty page).
            return wrap((items, shard + 1 < MaxShards ? $"{shard + 1}:" : null));
        }

        return wrap((items, null));
    }

    /// <summary>
    /// Physically removes each key's current row, revision history and no-revision provenance,
    /// grouped by shard with one transaction per shard.
    /// </summary>
    public bool DeleteKeyValues(IReadOnlyList<string> keys) => DeleteFamilyRows(
        keys,
        """
        DELETE FROM keys WHERE key = @key;
        DELETE FROM keys_revisions WHERE key = @key;
        DELETE FROM keys_norev WHERE key = @key;
        """);

    /// <summary>Physically removes each lock resource's row, grouped by shard.</summary>
    public bool DeleteLocks(IReadOnlyList<string> resources) => DeleteFamilyRows(
        resources,
        "DELETE FROM locks WHERE resource = @key;");

    private bool DeleteFamilyRows(IReadOnlyList<string> keys, string deleteQuery)
    {
        if (keys.Count == 0)
            return true;

        Dictionary<int, List<string>> plan = GroupKeysByShard(keys);

        foreach ((int shard, List<string> shardKeys) in plan)
        {
            // A shard whose database file was never created has no rows to remove.
            if (!File.Exists(Path.Combine(path, $"kahuna{shard}_{dbRevision}.db")))
                continue;

            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(shard);

            try
            {
                readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

                using SqliteTransaction transaction = connection.BeginTransaction();

                foreach (string key in shardKeys)
                {
                    using SqliteCommand command = new(deleteQuery, connection, transaction);
                    command.Parameters.AddWithValue("@key", key);
                    command.ExecuteNonQuery();
                }

                transaction.Commit();
            }
            finally
            {
                readerWriterLock.ReleaseWriterLock();
            }
        }

        return true;
    }

    /// <summary>
    /// Returns the smallest string that is strictly greater than every string with the given prefix,
    /// by finding the rightmost character that can be incremented and doing so.
    /// Returns null when the prefix consists entirely of char.MaxValue characters (no upper bound exists).
    /// </summary>
    private static string? GetPrefixUpperBound(string prefix)
    {
        for (int i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] < char.MaxValue)
                return string.Concat(prefix.AsSpan(0, i), ((char)(prefix[i] + 1)).ToString());
        }
        return null;
    }

    public bool PruneKeyValueRevisions(
        IReadOnlyCollection<string>? keys,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        HLCTimestamp floorTimestamp,
        out RevisionPruneResult result)
    {
        result = new(KeysVisited: 0, RevisionsDeleted: 0, BatchLimitReached: false);

        bool countEnabled = retentionCount > 0;
        bool ageEnabled = retentionAge > TimeSpan.Zero;
        if (!countEnabled && !ageEnabled)
            return true;

        if (batchSize <= 0)
            return true;

        long cutoffPhysical = ageEnabled
            ? DateTimeOffset.UtcNow.Subtract(retentionAge).ToUnixTimeMilliseconds()
            : 0;

        int keysVisited = 0;
        int deleted = 0;
        int floorViolations = 0;
        bool batchLimitReached = false;
        List<string>? remaining = null;
        HLCTimestamp passFloor = HLCTimestamp.Zero;

        if (keys is { Count: > 0 })
        {
            Dictionary<int, List<string>> plan = GroupKeysByShard(keys);

            foreach ((int shard, List<string> shardKeys) in plan)
            {
                if (deleted >= batchSize)
                {
                    // Batch full before reaching this shard — none of its keys were visited.
                    batchLimitReached = true;
                    foreach (string key in shardKeys)
                        (remaining ??= []).Add(key);
                    continue;
                }

                (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(shard);

                try
                {
                    readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

                    for (int i = 0; i < shardKeys.Count; i++)
                    {
                        if (deleted >= batchSize)
                        {
                            batchLimitReached = true;
                            for (int j = i; j < shardKeys.Count; j++)
                                (remaining ??= []).Add(shardKeys[j]);
                            break;
                        }

                        string key = shardKeys[i];
                        keysVisited++;

                        int budget = batchSize - deleted;
                        int deletedForKey = PruneKeyRevisions(
                            connection,
                            key,
                            countEnabled,
                            retentionCount,
                            ageEnabled,
                            cutoffPhysical,
                            budget,
                            floorTimestamp,
                            out int violationsForKey,
                            out HLCTimestamp oldestForKey);

                        deleted += deletedForKey;
                        floorViolations += violationsForKey;
                        if (oldestForKey.CompareTo(passFloor) > 0)
                            passFloor = oldestForKey;

                        // Only a delete that consumed the whole per-key budget can have left more
                        // work behind; a short delete removed every matching row for this key.
                        if (deletedForKey == budget
                            && KeyHasMorePrunableRevisions(
                                connection,
                                key,
                                countEnabled,
                                retentionCount,
                                ageEnabled,
                                cutoffPhysical,
                                floorTimestamp))
                        {
                            batchLimitReached = true;
                            (remaining ??= []).Add(key);
                        }
                    }
                }
                finally
                {
                    readerWriterLock.ReleaseWriterLock();
                }
            }
        }
        else
        {
            // Backend-wide sweep: resume from the (shard, key) cursor left by the previous pass so
            // each pass scans at most batchSize keys (or performs batchSize deletes) instead of every
            // revision row on every interval. When the whole keyspace has been scanned the cursor
            // wraps to the start and the pass reports no backlog so the interval gate re-engages.
            int keyBudget = batchSize;
            bool paused = false;

            int shard = sweepShardCursor;

            while (shard < MaxShards)
            {
                if (deleted >= batchSize || keysVisited >= keyBudget)
                {
                    batchLimitReached = true;
                    paused = true;
                    break;
                }

                (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabaseByShard(shard);

                try
                {
                    readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

                    int pageLimit = keyBudget - keysVisited;
                    List<string> page = GetRevisionCandidateKeys(connection, sweepKeyCursor, pageLimit);

                    foreach (string key in page)
                    {
                        if (deleted >= batchSize)
                        {
                            batchLimitReached = true;
                            paused = true;
                            break;
                        }

                        keysVisited++;

                        int budget = batchSize - deleted;
                        int deletedForKey = PruneKeyRevisions(
                            connection,
                            key,
                            countEnabled,
                            retentionCount,
                            ageEnabled,
                            cutoffPhysical,
                            budget,
                            floorTimestamp,
                            out int violationsForKey,
                            out HLCTimestamp oldestForKey);

                        deleted += deletedForKey;
                        floorViolations += violationsForKey;
                        if (oldestForKey.CompareTo(passFloor) > 0)
                            passFloor = oldestForKey;

                        if (deletedForKey == budget)
                        {
                            // Key may still have rows; resume AT this key next pass by leaving the
                            // cursor on the previously completed key.
                            batchLimitReached = true;
                            paused = true;
                            break;
                        }

                        // Key fully processed — advance the cursor past it.
                        sweepKeyCursor = key;
                    }

                    if (paused)
                        break;

                    if (page.Count < pageLimit)
                    {
                        // Shard exhausted — move to the next shard from its first key.
                        shard++;
                        sweepShardCursor = shard;
                        sweepKeyCursor = null;
                    }
                    // else: a full page was returned and the key budget is now spent; the loop top
                    // will pause and resume this same shard after sweepKeyCursor next pass.
                }
                finally
                {
                    readerWriterLock.ReleaseWriterLock();
                }
            }

            // Reached the end of the last shard without pausing: full scan complete, wrap around.
            if (!paused && shard >= MaxShards)
            {
                sweepShardCursor = 0;
                sweepKeyCursor = null;
            }
        }

        // The durable floor was recorded write-ahead per key (in pitr_meta). Refresh the in-memory
        // cache so subsequent GetPrunedHistoryFloor reads reflect this pass without re-reading shards.
        if (passFloor != HLCTimestamp.Zero)
            CommitPrunedFloorCache(passFloor);

        result = new(keysVisited, deleted, batchLimitReached, remaining, floorViolations);
        return true;
    }

    private static Dictionary<int, List<string>> GroupKeysByShard(IReadOnlyCollection<string> keys)
    {
        Dictionary<int, List<string>> plan = new();

        foreach (string key in keys.Distinct(StringComparer.Ordinal))
        {
            int shard = (int)HashUtils.InversePrefixedHash(key, '/', MaxShards);

            if (plan.TryGetValue(shard, out List<string>? shardKeys))
                shardKeys.Add(key);
            else
                plan.Add(shard, [key]);
        }

        return plan;
    }

    /// <summary>
    /// Returns up to <paramref name="limit"/> distinct keys that have historical revision rows,
    /// ordered ascending and strictly greater than <paramref name="afterKey"/> (or from the start
    /// when it is <c>null</c>). The ordered, bounded scan lets the backend-wide sweep page through
    /// the keyspace using the <c>idx_keys_revisions_key_revision</c> index instead of materialising
    /// every candidate key on each pass.
    /// </summary>
    private static List<string> GetRevisionCandidateKeys(SqliteConnection connection, string? afterKey, int limit)
    {
        const string query = """
            SELECT DISTINCT kr.key
            FROM keys_revisions kr
            INNER JOIN keys k ON k.key = kr.key
            WHERE (@after IS NULL OR kr.key > @after)
            ORDER BY kr.key
            LIMIT @limit
            """;

        List<string> keys = [];

        using SqliteCommand command = new(query, connection);
        command.Parameters.AddWithValue("@after", (object?)afterKey ?? DBNull.Value);
        command.Parameters.AddWithValue("@limit", limit);

        using SqliteDataReader reader = command.ExecuteReader();

        while (reader.Read())
            keys.Add(reader.GetString(0));

        return keys;
    }

    // Binds the shared prune-predicate parameters (@key, @currentRevision, and the active
    // count/age/floor bounds) so the will-delete probe and kept-min query match the delete exactly.
    private static void BindPrunePredicate(
        SqliteCommand cmd, string key, long currentRevision,
        bool floorActive, long floorRevision, bool countEnabled, int retentionCount, bool ageEnabled, long cutoffPhysical)
    {
        cmd.Parameters.AddWithValue("@key", key);
        cmd.Parameters.AddWithValue("@currentRevision", currentRevision);
        if (floorActive)
            cmd.Parameters.AddWithValue("@floorRevision", floorRevision);
        if (countEnabled)
            cmd.Parameters.AddWithValue("@retentionCount", retentionCount);
        if (ageEnabled)
            cmd.Parameters.AddWithValue("@cutoffPhysical", cutoffPhysical);
    }

    private static long? GetCurrentRevision(SqliteConnection connection, string key)
    {
        const string query = "SELECT revision FROM keys WHERE key = @key";

        using SqliteCommand command = new(query, connection);
        command.Parameters.AddWithValue("@key", key);

        object? revision = command.ExecuteScalar();
        if (revision is null or DBNull)
            return null;

        return Convert.ToInt64(revision);
    }

    private static int PruneKeyRevisions(
        SqliteConnection connection,
        string key,
        bool countEnabled,
        int retentionCount,
        bool ageEnabled,
        long cutoffPhysical,
        int limit,
        HLCTimestamp floorTimestamp,
        out int floorViolations,
        out HLCTimestamp oldestSurviving)
    {
        floorViolations = 0;
        oldestSurviving = HLCTimestamp.Zero;

        if (limit <= 0)
            return 0;

        long? currentRevision = GetCurrentRevision(connection, key);
        if (currentRevision is null)
            return 0;

        // Determine floor revision: the highest revision whose LastModified ≤ floorTimestamp.
        // All revisions >= floorRevision are protected from deletion.
        long floorRevision = -1;
        if (floorTimestamp != HLCTimestamp.Zero)
        {
            const string floorQuery = """
                SELECT COALESCE(MAX(revision), -1)
                FROM keys_revisions
                WHERE key = @key
                  AND (
                      lastModifiedPhysical < @floorPhysical
                      OR (lastModifiedPhysical = @floorPhysical AND lastModifiedCounter < @floorCounter)
                      OR (lastModifiedPhysical = @floorPhysical AND lastModifiedCounter = @floorCounter AND lastModifiedNode <= @floorNode)
                  )
                """;

            using SqliteCommand floorCmd = new(floorQuery, connection);
            floorCmd.Parameters.AddWithValue("@key", key);
            floorCmd.Parameters.AddWithValue("@floorPhysical", floorTimestamp.L);
            floorCmd.Parameters.AddWithValue("@floorCounter", (long)floorTimestamp.C);
            floorCmd.Parameters.AddWithValue("@floorNode", floorTimestamp.N);

            object? scalar = floorCmd.ExecuteScalar();
            floorRevision = scalar is null or DBNull ? -1 : Convert.ToInt64(scalar);
        }

        List<string> predicates = [];

        if (countEnabled)
        {
            predicates.Add("""
                revision < (
                    SELECT MIN(revision)
                    FROM (
                        SELECT revision
                        FROM keys_revisions
                        WHERE key = @key
                        ORDER BY revision DESC
                        LIMIT @retentionCount
                    )
                )
                """);
        }

        if (ageEnabled)
            predicates.Add("lastModifiedPhysical < @cutoffPhysical");

        string policy = string.Join(" OR ", predicates);

        // Gate on floorTimestamp != Zero (not floorRevision >= 0): when a floor is active but
        // no revision was written at-or-before it (key entirely created after the floor), all
        // revisions must still be protected.  floorRevision = -1 makes "revision < -1" match
        // nothing, so every row for that key is skipped — correct, since every revision is after
        // the floor and must be kept.
        bool floorActive = floorTimestamp != HLCTimestamp.Zero;
        string floorFilter = floorActive ? "AND revision < @floorRevision" : "";

        string deleteQuery = $"""
            DELETE FROM keys_revisions
            WHERE rowid IN (
                SELECT rowid
                FROM keys_revisions
                WHERE key = @key
                  AND revision <> @currentRevision
                  {floorFilter}
                  AND ({policy})
                LIMIT @limit
            )
            """;

        using SqliteCommand command = new(deleteQuery, connection);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@currentRevision", currentRevision.Value);

        if (floorActive)
            command.Parameters.AddWithValue("@floorRevision", floorRevision);

        if (countEnabled)
            command.Parameters.AddWithValue("@retentionCount", retentionCount);

        if (ageEnabled)
            command.Parameters.AddWithValue("@cutoffPhysical", cutoffPhysical);

        command.Parameters.AddWithValue("@limit", limit);

        // Write-ahead the pruned-history floor: before deleting, compute the boundary that would
        // survive full retention (the min HLC of the kept set, or the current row's HLC when the key's
        // only surviving value is a no-revision write) and durably record it. Recording the floor
        // before the delete guarantees the floor can never trail the deleted history on a crash; a
        // truncated (batch-limited) delete only over-records, which the next pass refines.
        HLCTimestamp candidate = HLCTimestamp.Zero;
        {
            string willDeleteSql = $"SELECT EXISTS(SELECT 1 FROM keys_revisions WHERE key = @key AND revision <> @currentRevision {floorFilter} AND ({policy}) LIMIT 1)";
            using SqliteCommand willCmd = new(willDeleteSql, connection);
            BindPrunePredicate(willCmd, key, currentRevision.Value, floorActive, floorRevision, countEnabled, retentionCount, ageEnabled, cutoffPhysical);
            bool willDelete = Convert.ToInt64(willCmd.ExecuteScalar()) != 0;
            if (willDelete)
            {
                string keptMinSql = $"""
                    SELECT lastModifiedNode, lastModifiedPhysical, lastModifiedCounter
                    FROM keys_revisions
                    WHERE key = @key AND NOT (revision <> @currentRevision {floorFilter} AND ({policy}))
                    ORDER BY lastModifiedPhysical ASC, lastModifiedCounter ASC, lastModifiedNode ASC
                    LIMIT 1
                    """;
                using (SqliteCommand keptCmd = new(keptMinSql, connection))
                {
                    BindPrunePredicate(keptCmd, key, currentRevision.Value, floorActive, floorRevision, countEnabled, retentionCount, ageEnabled, cutoffPhysical);
                    using SqliteDataReader kr = keptCmd.ExecuteReader();
                    if (kr.Read())
                        candidate = new HLCTimestamp(kr.GetInt32(0), kr.GetInt64(1), (uint)kr.GetInt64(2));
                }
                if (candidate == HLCTimestamp.Zero)
                {
                    using SqliteCommand curCmd = new(
                        "SELECT lastModifiedNode, lastModifiedPhysical, lastModifiedCounter FROM keys WHERE key = @key", connection);
                    curCmd.Parameters.AddWithValue("@key", key);
                    using SqliteDataReader cr = curCmd.ExecuteReader();
                    if (cr.Read())
                        candidate = new HLCTimestamp(cr.GetInt32(0), cr.GetInt64(1), (uint)cr.GetInt64(2));
                }
                if (candidate != HLCTimestamp.Zero)
                    UpsertPrunedFloor(connection, candidate);
            }
        }

        // Independent floor-protection audit. The DELETE above must never remove a revision at or
        // above the floor boundary (revision >= floorRevision), which the floorFilter enforces.
        // To catch a regression in that filter, count protected rows before and after the delete
        // using only the floor boundary (not the same clamp predicate); a correct delete leaves
        // this delta at 0, and any positive value is a real protected-version deletion.
        long protectedBefore = floorActive ? CountRevisionsAtOrAbove(connection, key, floorRevision) : 0;

        int deletedForKey = command.ExecuteNonQuery();

        if (floorActive)
        {
            long protectedAfter = CountRevisionsAtOrAbove(connection, key, floorRevision);
            long delta = protectedBefore - protectedAfter;
            if (delta > 0)
                floorViolations = (int)delta;
        }

        // The floor was recorded write-ahead above, before the delete. Return the same candidate so
        // the caller can advance the in-memory cache after the pass.
        oldestSurviving = candidate;

        return deletedForKey;
    }

    /// <summary>
    /// Counts revision rows for <paramref name="key"/> whose revision number is at or above
    /// <paramref name="floorRevision"/> — the floor-protected set. Used to audit that a prune did
    /// not delete any floor-protected revision, independent of the prune's own clamp predicate.
    /// </summary>
    private static long CountRevisionsAtOrAbove(SqliteConnection connection, string key, long floorRevision)
    {
        const string countQuery = """
            SELECT COUNT(*) FROM keys_revisions
            WHERE key = @key AND revision >= @floorRevision
            """;

        using SqliteCommand command = new(countQuery, connection);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@floorRevision", floorRevision);

        object? scalar = command.ExecuteScalar();
        return scalar is null or DBNull ? 0 : Convert.ToInt64(scalar);
    }

    private static bool KeyHasMorePrunableRevisions(
        SqliteConnection connection,
        string key,
        bool countEnabled,
        int retentionCount,
        bool ageEnabled,
        long cutoffPhysical,
        HLCTimestamp floorTimestamp)
    {
        long? currentRevision = GetCurrentRevision(connection, key);
        if (currentRevision is null)
            return false;

        // Compute floor revision the same way PruneKeyRevisions does.
        long floorRevision = -1;
        if (floorTimestamp != HLCTimestamp.Zero)
        {
            const string floorQuery = """
                SELECT COALESCE(MAX(revision), -1)
                FROM keys_revisions
                WHERE key = @key
                  AND (
                      lastModifiedPhysical < @floorPhysical
                      OR (lastModifiedPhysical = @floorPhysical AND lastModifiedCounter < @floorCounter)
                      OR (lastModifiedPhysical = @floorPhysical AND lastModifiedCounter = @floorCounter AND lastModifiedNode <= @floorNode)
                  )
                """;

            using SqliteCommand floorCmd = new(floorQuery, connection);
            floorCmd.Parameters.AddWithValue("@key", key);
            floorCmd.Parameters.AddWithValue("@floorPhysical", floorTimestamp.L);
            floorCmd.Parameters.AddWithValue("@floorCounter", (long)floorTimestamp.C);
            floorCmd.Parameters.AddWithValue("@floorNode", floorTimestamp.N);

            object? scalar = floorCmd.ExecuteScalar();
            floorRevision = scalar is null or DBNull ? -1 : Convert.ToInt64(scalar);
        }

        List<string> predicates = [];

        if (countEnabled)
        {
            predicates.Add("""
                revision < (
                    SELECT MIN(revision)
                    FROM (
                        SELECT revision
                        FROM keys_revisions
                        WHERE key = @key
                        ORDER BY revision DESC
                        LIMIT @retentionCount
                    )
                )
                """);
        }

        if (ageEnabled)
            predicates.Add("lastModifiedPhysical < @cutoffPhysical");

        string policy = string.Join(" OR ", predicates);

        bool floorActive = floorTimestamp != HLCTimestamp.Zero;
        string floorFilter = floorActive ? "AND revision < @floorRevision" : "";

        string query = $"""
            SELECT 1
            FROM keys_revisions
            WHERE key = @key
              AND revision <> @currentRevision
              {floorFilter}
              AND ({policy})
            LIMIT 1
            """;

        using SqliteCommand command = new(query, connection);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@currentRevision", currentRevision.Value);

        if (floorActive)
            command.Parameters.AddWithValue("@floorRevision", floorRevision);

        if (countEnabled)
            command.Parameters.AddWithValue("@retentionCount", retentionCount);

        if (ageEnabled)
            command.Parameters.AddWithValue("@cutoffPhysical", cutoffPhysical);

        return command.ExecuteScalar() is not null;
    }

    public bool SupportsExactAsOfCheckpoint => true;

    public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime)
        => CreateCheckpointCore(destinationPath, appliedIndex, appliedTime, cut: null);

    /// <summary>
    /// Exact as-of-<paramref name="cut"/> checkpoint: after copying each shard, trims the copy so it
    /// contains, per key, the newest revision with <c>LastModified ≤ cut</c> and no state newer than
    /// the cut. The live database is never modified — only the checkpoint copy.
    /// </summary>
    public CheckpointResult CreateCheckpointAsOf(
        string destinationPath, long appliedIndex, HLCTimestamp cut, CancellationToken ct = default)
        => CreateCheckpointCore(destinationPath, appliedIndex, cut, cut: cut, ct: ct);

    private CheckpointResult CreateCheckpointCore(
        string destinationPath, long appliedIndex, HLCTimestamp appliedTime, HLCTimestamp? cut, CancellationToken ct = default)
    {
        // Write into a temp sibling so any failure — lock timeout or VACUUM error — cannot
        // leave a partial checkpoint at destinationPath that a catalog scan might treat as valid.
        string tmpPath = destinationPath + ".tmp_" + Guid.NewGuid().ToString("N")[..8];
        Directory.CreateDirectory(tmpPath);

        try
        {
            // VACUUM INTO holds the per-shard exclusive lock for the entire copy duration,
            // stalling writes to that shard. Schedule off the hot write path.
            for (int shard = 0; shard < MaxShards; shard++)
            {
                ct.ThrowIfCancellationRequested();

                // Skip shards whose DB file does not exist yet — opening an absent shard just
                // to VACUUM INTO it would create empty .db files in the checkpoint directory.
                string shardFile = Path.Combine(path, $"kahuna{shard}_{dbRevision}.db");
                if (!File.Exists(shardFile))
                    continue;

                (ReaderWriterLock rwLock, SqliteConnection connection) = TryOpenDatabaseByShard(shard);
                string destFile = Path.Combine(tmpPath, $"kahuna{shard}_{dbRevision}.db");

                bool lockTaken = false;
                try
                {
                    rwLock.AcquireWriterLock(TimeSpan.FromSeconds(5));
                    lockTaken = true;

                    using SqliteCommand cmd = new($"VACUUM INTO '{destFile.Replace("'", "''")}'", connection);
                    cmd.ExecuteNonQuery();
                }
                finally
                {
                    if (lockTaken)
                        rwLock.ReleaseWriterLock();
                }

                // Trim the copy (not the live DB) to the as-of cut. A separate connection to the
                // copied file — no contention with the live shard.
                if (cut.HasValue)
                    TrimShardAsOf(destFile, cut.Value, ct);
            }

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

    /// <summary>
    /// Removes all state newer than <paramref name="cut"/> from a copied shard database, leaving each
    /// key at its newest revision with <c>LastModified ≤ cut</c> (or absent if its whole history is
    /// newer). Uses the same physical→counter→node HLC ordering as
    /// <see cref="GetKeyValueRevisionAtOrBefore"/>.
    /// </summary>
    private static void TrimShardAsOf(string destFile, HLCTimestamp cut, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        // Preflight: VACUUM (below) rewrites the whole DB into a temp file, needing roughly the DB
        // size again in free space. Fail before mutating if the volume clearly can't hold it.
        long dbBytes = new FileInfo(destFile).Length;
        string? destDir = Path.GetDirectoryName(destFile);
        if (destDir is not null)
        {
            try
            {
                long free = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(destDir))!).AvailableFreeSpace;
                if (free < dbBytes + (64L * 1024 * 1024))
                    throw new IOException(
                        $"Insufficient free space to compact the checkpoint copy: need ~{dbBytes} bytes, " +
                        $"have {free}.");
            }
            catch (Exception ex) when (ex is not IOException)
            {
                // DriveInfo can throw on some paths/platforms; a failed preflight is best-effort.
            }
        }

        // Rows strictly after the cut, matching the read path's L→C→N compare.
        const string afterCut =
            "(lastModifiedPhysical > @cutL " +
            "OR (lastModifiedPhysical = @cutL AND lastModifiedCounter > @cutC) " +
            "OR (lastModifiedPhysical = @cutL AND lastModifiedCounter = @cutC AND lastModifiedNode > @cutN))";

        using SqliteConnection conn = new($"Data Source={destFile.Replace("'", "''")};Pooling=False");
        conn.Open();
        using SqliteTransaction tx = conn.BeginTransaction();

        // 1) Drop every revision committed after the cut. Afterwards the max surviving revision per
        //    key is that key's boundary (newest revision at/before the cut).
        RunTrim(conn, tx, $"DELETE FROM keys_revisions WHERE {afterCut};", cut);

        // 1a) Fail closed when a key whose current value is after the cut has a SetNoRevision write in
        //     its as-of boundary window: earliest no-revision write ≤ cut, and latest no-revision
        //     write is newer than the surviving revision boundary (0 if the key has no revision at or
        //     before the cut). Such a boundary value was overwritten and left no history row, so the
        //     cut cannot be reconstructed exactly — unlike a key whose current no-revision value is
        //     itself ≤ the cut (handled by keeping the current row), or one created entirely after the
        //     cut (correctly omitted below).
        using (SqliteCommand check = new(
            """
            SELECT COUNT(*)
            FROM keys k
            JOIN keys_norev nr ON nr.key = k.key
            LEFT JOIN (
                SELECT kr.key AS bkey,
                       kr.lastModifiedPhysical AS bP, kr.lastModifiedCounter AS bC, kr.lastModifiedNode AS bN
                FROM keys_revisions kr
                WHERE kr.revision = (SELECT MAX(revision) FROM keys_revisions k2 WHERE k2.key = kr.key)
            ) b ON b.bkey = k.key
            WHERE (k.lastModifiedPhysical > @cutL
                   OR (k.lastModifiedPhysical = @cutL AND k.lastModifiedCounter > @cutC)
                   OR (k.lastModifiedPhysical = @cutL AND k.lastModifiedCounter = @cutC AND k.lastModifiedNode > @cutN))
              AND (nr.earliestPhysical < @cutL
                   OR (nr.earliestPhysical = @cutL AND nr.earliestCounter < @cutC)
                   OR (nr.earliestPhysical = @cutL AND nr.earliestCounter = @cutC AND nr.earliestNode <= @cutN))
              AND (nr.latestPhysical > COALESCE(b.bP, 0)
                   OR (nr.latestPhysical = COALESCE(b.bP, 0) AND nr.latestCounter > COALESCE(b.bC, 0))
                   OR (nr.latestPhysical = COALESCE(b.bP, 0) AND nr.latestCounter = COALESCE(b.bC, 0) AND nr.latestNode > COALESCE(b.bN, 0)));
            """, conn, tx))
        {
            check.Parameters.AddWithValue("@cutL", cut.L);
            check.Parameters.AddWithValue("@cutC", cut.C);
            check.Parameters.AddWithValue("@cutN", cut.N);
            long unreconstructable = Convert.ToInt64(check.ExecuteScalar());
            if (unreconstructable > 0)
                throw new ExactCheckpointUnavailableException(
                    $"{unreconstructable} key(s) have a SetNoRevision write in their as-of-{cut} boundary " +
                    "window whose value was overwritten and cannot be reconstructed; the cut cannot be produced exactly.");
        }

        // 2) Drop latest-rows that are after the cut AND have no surviving revision (NoRevision keys
        //    newer than the cut, and keys whose entire revision history was after the cut).
        RunTrim(conn, tx,
            $"DELETE FROM keys WHERE {afterCut} AND NOT EXISTS " +
            "(SELECT 1 FROM keys_revisions kr WHERE kr.key = keys.key);", cut);

        // 3) Roll back to the surviving max revision ONLY for keys whose current value is after the
        //    cut. A key whose current value is at/before the cut — including one whose current value
        //    is a SetNoRevision write newer than every revision — is already exact and must be kept as
        //    is, never reset to an older revision.
        RunTrim(conn, tx, $"""
            UPDATE keys
               SET revision = src.revision,
                   value = src.value,
                   expiresNode = src.expiresNode, expiresPhysical = src.expiresPhysical, expiresCounter = src.expiresCounter,
                   lastUsedNode = src.lastUsedNode, lastUsedPhysical = src.lastUsedPhysical, lastUsedCounter = src.lastUsedCounter,
                   lastModifiedNode = src.lastModifiedNode, lastModifiedPhysical = src.lastModifiedPhysical, lastModifiedCounter = src.lastModifiedCounter,
                   state = src.state
              FROM (
                 SELECT kr.key, kr.revision, kr.value, kr.expiresNode, kr.expiresPhysical, kr.expiresCounter,
                        kr.lastUsedNode, kr.lastUsedPhysical, kr.lastUsedCounter,
                        kr.lastModifiedNode, kr.lastModifiedPhysical, kr.lastModifiedCounter, kr.state
                   FROM keys_revisions kr
                  WHERE kr.revision = (SELECT MAX(revision) FROM keys_revisions k2 WHERE k2.key = kr.key)
              ) AS src
             WHERE keys.key = src.key
               AND (keys.lastModifiedPhysical > @cutL
                    OR (keys.lastModifiedPhysical = @cutL AND keys.lastModifiedCounter > @cutC)
                    OR (keys.lastModifiedPhysical = @cutL AND keys.lastModifiedCounter = @cutC AND keys.lastModifiedNode > @cutN));
            """, cut);

        // 4) Exclude locks from the as-of image — volatile lease state re-established at runtime.
        RunTrim(conn, tx, "DELETE FROM locks;", cut: null);

        tx.Commit();

        ct.ThrowIfCancellationRequested();

        // Physically purge: VACUUM rebuilds the file, reclaiming the free pages left by the deletes so
        // no post-cut payload lingers in the artifact's free space. (Must run outside a transaction.)
        using (SqliteCommand vacuum = new("VACUUM;", conn))
            vacuum.ExecuteNonQuery();
    }

    private static void RunTrim(SqliteConnection conn, SqliteTransaction tx, string sql, HLCTimestamp? cut)
    {
        using SqliteCommand cmd = new(sql, conn, tx);
        if (cut.HasValue)
        {
            cmd.Parameters.AddWithValue("@cutL", cut.Value.L);
            cmd.Parameters.AddWithValue("@cutC", cut.Value.C);
            cmd.Parameters.AddWithValue("@cutN", cut.Value.N);
        }
        cmd.ExecuteNonQuery();
    }

    public void Dispose()
    {
        foreach (KeyValuePair<int, (ReaderWriterLock, SqliteConnection)> conn in connections)
        {
            bool lockTaken = false;

            try
            {
                conn.Value.Item1.AcquireWriterLock(TimeSpan.FromSeconds(5));
                lockTaken = true;

                conn.Value.Item2.Dispose();
            }
            finally
            {
                if (lockTaken)
                    conn.Value.Item1.ReleaseWriterLock();
            }
        }

        GC.SuppressFinalize(this);

        semaphore.Dispose();
    }

    /// <summary>
    /// Holds the reusable <see cref="SqliteParameter"/> objects for the key/value insert statements
    /// (<c>keys</c> and <c>keys_revisions</c>, which share an identical column set). Created once per
    /// prepared command; <see cref="Bind"/> rebinds the values for each row so the statement is
    /// parsed and planned a single time instead of per row.
    /// </summary>
    private readonly struct ShardInsertParameters
    {
        private readonly SqliteParameter key;
        private readonly SqliteParameter value;
        private readonly SqliteParameter expiresNode;
        private readonly SqliteParameter expiresPhysical;
        private readonly SqliteParameter expiresCounter;
        private readonly SqliteParameter lastUsedNode;
        private readonly SqliteParameter lastUsedPhysical;
        private readonly SqliteParameter lastUsedCounter;
        private readonly SqliteParameter lastModifiedNode;
        private readonly SqliteParameter lastModifiedPhysical;
        private readonly SqliteParameter lastModifiedCounter;
        private readonly SqliteParameter revision;
        private readonly SqliteParameter state;

        private ShardInsertParameters(
            SqliteParameter key, SqliteParameter value, SqliteParameter expiresNode,
            SqliteParameter expiresPhysical, SqliteParameter expiresCounter, SqliteParameter lastUsedNode,
            SqliteParameter lastUsedPhysical, SqliteParameter lastUsedCounter, SqliteParameter lastModifiedNode,
            SqliteParameter lastModifiedPhysical, SqliteParameter lastModifiedCounter, SqliteParameter revision,
            SqliteParameter state)
        {
            this.key = key;
            this.value = value;
            this.expiresNode = expiresNode;
            this.expiresPhysical = expiresPhysical;
            this.expiresCounter = expiresCounter;
            this.lastUsedNode = lastUsedNode;
            this.lastUsedPhysical = lastUsedPhysical;
            this.lastUsedCounter = lastUsedCounter;
            this.lastModifiedNode = lastModifiedNode;
            this.lastModifiedPhysical = lastModifiedPhysical;
            this.lastModifiedCounter = lastModifiedCounter;
            this.revision = revision;
            this.state = state;
        }

        public static ShardInsertParameters Create(SqliteCommand command)
        {
            return new(
                command.Parameters.Add("@key", SqliteType.Text),
                command.Parameters.Add("@value", SqliteType.Blob),
                command.Parameters.Add("@expiresNode", SqliteType.Integer),
                command.Parameters.Add("@expiresPhysical", SqliteType.Integer),
                command.Parameters.Add("@expiresCounter", SqliteType.Integer),
                command.Parameters.Add("@lastUsedNode", SqliteType.Integer),
                command.Parameters.Add("@lastUsedPhysical", SqliteType.Integer),
                command.Parameters.Add("@lastUsedCounter", SqliteType.Integer),
                command.Parameters.Add("@lastModifiedNode", SqliteType.Integer),
                command.Parameters.Add("@lastModifiedPhysical", SqliteType.Integer),
                command.Parameters.Add("@lastModifiedCounter", SqliteType.Integer),
                command.Parameters.Add("@revision", SqliteType.Integer),
                command.Parameters.Add("@state", SqliteType.Integer)
            );
        }

        public void Bind(in PersistenceRequestItem item)
        {
            key.Value = item.Key;
            value.Value = item.Value is null ? DBNull.Value : item.Value;
            expiresNode.Value = item.ExpiresNode;
            expiresPhysical.Value = item.ExpiresPhysical;
            expiresCounter.Value = item.ExpiresCounter;
            lastUsedNode.Value = item.LastUsedNode;
            lastUsedPhysical.Value = item.LastUsedPhysical;
            lastUsedCounter.Value = item.LastUsedCounter;
            lastModifiedNode.Value = item.LastModifiedNode;
            lastModifiedPhysical.Value = item.LastModifiedPhysical;
            lastModifiedCounter.Value = item.LastModifiedCounter;
            revision.Value = item.Revision;
            state.Value = item.State;
        }
    }
}
