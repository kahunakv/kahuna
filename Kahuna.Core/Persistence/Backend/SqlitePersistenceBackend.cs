
using Kahuna.Server.Locks;
using Kommander;
using Kahuna.Server.KeyValues;
using Microsoft.Data.Sqlite;
using System.Collections.Generic;
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
    public SqlitePersistenceBackend(string path = ".", string dbRevision = "v1")
    {
        this.path = path;
        this.dbRevision = dbRevision;
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
            
            string connectionString = $"Data Source={path}/kahuna{shard}_{dbRevision}.db";
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

            foreach (PersistenceRequestItem item in items)
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

                        foreach (PersistenceRequestItem item in kv.Value)
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
            Console.WriteLine(
                "StoreLock: {0} {1} {2}", 
                ex.GetType().Name, 
                ex.Message, 
                ex.StackTrace
            );
        }

        return false;
    }

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
              state=@state;
              """;
                    
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
              state=@state;
              """;

            Dictionary<int, List<PersistenceRequestItem>> plan = new();

            foreach (PersistenceRequestItem item in items)
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

                        foreach (PersistenceRequestItem item in kv.Value)
                        {
                            revisionsParams.Bind(item);
                            revisionsCommand.ExecuteNonQuery();

                            keysParams.Bind(item);
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
            Console.WriteLine(
                "StoreKeyValue: {0} {1} {2}", 
                ex.GetType().Name, 
                ex.Message, 
                ex.StackTrace
            );
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
            Console.WriteLine("GetLock: {0} {1} {2}", ex.GetType().Name, ex.Message, ex.StackTrace);
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
            Console.WriteLine("GetKeyValue: {0} {1} {2}", ex.GetType().Name, ex.Message, ex.StackTrace);
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
            Console.WriteLine("GetKeyValueRevision: {0} {1} {2}", ex.GetType().Name, ex.Message, ex.StackTrace);
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
             """;
            
            using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@key", prefixKeyName + "%");

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
        bool batchLimitReached = false;
        List<string>? remaining = null;

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
                            budget);

                        deleted += deletedForKey;

                        // Only a delete that consumed the whole per-key budget can have left more
                        // work behind; a short delete removed every matching row for this key.
                        if (deletedForKey == budget
                            && KeyHasMorePrunableRevisions(
                                connection,
                                key,
                                countEnabled,
                                retentionCount,
                                ageEnabled,
                                cutoffPhysical))
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
                            budget);

                        deleted += deletedForKey;

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

        result = new(keysVisited, deleted, batchLimitReached, remaining);
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
        int limit)
    {
        if (limit <= 0)
            return 0;

        long? currentRevision = GetCurrentRevision(connection, key);
        if (currentRevision is null)
            return 0;

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

        string deleteQuery = $"""
            DELETE FROM keys_revisions
            WHERE rowid IN (
                SELECT rowid
                FROM keys_revisions
                WHERE key = @key
                  AND revision <> @currentRevision
                  AND ({policy})
                LIMIT @limit
            )
            """;

        using SqliteCommand command = new(deleteQuery, connection);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@currentRevision", currentRevision.Value);

        if (countEnabled)
            command.Parameters.AddWithValue("@retentionCount", retentionCount);

        if (ageEnabled)
            command.Parameters.AddWithValue("@cutoffPhysical", cutoffPhysical);

        command.Parameters.AddWithValue("@limit", limit);

        return command.ExecuteNonQuery();
    }

    private static bool KeyHasMorePrunableRevisions(
        SqliteConnection connection,
        string key,
        bool countEnabled,
        int retentionCount,
        bool ageEnabled,
        long cutoffPhysical)
    {
        long? currentRevision = GetCurrentRevision(connection, key);
        if (currentRevision is null)
            return false;

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

        string query = $"""
            SELECT 1
            FROM keys_revisions
            WHERE key = @key
              AND revision <> @currentRevision
              AND ({policy})
            LIMIT 1
            """;

        using SqliteCommand command = new(query, connection);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@currentRevision", currentRevision.Value);

        if (countEnabled)
            command.Parameters.AddWithValue("@retentionCount", retentionCount);

        if (ageEnabled)
            command.Parameters.AddWithValue("@cutoffPhysical", cutoffPhysical);

        return command.ExecuteScalar() is not null;
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

        public void Bind(PersistenceRequestItem item)
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
