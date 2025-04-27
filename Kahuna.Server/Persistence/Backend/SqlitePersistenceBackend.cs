
using Kahuna.Server.Locks;
using Kommander;
using Kahuna.Server.KeyValues;
using Microsoft.Data.Sqlite;
using System.Collections.Generic;

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
public class SqlitePersistenceBackend : IPersistenceBackend, IDisposable
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
            command2.ExecuteNonQuery();
            
            const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;";
            using SqliteCommand command4 = new(pragmasQuery, connection);
            command3.ExecuteNonQuery();

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
            
            foreach (PersistenceRequestItem item in items)
            {
                (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(item.Key);

                try
                {
                    readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

                    using SqliteCommand command = new(insert, connection);

                    command.Parameters.AddWithValue("@resource", item.Key);

                    if (item.Value is null)
                        command.Parameters.AddWithValue("@owner", DBNull.Value);
                    else
                        command.Parameters.AddWithValue("@owner", item.Value);

                    command.Parameters.AddWithValue("@expiresNode", item.ExpiresNode);
                    command.Parameters.AddWithValue("@expiresPhysical", item.ExpiresPhysical);
                    command.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                    command.Parameters.AddWithValue("@lastUsedNode", item.LastUsedNode);
                    command.Parameters.AddWithValue("@lastUsedPhysical", item.LastUsedPhysical);
                    command.Parameters.AddWithValue("@lastUsedCounter", item.LastUsedCounter);
                    command.Parameters.AddWithValue("@lastModifiedNode", item.LastModifiedNode);
                    command.Parameters.AddWithValue("@lastModifiedPhysical", item.LastModifiedPhysical);
                    command.Parameters.AddWithValue("@lastModifiedCounter", item.LastModifiedCounter);
                    command.Parameters.AddWithValue("@fencingToken", item.Revision);
                    command.Parameters.AddWithValue("@state", item.State);

                    command.ExecuteNonQuery();
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
              INSERT INTO keys (key, revision, value, expiresNode, expiresPhysical, expiresCounter, lastUsedNode, lastUsedPhysical, lastUsedCounter, @lastModifiedNode, lastModifiedPhysical, lastModifiedCounter, state) 
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
                        foreach (PersistenceRequestItem item in kv.Value)
                        {
                            using SqliteCommand command = new(insertKeyRevisions, connection);

                            command.Transaction = transaction;

                            command.Parameters.AddWithValue("@key", item.Key);

                            if (item.Value is null)
                                command.Parameters.AddWithValue("@value", DBNull.Value);
                            else
                                command.Parameters.AddWithValue("@value", item.Value);

                            command.Parameters.AddWithValue("@expiresNode", item.ExpiresNode);
                            command.Parameters.AddWithValue("@expiresPhysical", item.ExpiresPhysical);
                            command.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                            command.Parameters.AddWithValue("@lastUsedNode", item.LastUsedNode);
                            command.Parameters.AddWithValue("@lastUsedPhysical", item.LastUsedPhysical);
                            command.Parameters.AddWithValue("@lastUsedCounter", item.LastUsedCounter);
                            command.Parameters.AddWithValue("@lastModifiedNode", item.LastModifiedNode);
                            command.Parameters.AddWithValue("@lastModifiedPhysical", item.LastModifiedPhysical);
                            command.Parameters.AddWithValue("@lastModifiedCounter", item.LastModifiedCounter);
                            command.Parameters.AddWithValue("@revision", item.Revision);
                            command.Parameters.AddWithValue("@state", item.State);

                            command.ExecuteNonQuery();

                            using SqliteCommand command2 = new(insertKeys, connection);

                            command2.Transaction = transaction;

                            command2.Parameters.AddWithValue("@key", item.Key);

                            if (item.Value is null)
                                command2.Parameters.AddWithValue("@value", DBNull.Value);
                            else
                                command2.Parameters.AddWithValue("@value", item.Value);

                            command2.Parameters.AddWithValue("@expiresNode", item.ExpiresNode);
                            command2.Parameters.AddWithValue("@expiresPhysical", item.ExpiresPhysical);
                            command2.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                            command2.Parameters.AddWithValue("@lastUsedNode", item.LastUsedNode);
                            command2.Parameters.AddWithValue("@lastUsedPhysical", item.LastUsedPhysical);
                            command2.Parameters.AddWithValue("@lastUsedCounter", item.LastUsedCounter);
                            command2.Parameters.AddWithValue("@lastModifiedNode", item.LastModifiedNode);
                            command2.Parameters.AddWithValue("@lastModifiedPhysical", item.LastModifiedPhysical);
                            command2.Parameters.AddWithValue("@lastModifiedCounter", item.LastModifiedCounter);
                            command2.Parameters.AddWithValue("@revision", item.Revision);
                            command2.Parameters.AddWithValue("@state", item.State);

                            command2.ExecuteNonQuery();                            
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
    /// A <see cref="LockContext"/> instance representing the acquired lock context or null if the lock could not be acquired.
    /// </returns>
    public LockContext? GetLock(string resource)
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
    /// <returns>An instance of <see cref="KeyValueContext"/> if the key exists, or null if no context is found.</returns>
    public KeyValueContext? GetKeyValue(string keyName)
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
    /// A <see cref="KeyValueContext"/> representing the key-value pair context for the specified key and revision,
    /// or null if the key or specific revision does not exist.
    /// </returns>
    public KeyValueContext? GetKeyValueRevision(string keyName, long revision)
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
    /// <see cref="ReadOnlyKeyValueContext"/> representing the value.
    /// </returns>
    public List<(string, ReadOnlyKeyValueContext)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueContext)> results = [];
        
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(prefixKeyName);
        
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

    public void Dispose()
    {
        foreach (KeyValuePair<int, (ReaderWriterLock, SqliteConnection)> conn in connections)
        {
            try
            {
                conn.Value.Item1.AcquireWriterLock(TimeSpan.FromSeconds(5));

                conn.Value.Item2.Dispose();
            }
            finally
            {
                conn.Value.Item1.ReleaseWriterLock();
            }
        }

        GC.SuppressFinalize(this);
        
        semaphore.Release();
    }
}