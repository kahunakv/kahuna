
using Kahuna.Server.Locks;
using Kommander;
using Kahuna.Server.KeyValues;
using Microsoft.Data.Sqlite;

namespace Kahuna.Server.Persistence.Backend;

public class SqlitePersistenceBackend : IPersistenceBackend, IDisposable
{
    private const int MaxShards = 8;
    
    private readonly SemaphoreSlim semaphore = new(1, 1);
    
    private readonly Dictionary<int, (ReaderWriterLock, SqliteConnection)> connections = new();
    
    private readonly string path;
    
    private readonly string dbRevision;
    
    public SqlitePersistenceBackend(string path = ".", string dbRevision = "v1")
    {
        this.path = path;
        this.dbRevision = dbRevision;
    }
    
    private (ReaderWriterLock readerWriterLock, SqliteConnection connection) TryOpenDatabase(string resource)
    {
        int shard = (int)HashUtils.InversePrefixedHash(resource, '/', MaxShards);
        
        return TryOpenDatabaseByShard(shard);
    }
    
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
                expiresPhysical INT,
                expiresCounter INT, 
                fencingToken INT, 
                lastUsedPhysical INT,
                lastUsedCounter INT,
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
                expiresPhysical INT, 
                expiresCounter INT, 
                lastUsedPhysical INT,
                lastUsedCounter INT,
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
                 lastUsedPhysical INT,
                 lastUsedCounter INT,
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
    
    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        try
        {
            const string insert = """
              INSERT INTO locks (resource, owner, expiresPhysical, expiresCounter, fencingToken, state) 
              VALUES (@resource, @owner, @expiresPhysical, @expiresCounter, @fencingToken, @state) 
              ON CONFLICT(resource) DO UPDATE SET 
              owner=@owner, 
              expiresPhysical=@expiresPhysical, 
              expiresCounter=@expiresCounter,
              lastUsedPhysical=@lastUsedPhysical, 
              lastUsedCounter=@lastUsedCounter,
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

                    command.Parameters.AddWithValue("@expiresPhysical", item.ExpiresPhysical);
                    command.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                    command.Parameters.AddWithValue("@lastUsedPhysical", item.LastUsedPhysical);
                    command.Parameters.AddWithValue("@lastUsedCounter", item.LastUsedCounter);
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
    
    public bool StoreKeyValues(List<PersistenceRequestItem> items)
    {
        try
        {
            const string insertKeys = """
              INSERT INTO keys (key, revision, value, expiresPhysical, expiresCounter, lastUsedPhysical, lastUsedCounter, lastModifiedPhysical, lastModifiedCounter, state) 
              VALUES (@key, @revision, @value, @expiresPhysical, @expiresCounter, @lastUsedPhysical, @lastUsedCounter, @lastModifiedPhysical, @lastModifiedCounter, @state) 
              ON CONFLICT(key) DO UPDATE SET
              revision=@revision,
              value=@value, 
              expiresPhysical=@expiresPhysical, 
              expiresCounter=@expiresCounter,
              lastUsedPhysical=@lastUsedPhysical, 
              lastUsedCounter=@lastUsedCounter,
              lastModifiedPhysical=@lastModifiedPhysical, 
              lastModifiedCounter=@lastModifiedCounter, 
              state=@state;
              """;
                    
            const string insertKeyRevisions = """
              INSERT INTO keys_revisions (key, revision, value, expiresPhysical, expiresCounter, lastUsedPhysical, lastUsedCounter, lastModifiedPhysical, lastModifiedCounter, state) 
              VALUES (@key, @revision, @value, @expiresPhysical, @expiresCounter, @lastUsedPhysical, @lastUsedCounter, @lastModifiedPhysical, @lastModifiedCounter, @state) 
              ON CONFLICT(key, revision) DO UPDATE SET 
              value=@value, 
              expiresPhysical=@expiresPhysical, 
              expiresCounter=@expiresCounter,
              lastUsedPhysical=@lastUsedPhysical, 
              lastUsedCounter=@lastUsedCounter,
              lastModifiedPhysical=@lastModifiedPhysical, 
              lastModifiedCounter=@lastModifiedCounter, 
              state=@state;
              """;
            
            Console.WriteLine("{0}", System.Text.Json.JsonSerializer.Serialize(items));
            
            foreach (PersistenceRequestItem item in items)
            {
                (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(item.Key);

                try
                {
                    readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

                    using SqliteTransaction transaction = connection.BeginTransaction();

                    try
                    {
                        using SqliteCommand command = new(insertKeyRevisions, connection);

                        command.Transaction = transaction;

                        command.Parameters.AddWithValue("@key", item.Key);

                        if (item.Value is null)
                            command.Parameters.AddWithValue("@value", DBNull.Value);
                        else
                            command.Parameters.AddWithValue("@value", item.Value);

                        command.Parameters.AddWithValue("@expiresPhysical", item.ExpiresPhysical);
                        command.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                        command.Parameters.AddWithValue("@lastUsedPhysical", item.LastUsedPhysical);
                        command.Parameters.AddWithValue("@lastUsedCounter", item.LastUsedCounter);
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

                        command2.Parameters.AddWithValue("@expiresPhysical", item.ExpiresPhysical);
                        command2.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                        command2.Parameters.AddWithValue("@lastUsedPhysical", item.LastUsedPhysical);
                        command2.Parameters.AddWithValue("@lastUsedCounter", item.LastUsedCounter);
                        command2.Parameters.AddWithValue("@lastModifiedPhysical", item.LastModifiedPhysical);
                        command2.Parameters.AddWithValue("@lastModifiedCounter", item.LastModifiedCounter);
                        command2.Parameters.AddWithValue("@revision", item.Revision);
                        command2.Parameters.AddWithValue("@state", item.State);

                        command2.ExecuteNonQuery();

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

    public LockContext? GetLock(string resource)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(resource);
            
            try
            {
                readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

                const string query = """
                SELECT owner, expiresPhysical, expiresCounter, lastUsedPhysical, lastUsedCounter, 
                       lastModifiedPhysical, lastModifiedCounter, fencingToken, state                               
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
                        Expires = new(reader.IsDBNull(1) ? 0 : reader.GetInt64(1), reader.IsDBNull(2) ? 0 : (uint)reader.GetInt64(2)),
                        LastUsed = new(reader.IsDBNull(3) ? 0 : reader.GetInt64(3), reader.IsDBNull(4) ? 0 : (uint)reader.GetInt64(4)),
                        LastModified = new(reader.IsDBNull(5) ? 0 : reader.GetInt64(5), reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6)),
                        FencingToken = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                        State = reader.IsDBNull(8) ? LockState.Locked : (LockState)reader.GetInt32(8)
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
                 SELECT value, expiresPhysical, expiresCounter, lastUsedPhysical, lastUsedCounter, 
                        lastModifiedPhysical, lastModifiedCounter, revision, state                               
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
                        Expires = new(reader.IsDBNull(1) ? 0 : reader.GetInt64(1), reader.IsDBNull(2) ? 0 : (uint)reader.GetInt64(2)),
                        LastUsed = new(reader.IsDBNull(3) ? 0 : reader.GetInt64(3), reader.IsDBNull(4) ? 0 : (uint)reader.GetInt64(4)),
                        LastModified = new(reader.IsDBNull(5) ? 0 : reader.GetInt64(5), reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6)),
                        Revision = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                        State = reader.IsDBNull(8) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(8)
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
    
    public KeyValueContext? GetKeyValueRevision(string keyName, long revision)
    {
        try
        {
            (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(keyName);

            try
            {
                readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

                const string query = """
                 SELECT value, expiresPhysical, expiresCounter, lastUsedPhysical, lastUsedCounter, 
                        lastModifiedPhysical, lastModifiedCounter, revision, state                               
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
                        Expires = new(reader.IsDBNull(1) ? 0 : reader.GetInt64(1), reader.IsDBNull(2) ? 0 : (uint)reader.GetInt64(2)),
                        LastUsed = new(reader.IsDBNull(3) ? 0 : reader.GetInt64(3), reader.IsDBNull(4) ? 0 : (uint)reader.GetInt64(4)),
                        LastModified = new(reader.IsDBNull(5) ? 0 : reader.GetInt64(5), reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6)),
                        Revision = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                        State = reader.IsDBNull(8) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(8)
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
    
    public List<(string, ReadOnlyKeyValueContext)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueContext)> results = [];
        
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(prefixKeyName);
        
        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

            const string query = """
             SELECT key, value, revision, expiresPhysical, expiresCounter, lastUsedPhysical, lastUsedCounter, 
                    lastModifiedPhysical, lastModifiedCounter, state                               
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
                        reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                        reader.IsDBNull(4) ? 0 : (uint)reader.GetInt64(4)
                    ),
                    lastUsed: new(
                        reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                        reader.IsDBNull(6) ? 0 : (uint)reader.GetInt64(6)
                    ),
                    lastModified: new(
                        reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                        reader.IsDBNull(8) ? 0 : (uint)reader.GetInt64(8)
                    ),
                    state: reader.IsDBNull(9) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(9)
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