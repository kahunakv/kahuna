
using DotNext.Threading;
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
                expiresLogical INT, 
                expiresCounter INT, 
                fencingToken INT, 
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
                expiresLogical INT, 
                expiresCounter INT, 
                state INT,
                PRIMARY KEY (key, revision)
            );
            """;
            
            using SqliteCommand command2 = new(createTableQuery2, connection);
            command2.ExecuteNonQuery();
            
            const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;";
            using SqliteCommand command3 = new(pragmasQuery, connection);
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
            foreach (PersistenceRequestItem item in items)
            {
                (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(item.Key);

                try
                {
                    readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

                    const string insert = """
                      INSERT INTO locks (resource, owner, expiresLogical, expiresCounter, fencingToken, state) 
                      VALUES (@resource, @owner, @expiresLogical, @expiresCounter, @fencingToken, @state) 
                      ON CONFLICT(resource) DO UPDATE SET owner=@owner, expiresLogical=@expiresLogical, expiresCounter=@expiresCounter, 
                      fencingToken=@fencingToken, state=@state;
                      """;

                    using SqliteCommand command = new(insert, connection);

                    command.Parameters.AddWithValue("@resource", item.Key);

                    if (item.Value is null)
                        command.Parameters.AddWithValue("@owner", DBNull.Value);
                    else
                        command.Parameters.AddWithValue("@owner", item.Value);

                    command.Parameters.AddWithValue("@expiresLogical", item.ExpiresPhysical);
                    command.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                    command.Parameters.AddWithValue("@fencingToken", item.Revision);
                    command.Parameters.AddWithValue("@state", item.State);

                    command.ExecuteNonQuery();

                    return true;
                }
                finally
                {
                    readerWriterLock.ReleaseWriterLock();
                }
            }
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
            foreach (PersistenceRequestItem item in items)
            {
                (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(item.Key);

                try
                {
                    readerWriterLock.AcquireWriterLock(TimeSpan.FromSeconds(5));

                    const string insert = """
                      INSERT INTO keys (key, revision, value, expiresLogical, expiresCounter, state) 
                      VALUES (@key, @revision, @value, @expiresLogical, @expiresCounter, @state) 
                      ON CONFLICT(key, revision) DO UPDATE SET value=@value, expiresLogical=@expiresLogical, expiresCounter=@expiresCounter, state=@state;
                      """;

                    using SqliteTransaction transaction = connection.BeginTransaction();

                    try
                    {
                        using SqliteCommand command = new(insert, connection);

                        command.Transaction = transaction;

                        command.Parameters.AddWithValue("@key", item.Key);

                        if (item.Value is null)
                            command.Parameters.AddWithValue("@value", DBNull.Value);
                        else
                            command.Parameters.AddWithValue("@value", item.Value);

                        command.Parameters.AddWithValue("@expiresLogical", item.ExpiresPhysical);
                        command.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                        command.Parameters.AddWithValue("@revision", item.Revision);
                        command.Parameters.AddWithValue("@state", item.State);

                        command.ExecuteNonQuery();

                        using SqliteCommand command2 = new(insert, connection);

                        command2.Transaction = transaction;

                        command2.Parameters.AddWithValue("@key", item.Key);

                        if (item.Value is null)
                            command2.Parameters.AddWithValue("@value", DBNull.Value);
                        else
                            command2.Parameters.AddWithValue("@value", item.Value);

                        command2.Parameters.AddWithValue("@expiresLogical", item.ExpiresPhysical);
                        command2.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                        command2.Parameters.AddWithValue("@revision", -1);
                        command2.Parameters.AddWithValue("@state", item.State);

                        command2.ExecuteNonQuery();

                        transaction.Commit();

                        return true;
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

                const string query = "SELECT owner, expiresLogical, expiresCounter, fencingToken, state FROM locks WHERE resource = @resource";
                using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@resource", resource);

                using SqliteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                    return new()
                    {
                        Owner = reader.IsDBNull(0) ? null : (byte[])reader[0],
                        Expires = new(reader.IsDBNull(1) ? 0 : reader.GetInt64(1), reader.IsDBNull(2) ? 0 : (uint)reader.GetInt64(2)),
                        FencingToken = reader.IsDBNull(3) ? 0 : reader.GetInt64(3)
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
                readerWriterLock.AcquireReadLock(TimeSpan.FromSeconds(5));

                const string query = "SELECT value, revision, expiresLogical, expiresCounter, state FROM keys WHERE key = @key AND revision = -1";
                using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@key", keyName);

                using SqliteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                    return new()
                    {
                        Value = reader.IsDBNull(0) ? null : (byte[])reader[0],
                        Revision = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                        Expires = new(reader.IsDBNull(2) ? 0 : reader.GetInt64(2), reader.IsDBNull(3) ? 0 : (uint)reader.GetInt64(3))
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

                const string query = "SELECT value, revision, expiresLogical, expiresCounter, state FROM keys WHERE key = @key AND revision = @revision";
                using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@key", keyName);
                command.Parameters.AddWithValue("@revision", revision);

                using SqliteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                    return new()
                    {
                        Value = reader.IsDBNull(0) ? null : (byte[])reader[0],
                        Revision = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                        Expires = new(reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                            reader.IsDBNull(3) ? 0 : (uint)reader.GetInt64(3))
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
    
    public List<(string, ReadOnlyKeyValueContext)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueContext)> results = [];
        
        (ReaderWriterLock readerWriterLock, SqliteConnection connection) = TryOpenDatabase(prefixKeyName);
        
        try
        {
            readerWriterLock.AcquireReaderLock(TimeSpan.FromSeconds(5));

            const string query = "SELECT key, value, revision, expiresLogical, expiresCounter, state FROM keys WHERE key LIKE @key";
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
                    state: reader.IsDBNull(5) ? KeyValueState.Undefined : (KeyValueState)reader.GetInt32(5)
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