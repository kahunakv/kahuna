
using DotNext.Threading;
using Kahuna.Server.Locks;
using Kommander;
using Kahuna.Server.KeyValues;
using Microsoft.Data.Sqlite;

namespace Kahuna.Server.Persistence;

public class SqlitePersistence : IPersistence
{
    private const int MaxShards = 8;
    
    private readonly SemaphoreSlim semaphore = new(1, 1);
    
    private readonly Dictionary<int, (AsyncReaderWriterLock, SqliteConnection)> connections = new();
    
    private readonly string path;
    
    private readonly string dbRevision;
    
    public SqlitePersistence(string path = ".", string dbRevision = "v1")
    {
        this.path = path;
        this.dbRevision = dbRevision;
    }
    
    private async ValueTask<(AsyncReaderWriterLock readerWriterLock, SqliteConnection connection)> TryOpenDatabase(string resource)
    {
        int shard = (int)HashUtils.ConsistentHash(resource, MaxShards);
        
        return await TryOpenDatabaseByShard(shard);
    }
    
    private async ValueTask<(AsyncReaderWriterLock readerWriterLock, SqliteConnection connection)> TryOpenDatabaseByShard(int shard)
    {
        if (connections.TryGetValue(shard, out (AsyncReaderWriterLock readerWriterLock, SqliteConnection connection) sqlConnection))
            return sqlConnection;
        
        try
        {
            await semaphore.WaitAsync();

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
            
            await using SqliteCommand command1 = new(createTableQuery, connection);
            await command1.ExecuteNonQueryAsync();
            
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
            
            await using SqliteCommand command2 = new(createTableQuery2, connection);
            await command2.ExecuteNonQueryAsync();
            
            const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;";
            await using SqliteCommand command3 = new(pragmasQuery, connection);
            await command3.ExecuteNonQueryAsync();

            AsyncReaderWriterLock readerWriterLock = new();
            
            connections.Add(shard, (readerWriterLock, connection));

            return (readerWriterLock, connection);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    public async Task<bool> StoreLocks(List<PersistenceRequestItem> items)
    {
        try
        {
            foreach (PersistenceRequestItem item in items)
            {
                (AsyncReaderWriterLock readerWriterLock, SqliteConnection connection) = await TryOpenDatabase(item.Key);

                try
                {
                    await readerWriterLock.EnterWriteLockAsync(TimeSpan.FromSeconds(5));

                    const string insert = """
                      INSERT INTO locks (resource, owner, expiresLogical, expiresCounter, fencingToken, state) 
                      VALUES (@resource, @owner, @expiresLogical, @expiresCounter, @fencingToken, @state) 
                      ON CONFLICT(resource) DO UPDATE SET owner=@owner, expiresLogical=@expiresLogical, expiresCounter=@expiresCounter, 
                      fencingToken=@fencingToken, state=@state;
                      """;

                    await using SqliteCommand command = new(insert, connection);

                    command.Parameters.AddWithValue("@resource", item.Key);

                    if (item.Value is null)
                        command.Parameters.AddWithValue("@owner", DBNull.Value);
                    else
                        command.Parameters.AddWithValue("@owner", item.Value);

                    command.Parameters.AddWithValue("@expiresLogical", item.ExpiresPhysical);
                    command.Parameters.AddWithValue("@expiresCounter", item.ExpiresCounter);
                    command.Parameters.AddWithValue("@fencingToken", item.Revision);
                    command.Parameters.AddWithValue("@state", item.State);

                    await command.ExecuteNonQueryAsync();

                    return true;
                }
                finally
                {
                    readerWriterLock.Release();
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
    
    public async Task<bool> StoreKeyValues(List<PersistenceRequestItem> items)
    {
        try
        {
            foreach (PersistenceRequestItem item in items)
            {

                (AsyncReaderWriterLock readerWriterLock, SqliteConnection connection) = await TryOpenDatabase(item.Key);

                try
                {
                    await readerWriterLock.EnterWriteLockAsync(TimeSpan.FromSeconds(5));

                    const string insert = """
                      INSERT INTO keys (key, revision, value, expiresLogical, expiresCounter, state) 
                      VALUES (@key, @revision, @value, @expiresLogical, @expiresCounter, @state) 
                      ON CONFLICT(key, revision) DO UPDATE SET value=@value, expiresLogical=@expiresLogical, expiresCounter=@expiresCounter, state=@state;
                      """;

                    await using SqliteTransaction transaction = connection.BeginTransaction();

                    try
                    {
                        await using SqliteCommand command = new(insert, connection);

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

                        await command.ExecuteNonQueryAsync();

                        await using SqliteCommand command2 = new(insert, connection);

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

                        await command2.ExecuteNonQueryAsync();

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
                    readerWriterLock.Release();
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

    public async Task<LockContext?> GetLock(string resource)
    {
        try
        {
            (AsyncReaderWriterLock readerWriterLock, SqliteConnection connection) = await TryOpenDatabase(resource);
            
            try
            {
                await readerWriterLock.EnterReadLockAsync(TimeSpan.FromSeconds(5));

                const string query = "SELECT owner, expiresLogical, expiresCounter, fencingToken, state FROM locks WHERE resource = @resource";
                await using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@resource", resource);

                await using SqliteDataReader reader = await command.ExecuteReaderAsync();

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
                readerWriterLock.Release();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("GetLock: {0} {1} {2}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
        
        return null;
    }

    public async Task<KeyValueContext?> GetKeyValue(string keyName)
    {
        try
        {
            (AsyncReaderWriterLock readerWriterLock, SqliteConnection connection) = await TryOpenDatabase(keyName);

            try
            {
                await readerWriterLock.EnterReadLockAsync(TimeSpan.FromSeconds(5));

                const string query = "SELECT value, revision, expiresLogical, expiresCounter, state FROM keys WHERE key = @key AND revision = -1";
                await using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@key", keyName);

                await using SqliteDataReader reader = await command.ExecuteReaderAsync();

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
                readerWriterLock.Release();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("GetKeyValue: {0} {1} {2}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
        
        return null;
    }
    
    public async Task<KeyValueContext?> GetKeyValueRevision(string keyName, long revision)
    {
        try
        {
            (AsyncReaderWriterLock readerWriterLock, SqliteConnection connection) = await TryOpenDatabase(keyName);

            try
            {
                await readerWriterLock.EnterReadLockAsync(TimeSpan.FromSeconds(5));

                const string query = "SELECT value, revision, expiresLogical, expiresCounter, state FROM keys WHERE key = @key AND revision = @revision";
                await using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@key", keyName);
                command.Parameters.AddWithValue("@revision", revision);

                await using SqliteDataReader reader = await command.ExecuteReaderAsync();

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
                readerWriterLock.Release();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("GetKeyValue: {0} {1} {2}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
        
        return null;
    }
    
    public async IAsyncEnumerable<(string, ReadOnlyKeyValueContext)> GetKeyValueByPrefix(string prefixKeyName)
    {
        for (int i = 0; i < MaxShards; i++)
        {
            (AsyncReaderWriterLock readerWriterLock, SqliteConnection connection) = await TryOpenDatabaseByShard(i);

            try
            {
                await readerWriterLock.EnterReadLockAsync(TimeSpan.FromSeconds(5));

                const string query = "SELECT key, value, revision, expiresLogical, expiresCounter, state FROM keys WHERE key LIKE @key";
                await using SqliteCommand command = new(query, connection);

                command.Parameters.AddWithValue("@key", prefixKeyName + "%");

                await using SqliteDataReader reader = await command.ExecuteReaderAsync();

                while (reader.Read())
                    yield return (reader.IsDBNull(0) ? "" : reader.GetString(0), new(
                        value: reader.IsDBNull(1) ? null : (byte[])reader[1],
                        revision: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                        expires: new(
                            reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                            reader.IsDBNull(4) ? 0 : (uint)reader.GetInt64(4)
                        )
                    ));
            }
            finally
            {
                readerWriterLock.Release();
            }
        }
    }
}