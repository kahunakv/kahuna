
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
    
    public async Task<bool> StoreLock(string resource, byte[]? owner, long expiresPhysical, uint expiresCounter, long fencingToken, int state)
    {
        try
        {
            (AsyncReaderWriterLock readerWriterLock, SqliteConnection connection) = await TryOpenDatabase(resource);

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

                command.Parameters.AddWithValue("@resource", resource);

                if (owner is null)
                    command.Parameters.AddWithValue("@owner", DBNull.Value);
                else
                    command.Parameters.AddWithValue("@owner", owner);

                command.Parameters.AddWithValue("@expiresLogical", expiresPhysical);
                command.Parameters.AddWithValue("@expiresCounter", expiresCounter);
                command.Parameters.AddWithValue("@fencingToken", fencingToken);
                command.Parameters.AddWithValue("@state", state);

                await command.ExecuteNonQueryAsync();

                return true;
            }
            finally
            {
                readerWriterLock.Release();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                "StoreLock: {0} {1} {2} [{3}][{4}][{5}][{6}][{7}][{8}]", 
                ex.GetType().Name, 
                ex.Message, 
                ex.StackTrace, 
                resource, 
                owner?.Length, 
                expiresPhysical, 
                expiresCounter, 
                fencingToken,
                state
            );
        }

        return false;
    }
    
    public async Task<bool> StoreKeyValue(string key, byte[]? value, long expiresPhysical, uint expiresCounter, long revision, int state)
    {
        try
        {
            (AsyncReaderWriterLock readerWriterLock, SqliteConnection connection) = await TryOpenDatabase(key);

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

                    command.Parameters.AddWithValue("@key", key);

                    if (value is null)
                        command.Parameters.AddWithValue("@value", DBNull.Value);
                    else
                        command.Parameters.AddWithValue("@value", value);

                    command.Parameters.AddWithValue("@expiresLogical", expiresPhysical);
                    command.Parameters.AddWithValue("@expiresCounter", expiresCounter);
                    command.Parameters.AddWithValue("@revision", revision);
                    command.Parameters.AddWithValue("@state", state);

                    await command.ExecuteNonQueryAsync();

                    await using SqliteCommand command2 = new(insert, connection);

                    command2.Transaction = transaction;

                    command2.Parameters.AddWithValue("@key", key);

                    if (value is null)
                        command2.Parameters.AddWithValue("@value", DBNull.Value);
                    else
                        command2.Parameters.AddWithValue("@value", value);

                    command2.Parameters.AddWithValue("@expiresLogical", expiresPhysical);
                    command2.Parameters.AddWithValue("@expiresCounter", expiresCounter);
                    command2.Parameters.AddWithValue("@revision", -1);
                    command2.Parameters.AddWithValue("@state", state);

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
        catch (Exception ex)
        {
            Console.WriteLine(
                "StoreKeyValue: {0} {1} {2} [{3}][{4}][{5}][{6}][{7}][{8}]", 
                ex.GetType().Name, 
                ex.Message, 
                ex.StackTrace, 
                key, 
                value?.Length, 
                expiresPhysical, 
                expiresCounter, 
                revision, 
                state
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
}