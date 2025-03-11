
using Kahuna.KeyValues;
using Kahuna.Locks;
using Kommander;
using Microsoft.Data.Sqlite;

namespace Kahuna.Persistence;

public class SqlitePersistence : IPersistence
{
    private const int MaxShards = 8;
    
    private readonly SemaphoreSlim semaphore = new(1, 1);
    
    private readonly Dictionary<int, SqliteConnection> connections = new();
    
    private readonly string path;
    
    private readonly string dbRevision;
    
    public SqlitePersistence(string path = ".", string dbRevision = "v1")
    {
        this.path = path;
        this.dbRevision = dbRevision;
    }
    
    private async ValueTask<SqliteConnection> TryOpenDatabase(string resource)
    {
        int shard = (int)HashUtils.ConsistentHash(resource, MaxShards);
        
        if (connections.TryGetValue(shard, out SqliteConnection? sqlConnection))
            return sqlConnection;
        
        try
        {
            await semaphore.WaitAsync();

            if (connections.TryGetValue(shard, out sqlConnection))
                return sqlConnection;
            
            string connectionString = $"Data Source={path}/locks{shard}_{dbRevision}.db";
            SqliteConnection connection = new(connectionString);

            connection.Open();

            const string createTableQuery = """
            CREATE TABLE IF NOT EXISTS locks (
                resource STRING PRIMARY KEY, 
                owner STRING, 
                expiresLogical INT, 
                expiresCounter INT, 
                fencingToken INT, 
                consistency INT,
                state INT
            );
            """;
            
            await using SqliteCommand command1 = new(createTableQuery, connection);
            await command1.ExecuteNonQueryAsync();
            
            const string createTableQuery2 = """
            CREATE TABLE IF NOT EXISTS keys (
                key STRING PRIMARY KEY, 
                value STRING, 
                expiresLogical INT, 
                expiresCounter INT, 
                revision INT,
                consistency INT,
                state INT
            );
            """;
            
            await using SqliteCommand command2 = new(createTableQuery2, connection);
            await command2.ExecuteNonQueryAsync();
            
            const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;";
            await using SqliteCommand command3 = new(pragmasQuery, connection);
            await command3.ExecuteNonQueryAsync();
            
            connections.Add(shard, connection);

            return connection;
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    public async Task<bool> StoreLock(string resource, string? owner, long expiresPhysical, uint expiresCounter, long fencingToken, int consistency, int state)
    {
        try
        {
            SqliteConnection connection = await TryOpenDatabase(resource);

            const string query = """
             INSERT INTO locks (resource, owner, expiresLogical, expiresCounter, fencingToken, consistency, state) 
             VALUES (@resource, @owner, @expiresLogical, @expiresCounter, @fencingToken, @consistency, @state) 
             ON CONFLICT(resource) DO UPDATE SET owner=@owner, expiresLogical=@expiresLogical, expiresCounter=@expiresCounter, 
             fencingToken=@fencingToken, consistency=@consistency, state=@state;
             """;
            
            await using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@resource", resource);
            
            if (owner is null)
                command.Parameters.AddWithValue("@owner", DBNull.Value);
            else
                command.Parameters.AddWithValue("@owner", owner);
            
            command.Parameters.AddWithValue("@expiresLogical", expiresPhysical);
            command.Parameters.AddWithValue("@expiresCounter", expiresCounter);
            command.Parameters.AddWithValue("@fencingToken", fencingToken);
            command.Parameters.AddWithValue("@consistency", consistency);
            command.Parameters.AddWithValue("@state", state);

            await command.ExecuteNonQueryAsync();

            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                "StoreLock: {0} {1} {2} [{3}][{4}][{5}][{6}][{7}][{8}][{9}]", 
                ex.GetType().Name, 
                ex.Message, 
                ex.StackTrace, 
                resource, 
                owner, 
                expiresPhysical, 
                expiresCounter, 
                fencingToken, 
                consistency, 
                state
            );
        }

        return false;
    }
    
    public async Task<bool> StoreKeyValue(string key, string? value, long expiresPhysical, uint expiresCounter, long revision, int consistency, int state)
    {
        try
        {
            SqliteConnection connection = await TryOpenDatabase(key);

            const string query = """
             INSERT INTO keys (key, value, expiresLogical, expiresCounter, revision, consistency, state) 
             VALUES (@key, @value, @expiresLogical, @expiresCounter, @revision, @consistency, @state) 
             ON CONFLICT(key) DO UPDATE SET value=@value, expiresLogical=@expiresLogical, expiresCounter=@expiresCounter, 
             revision=@revision, consistency=@consistency, state=@state;
             """;
            
            await using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@key", key);
            
            if (value is null)
                command.Parameters.AddWithValue("@value", DBNull.Value);
            else
                command.Parameters.AddWithValue("@value", value);
            
            command.Parameters.AddWithValue("@expiresLogical", expiresPhysical);
            command.Parameters.AddWithValue("@expiresCounter", expiresCounter);
            command.Parameters.AddWithValue("@revision", revision);
            command.Parameters.AddWithValue("@consistency", consistency);
            command.Parameters.AddWithValue("@state", state);

            await command.ExecuteNonQueryAsync();

            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                "StoreKeyValue: {0} {1} {2} [{3}][{4}][{5}][{6}][{7}][{8}][{9}]", 
                ex.GetType().Name, 
                ex.Message, 
                ex.StackTrace, 
                key, 
                value, 
                expiresPhysical, 
                expiresCounter, 
                revision, 
                consistency, 
                state
            );
        }

        return false;
    }

    public async Task<LockContext?> GetLock(string resource)
    {
        try
        {
            SqliteConnection connection = await TryOpenDatabase(resource);

            const string query = "SELECT owner, expiresLogical, expiresCounter, fencingToken, consistency, state FROM locks WHERE resource = @resource";
            await using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@resource", resource);

            await using SqliteDataReader reader = await command.ExecuteReaderAsync();

            while (reader.Read())
                return new()
                {
                    Owner = reader.IsDBNull(0) ? null : reader.GetString(0),
                    Expires = new(reader.IsDBNull(1) ? 0 : reader.GetInt64(1), reader.IsDBNull(2) ? 0 : (uint)reader.GetInt64(2)),
                    FencingToken = reader.IsDBNull(3) ? 0 : reader.GetInt64(3)
                };
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
            SqliteConnection connection = await TryOpenDatabase(keyName);

            const string query = "SELECT value, revision, expiresLogical, expiresCounter, consistency, state FROM keys WHERE key = @key";
            await using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@key", keyName);

            await using SqliteDataReader reader = await command.ExecuteReaderAsync();

            while (reader.Read())
                return new()
                {
                    Value = reader.IsDBNull(0) ? null : reader.GetString(0),
                    Revision = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                    Expires = new(reader.IsDBNull(2) ? 0 : reader.GetInt64(2), reader.IsDBNull(3) ? 0 : (uint)reader.GetInt64(3))
                };
        }
        catch (Exception ex)
        {
            Console.WriteLine("GetKeyValue: {0} {1} {2}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
        
        return null;
    }
}