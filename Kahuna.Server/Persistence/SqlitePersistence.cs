
using Kahuna.Locks;
using Microsoft.Data.Sqlite;

namespace Kahuna.Persistence;

public class SqlitePersistence
{
    private SqliteConnection? sqlConnection; 
    
    private readonly SemaphoreSlim semaphore = new(1, 1);
    
    private readonly string path;
    
    private readonly string version;
    
    public SqlitePersistence(string path = ".", string version = "v1")
    {
        this.path = path;
        this.version = version;
    }
    
    private async ValueTask<SqliteConnection> TryOpenDatabase()
    {
        if (sqlConnection is not null)
            return sqlConnection;
        
        try
        {
            await semaphore.WaitAsync();

            if (sqlConnection is not null)
                return sqlConnection;

            string connectionString = $"Data Source={path}/locks_{version}.db";
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
            
            const string pragmasQuery = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY; PRAGMA wal_checkpoint(FULL);";
            await using SqliteCommand command3 = new(pragmasQuery, connection);
            await command3.ExecuteNonQueryAsync();

            sqlConnection = connection;

            return connection;
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    public async Task UpdateLock(string resource, string owner, long expiresLogical, long expiresCounter, long fencingToken, long consistency, LockState state)
    {
        try
        {
            SqliteConnection connection = await TryOpenDatabase();

            const string query = """
             INSERT INTO locks (resource, owner, expiresLogical, expiresCounter, fencingToken, consistency, state) 
             VALUES (@resource, @owner, @expiresLogical, @expiresCounter, @fencingToken, @consistency, @state) 
             ON CONFLICT(resource) DO UPDATE SET owner=@owner, expiresLogical=@expiresLogical, expiresCounter=@expiresCounter, 
             fencingToken=@fencingToken, consistency=@consistency, state=@state;
             """;

            await using SqliteCommand command = new(query, connection);

            command.Parameters.AddWithValue("@resource", resource);
            command.Parameters.AddWithValue("@owner", owner);
            command.Parameters.AddWithValue("@expiresLogical", expiresLogical);
            command.Parameters.AddWithValue("@expiresCounter", expiresCounter);
            command.Parameters.AddWithValue("@fencingToken", fencingToken);
            command.Parameters.AddWithValue("@consistency", consistency);
            command.Parameters.AddWithValue("@state", state);

            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    }
}