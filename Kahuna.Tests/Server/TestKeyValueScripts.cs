
using Kommander;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

[Collection("ClusterTests")]
public class TestKeyValueScripts : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScripts(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }    
    
    [Theory, CombinatorialData]
    public async Task TestSetGetExecuteScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);
        try
        {

            // Persistent tests
            string script = "SET pp 'hello world'";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);
        
            script = "SET @key 'hello world'";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@key", Value = "pp" }]);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(1, resp.Revision);
        
            script = "SET pp 'another world'";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(2, resp.Revision);
        
            script = "GET pp";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(2, resp.Revision);
            Assert.Equal("another world"u8.ToArray(), resp.Value);
                
            // Ephemeral tests
            script = "ESET pp 'hello world'";
        
            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);
        
            script = "ESET @key 'hello world'";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@key", Value = "pp" }]);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(1, resp.Revision);
        
            script = "ESET pp 'another world'";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(2, resp.Revision);
        
            script = "EGET pp";
        
            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(2, resp.Revision);
            Assert.Equal("another world"u8.ToArray(), resp.Value);

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestSetGetExtendExecuteScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "SET pp 'hello world' EX 1000";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);        
        
            script = "GET pp";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            await Task.Delay(1100, TestContext.Current.CancellationToken);
        
            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);   
        
            // Ephemeral tests        
            script = "ESET pp 'hello world' EX 1000";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);        
        
            script = "EGET pp";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            await Task.Delay(1100, TestContext.Current.CancellationToken);
        
            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);   
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetExtendExecuteScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "SET pp 'hello world' EX 1000";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);
        
            script = "SET pp 'hello world' NX";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.NotSet, resp.Type);
            Assert.Equal(0, resp.Revision);
        
            script = "SET pp 'hello world' XX";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(1, resp.Revision);
        
            script = "SET pp 'hello hello world' CMP 'hello world'";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(2, resp.Revision);
        
            script = "SET pp 'hello hello world' CMP 'hello world'";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.NotSet, resp.Type);
            Assert.Equal(2, resp.Revision);
        
            script = "SET pp 'hello world' CMPREV 2";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(3, resp.Revision);
        
            script = "SET pp 'hello hello world' CMPREV 2";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.NotSet, resp.Type);
            Assert.Equal(3, resp.Revision);
        
            script = "GET pp";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(3, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "EXTEND pp 2000";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Extended, resp.Type);        
        
            await Task.Delay(1100, TestContext.Current.CancellationToken);

            // A Raft re-election during the delay may leave the new leader momentarily without the
            // WAL-restored entry; retry until it is visible.
            script = "GET pp";
            await WaitUntilAsync(async () =>
            {
                resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
                return resp.Type == KeyValueResponseType.Get;
            });
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(3, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);   
        
            // Ephemeral tests — use a short TTL (50ms) + short delay (100ms, below the CI minimum
            // election timeout of 150ms) so a Raft re-election cannot occur during the wait and evict
            // the non-replicated ephemeral key.
            script = "ESET pp 'hello world' EX 50";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);

            script = "EGET pp";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);

            script = "EEXTEND pp 2000";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Extended, resp.Type);

            // 100ms > 50ms original TTL: key would have expired without the extend.
            await Task.Delay(100, TestContext.Current.CancellationToken);

            script = "EGET pp";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetExistsExecuteScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "SET pp 'hello world' EX 1000";

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);
        
            script = "GET pp";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "EXISTS pp";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Exists, resp.Type);
        
            script = "EXISTS ppn";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
        
            // Ephemeral tests        
            script = "ESET pp 'hello world' EX 1000";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);        
        
            script = "EGET pp";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "EEXISTS pp";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Exists, resp.Type);
        
            script = "EEXISTS ppn";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestSetGetSameScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'hello world' EX 1000                       
            GET pp
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = """
            ESET pp 'hello world' EX 1000                       
            EGET pp
            """;

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'hello world' EX 1000
            SET pp 'hello world 2' EX 1000                       
            GET pp
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
            script = """
            ESET pp 'hello world' EX 1000
            ESET pp 'hello world 2' EX 1000
            EGET pp
            """;

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameNxScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'hello world' EX 1000
            SET pp 'hello world 2' EX 1000 NX                    
            GET pp
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = """
            ESET pp 'hello world' EX 1000
            ESET pp 'hello world 2' EX 1000 NX
            EGET pp
            """;

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameXxScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'hello world' EX 1000
            SET pp 'hello world 2' EX 1000 XX                    
            GET pp
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
            script = """
            ESET pp 'hello world' EX 1000
            ESET pp 'hello world 2' EX 1000 XX
            EGET pp
            """;

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameConditionalScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            LET s = 'hello world'
            IF s = 'hello world' THEN
                SET pp s EX 1000
            ELSE
                SET pp 'hello world 2' EX 1000
            END                               
            GET pp
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
             script = """
              LET s = 'hello world'
              IF s = 'hello world' THEN
                  ESET pp s EX 1000
              ELSE
                  ESET pp 'hello world 2' EX 1000
              END                               
              EGET pp
              """;

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSetRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'hello world'                      
            GET pp
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = """
             BEGIN 
              SET pp 'another world'
              ROLLBACK
             END
             """;
        
            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "GET pp";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetDeleteRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'hello world'                      
            GET pp
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = """
             BEGIN 
              DELETE pp
              ROLLBACK
             END
             """;
        
            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "GET pp";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = """
             ESET pp 'hello world'                      
             EGET pp
             """;

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = """
             BEGIN 
              EDELETE pp
              ROLLBACK
             END
             """;
        
            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "EGET pp";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public Task TestSetGetExtendRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions) =>
        RetryAsync(() => RunSetGetExtendRollbackScript(storage, partitions));

    private async Task RunSetGetExtendRollbackScript(string storage, int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);
        try
        {

        // Unique key per run avoids interference when test classes execute in parallel
        string key = $"ppext{Guid.NewGuid():N}";

        string script = $"""
        SET {key} 'hello world'
        GET {key}
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);

        script = $"""
         BEGIN
          EXTEND {key} 500
          ROLLBACK
         END
         """;

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);

        await Task.Delay(1000, TestContext.Current.CancellationToken);

        script = $"GET {key}";

        await WaitUntilAsync(async () =>
        {
            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            return resp.Type == KeyValueResponseType.Get;
        });
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);

        string ekey = $"ppexte{Guid.NewGuid():N}";

        script = $"""
         ESET {ekey} 'hello world'
         EGET {ekey}
         """;

        resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);

        script = $"""
         BEGIN
          EEXTEND {ekey} 500
          ROLLBACK
         END
         """;

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);

        // 100ms < 150ms CI election floor — no re-election can occur; ephemeral key survives.
        await Task.Delay(100, TestContext.Current.CancellationToken);

        script = $"EGET {ekey}";

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestSetGetMultiExecuteScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp1 'hello world 1' EX 1000
            SET pp2 'hello world 2' EX 1000
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);        
        
            script = "GET pp1";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world 1"u8.ToArray(), resp.Value);
        
            script = "GET pp2";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
            script = """
             ESET pp1 'hello world 1' EX 1000
             ESET pp2 'hello world 2' EX 1000
             """;
        
            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);        
        
            script = "EGET pp1";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world 1"u8.ToArray(), resp.Value);
        
            script = "EGET pp2";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
            script = """
            SET pp1 'hello world 1' EX 1000
            SET pp2 'hello world 2' EX 1000
        
            LET pp1v = GET pp1
            LET pp2v = GET pp2 
        
            IF pp1v = 'hello world 1' && pp2v = 'hello world 2' THEN
                SET pp3 'hello world 3' EX 1000
            ELSE
                SET pp4 'hello world 4' EX 1000
            END
            """;

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);    
        
            script = "GET pp3";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world 3"u8.ToArray(), resp.Value);
        
            script = "GET pp4";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
        
            script = """
             ESET pp1 'hello world 1' EX 1000
             ESET pp2 'hello world 2' EX 1000

             LET pp1v = EGET pp1
             LET pp2v = EGET pp2 

             IF pp1v = 'hello world 1' && pp2v = 'hello world 2' THEN
                 ESET pp3 'hello world 3' EX 1000
             ELSE
                 ESET pp4 'hello world 4' EX 1000
             END
             """;

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);    
        
            script = "EGET pp3";

            resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world 3"u8.ToArray(), resp.Value);
        
            script = "EGET pp4";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetByBucketExecuteScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET `my-prefix/pp1` 'hello world 1'
            SET `my-prefix/pp2` 'hello world 2'
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);

            //await Task.Delay(1000);
        
            script = "get by bucket `my-prefix`";

            resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
                
            Assert.NotNull(resp.Values);
            Assert.Equal(2, resp.Values.Count);
        
            Assert.Equal("hello world 1"u8.ToArray(), resp.Values[0].Value);
            Assert.Equal("hello world 2"u8.ToArray(), resp.Values[1].Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
