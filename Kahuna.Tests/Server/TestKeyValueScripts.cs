
using Kommander;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestKeyValueScripts : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScripts(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddXUnit(outputHelper)
                .AddFilter("Kommander", LogLevel.Warning)
                .AddFilter("Kahuna", LogLevel.Debug)
                .SetMinimumLevel(LogLevel.Debug);
        });

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string GetRandomKey()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetExecuteScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = "SET pp 'hello world'";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "SET @key 'hello world'";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@key", Value = "pp" }]);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(1, resp.Revision);
        
        script = "SET pp 'another world'";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(2, resp.Revision);
        
        script = "GET pp";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(2, resp.Revision);
        Assert.Equal("another world"u8.ToArray(), resp.Value);
                
        // Ephemeral tests
        script = "ESET pp 'hello world'";
        
        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "ESET @key 'hello world'";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@key", Value = "pp" }]);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(1, resp.Revision);
        
        script = "ESET pp 'another world'";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(2, resp.Revision);
        
        script = "EGET pp";
        
        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(2, resp.Revision);
        Assert.Equal("another world"u8.ToArray(), resp.Value);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory, CombinatorialData]
    public async Task TestSetGetExtendExecuteScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = "SET pp 'hello world' EX 1000";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);        
        
        script = "GET pp";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await Task.Delay(1100, TestContext.Current.CancellationToken);
        
        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);   
        
        // Ephemeral tests        
        script = "ESET pp 'hello world' EX 1000";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);        
        
        script = "EGET pp";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await Task.Delay(1100, TestContext.Current.CancellationToken);
        
        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);   
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetExtendExecuteScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = "SET pp 'hello world' EX 1000";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);        
        
        script = "GET pp";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "EXTEND pp 2000";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Extended, resp.Type);        
        
        await Task.Delay(1100, TestContext.Current.CancellationToken);
        
        script = "GET pp";
        
        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);   
        
        // Ephemeral tests        
        script = "ESET pp 'hello world' EX 1000";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);        
        
        script = "EGET pp";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "EEXTEND pp 2000";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Extended, resp.Type);        
        
        await Task.Delay(1000, TestContext.Current.CancellationToken);
        
        script = "EGET pp";
        
        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);    
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetExistsExecuteScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = "SET pp 'hello world' EX 1000";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "GET pp";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "EXISTS pp";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Exists, resp.Type);
        
        script = "EXISTS ppn";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
        
        // Ephemeral tests        
        script = "ESET pp 'hello world' EX 1000";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);        
        
        script = "EGET pp";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "EEXISTS pp";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Exists, resp.Type);
        
        script = "EEXISTS ppn";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory, CombinatorialData]
    public async Task TestSetGetSameScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'hello world' EX 1000                       
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
        ESET pp 'hello world' EX 1000                       
        EGET pp
        """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'hello world' EX 1000
        SET pp 'hello world 2' EX 1000                       
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
        script = """
        ESET pp 'hello world' EX 1000
        ESET pp 'hello world 2' EX 1000
        EGET pp
        """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameNxScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'hello world' EX 1000
        SET pp 'hello world 2' EX 1000 NX                    
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
        ESET pp 'hello world' EX 1000
        ESET pp 'hello world 2' EX 1000 NX
        EGET pp
        """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameXxScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'hello world' EX 1000
        SET pp 'hello world 2' EX 1000 XX                    
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
        script = """
        ESET pp 'hello world' EX 1000
        ESET pp 'hello world 2' EX 1000 XX
        EGET pp
        """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world 2"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameConditionalScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

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

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
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

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSetRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'hello world'                      
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
         BEGIN 
          SET pp 'another world'
          ROLLBACK
         END
         """;
        
        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "GET pp";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetDeleteRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'hello world'                      
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
         BEGIN 
          DELETE pp
          ROLLBACK
         END
         """;
        
        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "GET pp";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
         ESET pp 'hello world'                      
         EGET pp
         """;

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
         BEGIN 
          EDELETE pp
          ROLLBACK
         END
         """;
        
        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "EGET pp";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetExtendRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'hello world'                      
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
         BEGIN 
          EXTEND pp 500
          ROLLBACK
         END
         """;
        
        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        await Task.Delay(1000, TestContext.Current.CancellationToken);
        
        script = "GET pp";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
         ESET pp 'hello world'                      
         EGET pp
         """;

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
         BEGIN 
          EEXTEND pp 500
          ROLLBACK
         END
         """;
        
        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        await Task.Delay(1000, TestContext.Current.CancellationToken);
        
        script = "EGET pp";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
}