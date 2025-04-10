
using System.Diagnostics;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestKeyValueScriptControlStructures : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptControlStructures(ITestOutputHelper outputHelper)
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
    public async Task TestBasicIfScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'other world'                
        IF true THEN
            SET pp 'hello world' EX 1000        
        END                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 'other world'                
          IF true THEN
              ESET pp 'hello world' EX 1000        
          END                               
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);

        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicIfScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'other world'                
        IF false THEN
            SET pp 'hello world' EX 1000        
        END                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("other world"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 'other world'                
          IF false THEN
              ESET pp 'hello world' EX 1000        
          END                               
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("other world"u8.ToArray(), resp.Value);
        
        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicIfElseScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'other world'                
        IF true THEN
            SET pp 'hello world' EX 1000
        ELSE       
            SET pp 'big world' EX 1000
        END
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 'other world'                
          IF true THEN
              ESET pp 'hello world' EX 1000
          ELSE       
              ESET pp 'big world' EX 1000    
          END                               
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicIfElseScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'other world'                
        IF false THEN
            SET pp 'hello world' EX 1000
        ELSE
            SET pp 'big world' EX 1000
        END                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("big world"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 'other world'                
          IF false THEN
              ESET pp 'hello world' EX 1000
          ELSE
              ESET pp 'big world' EX 1000    
          END                               
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("big world"u8.ToArray(), resp.Value);
        
        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestNestedIfElseScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'other world'    
        IF true THEN
            IF true THEN
               SET pp 'hello world' EX 1000
            ELSE
               SET pp 'old world' EX 1000
            END
        ELSE       
            SET pp 'big world' EX 1000
        END
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 'other world'    
          IF true THEN
              IF true THEN
                 ESET pp 'hello world' EX 1000
              ELSE
                 ESET pp 'old world' EX 1000
              END
          ELSE       
              ESET pp 'big world' EX 1000
          END
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestNestedIfElseScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 'other world'    
        IF false THEN
            IF true THEN
               SET pp 'hello world' EX 1000
            ELSE
               SET pp 'old world' EX 1000
            END
        ELSE       
            IF false THEN
               SET pp 'hello world' EX 1000
            ELSE
               SET pp 'big world' EX 1000
            END
        END
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("big world"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 'other world'    
          IF false THEN
              IF true THEN
                 ESET pp 'hello world' EX 1000
              ELSE
                 ESET pp 'old world' EX 1000
              END
          ELSE       
              IF false THEN
                 ESET pp 'hello world' EX 1000
              ELSE
                 ESET pp 'big world' EX 1000
              END
          END
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(1, resp.Revision);
        Assert.Equal("big world"u8.ToArray(), resp.Value);
        
        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestLetReturnScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        LET my_var = 'hello world'                               
        RETURN my_var
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
          LET my_var = 'hello world'                               
          LET my_var = 'another world'
          RETURN my_var
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("another world"u8.ToArray(), resp.Value);

        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestLetReturnComplexScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        LET my_var = (100 + 50) * 2 - 1                               
        RETURN my_var
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("299", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
          LET my_var = (100 + 50) * 2 - 1                               
          LET my_var = my_var + (100 + 50) * 2 - 1
          RETURN my_var
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("598", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
          LET my_var = true          
          RETURN my_var
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
          LET my_var = null          
          RETURN my_var
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("", Encoding.UTF8.GetString(resp.Value ?? []));
        
         script = """
          LET my_var = 10.5          
          RETURN my_var
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("10.5", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
          LET my_var = 10.5          
          LET my_var2 = 0.5
          RETURN my_var + my_var2
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("11", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN (100 + 50) * 2 - 1";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("299", Encoding.UTF8.GetString(resp.Value ?? []));

        await LeaveCluster(node1, node2, node3);
    }

    [Theory, CombinatorialData]
    public async Task TestThrowScript([CombinatorialValues("memory")] string storage,[CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = "throw 'my exception'";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Errored, resp.Type);        
        Assert.Equal("my exception at line 1", resp.Reason);

        script = "throw 100";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Errored, resp.Type);        
        Assert.Equal("100 at line 1", resp.Reason);
        
        script = "throw false";

        resp = await kahuna3.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Errored, resp.Type);        
        Assert.Equal("false at line 1", resp.Reason);
        
        script = "throw null";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Errored, resp.Type);        
        Assert.Equal("(null) at line 1", resp.Reason);
        
        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSleepScript([CombinatorialValues("memory")] string storage,[CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);
        
        string script = "sleep 1050 return true";
        
        Stopwatch stopwatch = Stopwatch.StartNew();

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);        
        Assert.True(stopwatch.ElapsedMilliseconds >= 1000);

        stopwatch.Restart();

        script = "sleep 5050 return true";

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);        
        Assert.True(stopwatch.ElapsedMilliseconds >= 5000);
        
        await LeaveCluster(node1, node2, node3);
    }
}