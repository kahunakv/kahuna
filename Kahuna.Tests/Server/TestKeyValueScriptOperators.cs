
using Kommander;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestKeyValueScriptOperators : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptOperators(ITestOutputHelper outputHelper)
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
    public async Task TestSetGetSameConditionalOrScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        LET s = 'hello world'        
        IF s = 'hello world' || s = 'hello world 2' THEN
            SET pp s EX 1000
        ELSE
            SET pp 'hello world 3' EX 1000
        END                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = """
          LET s = 'hello world'        
          IF s = 'hello world' || s = 'hello world 2' THEN
              ESET pp s EX 1000
          ELSE
              ESET pp 'hello world 3' EX 1000
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
    public async Task TestSetGetSameConditionalAndScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        LET s = 'hello world'        
        IF s = 'hello world' && s = 'hello world 2' THEN
            SET pp s EX 1000
        ELSE
            SET pp 'hello world 3' EX 1000
        END                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world 3"u8.ToArray(), resp.Value);
        
        script = """
          LET s = 'hello world'        
          IF s = 'hello world' && s = 'hello world 2' THEN
              ESET pp s EX 1000
          ELSE
              ESET pp 'hello world 3' EX 1000
          END                               
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world 3"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetAddOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 100 + 50                                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("150"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 100 + 50                                               
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("150"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSubstractOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 100 - 25                                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("75"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 100 - 25                                       
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("75"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetMultOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 100 * 5                                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("500"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 100 * 5                                       
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("500"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetDivOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 100 / 5                                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("20"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 100 / 5
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("20"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetIncrementScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        SET pp 100                                               
        GET pp
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("100"u8.ToArray(), resp.Value);
        
        script = """
        LET current_pp = GET pp
        SET pp current_pp + 1                             
        GET pp
        """;

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("101"u8.ToArray(), resp.Value);
        
        script = """
          ESET pp 100
          EGET pp
          """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("100"u8.ToArray(), resp.Value);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
}