using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestKeyValueScriptFuncCalls : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptFuncCalls(ITestOutputHelper outputHelper)
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
    public async Task TestIsTypeFuncCalls([CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        LET x = 1
        RETURN is_long(x)
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));

        script = """
         LET x = 1.2
         RETURN is_long(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 1.2
         RETURN is_float(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 1
         RETURN is_float(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = null
         RETURN is_null(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = false
         RETURN is_null(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = ''
         RETURN is_null(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = ''
         RETURN is_string(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET y = 'hello'
         LET x = y
         RETURN is_string(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = false
         RETURN is_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = null
         RETURN is_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));

        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestCastTypeFuncCalls([CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
        LET x = 1
        RETURN to_long(x)
        """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("1", Encoding.UTF8.GetString(resp.Value ?? []));

        script = """
         LET x = 1.2
         RETURN to_long(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("1", Encoding.UTF8.GetString(resp.Value ?? []));   
        
        script = """
         LET x = 1
         RETURN to_float(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("1", Encoding.UTF8.GetString(resp.Value ?? []));  
        
        script = """
         LET x = 1.2
         RETURN to_float(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("1.2", Encoding.UTF8.GetString(resp.Value ?? []));  
        
        script = """
         LET x = false
         RETURN to_float(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("0", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = true
         RETURN to_float(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("1", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = false
         RETURN to_long(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("0", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = true
         RETURN to_long(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("1", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = true
         RETURN to_string(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 1.0
         RETURN to_string(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("1", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 100
         RETURN to_string(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 'hello'
         RETURN to_string(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = null
         RETURN to_string(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("", Encoding.UTF8.GetString(resp.Value ?? []));

        await LeaveCluster(node1, node2, node3);
        
        script = """
         LET x = false
         RETURN to_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = true
         RETURN to_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 0
         RETURN to_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 1
         RETURN to_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 0.0
         RETURN to_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 2.0
         RETURN to_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 'false'
         RETURN to_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         LET x = 'true'
         RETURN to_bool(x)
         """;

        resp = await kahuna2.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
        await LeaveCluster(node1, node2, node3);
    }

    [Theory, CombinatorialData]
    public async Task TestRevFuncCalls([CombinatorialValues("memory")] string storage,[CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
         await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
         SET pp 'hello'
         LET x = GET pp
         RETURN revision(x)
         """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("0", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         ESET pp 'hello'
         LET x = EGET pp
         RETURN revision(x)
         """;

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("0", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         SET ppb 'hello 1'
         SET ppb 'hello 2'
         SET ppb 'hello 3'
         LET x = GET ppb
         RETURN revision(x)
         """;

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(2, resp.Revision);
        Assert.Equal("2", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         ESET ppb 'hello 1'
         ESET ppb 'hello 2'
         ESET ppb 'hello 3'
         LET x = EGET ppb
         RETURN revision(x)
         """;

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(2, resp.Revision);
        Assert.Equal("2", Encoding.UTF8.GetString(resp.Value ?? []));
        
        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestLengthFuncCalls([CombinatorialValues("memory")] string storage,[CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
         await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = """
         SET pp 'hello'
         LET x = GET pp
         RETURN length(x)
         """;

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("5", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         ESET pp 'hello'
         LET x = EGET pp
         RETURN length(x)
         """;

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("5", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = """
         ESET pp ''
         LET x = EGET pp
         RETURN length(x)
         """;

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("0", Encoding.UTF8.GetString(resp.Value ?? []));     
        
        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestMathFuncCalls([CombinatorialValues("memory")] string storage,[CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
         await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = "RETURN abs(100)";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN abs(-100)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN pow(2, 3)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("8", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN pow(-2, 3)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("-8", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN round(2.551, 1)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("2.6", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN round(100.72, 0)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("101", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN max(2.551, 1)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("2.551", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN max(-100, 2.551)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("2.551", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN max(0, 100)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN min(2.551, 1)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("1", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN min(-100, 2.551)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("-100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN min(0, 100)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("0", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN ceil(-100)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("-100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN ceil(100)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN ceil(-100.25)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("-100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN ceil(100.75)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("101", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN floor(-100)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("-100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN floor(100)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN floor(-100.25)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("-101", Encoding.UTF8.GetString(resp.Value ?? []));
        
        script = "RETURN floor(100.25)";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("100", Encoding.UTF8.GetString(resp.Value ?? []));
        
        await LeaveCluster(node1, node2, node3);
    }
}