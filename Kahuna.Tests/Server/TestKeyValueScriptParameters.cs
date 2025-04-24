
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestKeyValueScriptParameters : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptParameters(ITestOutputHelper outputHelper)
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

    [Theory, CombinatorialData]
    public async Task TestBasicSetWithParameter([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = "SET @pp_param 'hello world'";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@pp_param", Value = "pp" }]);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "GET @pp_param";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@pp_param", Value = "pp" }]);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = """
         set @leader_param "node-A" cmprev 0
         if not set then
           throw "election failed"
         end
         return true
         """;

        resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@leader_param", Value = "election/leader1" }]);
        Assert.Equal(KeyValueResponseType.Errored, resp.Type);
        Assert.Equal("election failed at line 3", resp.Reason);
        
        script = """
         set @leader_param "node-A" nx
         set @leader_param "node-A" cmprev 0
         if not set then
           throw "election failed"
         end
         return true
         """;

        resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@leader_param", Value = "election/leader1" }]);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));     
           
        script = """
         set @leader_param "node-A"
         let current_leader = get @leader_param
         if rev(current_leader) == 0 then  
            set @leader_param "node-A"
         else
            throw "election failed"
         end  
         """;

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@leader_param", Value = "election/leader2" }]);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);        
        
        await LeaveCluster(node1, node2, node3);
    }
}