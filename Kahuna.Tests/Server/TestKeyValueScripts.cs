
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
                .SetMinimumLevel(LogLevel.Debug);
        });

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string GetRandomLockName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory, CombinatorialData]
    public async Task TestExecuteScript(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(16)] int partitions
        //[CombinatorialValues("memory")] string storage,
        //[CombinatorialValues(16)] int partitions,
        //[CombinatorialValues(LockDurability.Ephemeral)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string script = "SET pp 'hello world'";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "SET @key 'hello world'";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@key", Value = "pp" }]);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(1, resp.Revision);
        
        script = "ESET pp 'hello world'";
        
        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "ESET @key 'hello world'";

        resp = await kahuna1.TryExecuteTx(Encoding.UTF8.GetBytes(script), null, [new() { Key = "@key", Value = "pp" }]);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(1, resp.Revision);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
}