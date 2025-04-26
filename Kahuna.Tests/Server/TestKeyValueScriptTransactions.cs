
using Kommander;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestKeyValueScriptTransactions : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptTransactions(ITestOutputHelper outputHelper)
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
    public async Task TestExecuteTxCommitRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = "BEGIN SET pp 'hello world' COMMIT END";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "GET pp";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);

        script = "BEGIN ESET pp 'hello world' COMMIT END";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "EGET pp";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "BEGIN SET pp 'hello world 2' ROLLBACK END";

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "GET pp";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "BEGIN SET pp 'hello world 2' END";

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "GET pp";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        // Ephemeral
        script = "BEGIN ESET pp 'hello world 2' ROLLBACK END";

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "EGET pp";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "BEGIN ESET pp 'hello world 2' END";

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "EGET pp";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);

        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestExecuteTxMultiCommitRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        // Persistent tests
        string script = "BEGIN SET pp1 'hello world' SET pp2 'hello world' COMMIT END";

        KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "GET pp1";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "GET pp2";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "BEGIN SET pp1 'hello world 2' SET pp2 'hello world 2' ROLLBACK END";

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "GET pp1";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "GET pp2";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "BEGIN SET pp1 'hello world 2' SET pp2 'hello world 2' END";

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "GET pp1";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "GET pp2";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        // Ephemeral
        script = "BEGIN ESET pp1 'hello world' ESET pp2 'hello world' COMMIT END";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        Assert.Equal(0, resp.Revision);
        
        script = "EGET pp1";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "EGET pp2";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "BEGIN ESET pp1 'hello world 2' ESET pp2 'hello world 2' END";

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "EGET pp1";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "EGET pp2";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "BEGIN ESET pp1 'hello world 2' ESET pp1 'hello world 2' ROLLBACK END";

        resp = await kahuna3.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
        script = "EGET pp1";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        script = "EGET pp2";

        resp = await kahuna2.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(0, resp.Revision);
        Assert.Equal("hello world"u8.ToArray(), resp.Value);

        await LeaveCluster(node1, node2, node3);
    }

    [Theory, CombinatorialData]
    public async Task TestExecuteInteractiveTxCommitNoChanges([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        KeyValueTransactionOptions options = new()
        {
            UniqueId = Guid.NewGuid().ToString(),
            Locking = KeyValueTransactionLocking.Pessimistic
        };

        // Persistent tests        
        (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
        
        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.True(transactionId.L > 0);
        
        KeyValueResponseType response = await kahuna2.LocateAndCommitTransaction(options.UniqueId, transactionId, [], [], TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Committed, response);
        
        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestExecuteInteractiveTxRollbackNoChanges([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        KeyValueTransactionOptions options = new()
        {
            UniqueId = Guid.NewGuid().ToString(),
            Locking = KeyValueTransactionLocking.Pessimistic
        };

        // Persistent tests        
        (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
        
        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.True(transactionId.L > 0);
        
        KeyValueResponseType response = await kahuna2.LocateAndRollbackTransaction(options.UniqueId, transactionId, [], [], TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.RolledBack, response);
        
        await LeaveCluster(node1, node2, node3);
    }
}