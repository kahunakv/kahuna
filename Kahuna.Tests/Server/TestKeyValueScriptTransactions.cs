
using Kommander;
using System.Text;
using Kahuna.Server.KeyValues;
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
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Theory, CombinatorialData]
    public async Task TestExecuteTxCommitRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestExecuteTxMultiCommitRollbackScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestExecuteInteractiveTxCommitNoChanges([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            KeyValueTransactionOptions options = new()
            {
                UniqueId = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests        
            (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);
        
            KeyValueResponseType response = await kahuna2.LocateAndCommitTransaction(options.UniqueId, transactionId, [], [], [], TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Committed, response);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestExecuteInteractiveTxRollbackNoChanges([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestExecuteInteractiveTxCommitSingleChange([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            KeyValueTransactionOptions options = new()
            {
                UniqueId = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests        
            (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);

            (KeyValueResponseType type, string key, KeyValueDurability durability) resp = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, "pp1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp1", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);
        
            (type, long revision, HLCTimestamp modifiedTime) = await kahuna3.LocateAndTrySetKeyValue(transactionId, "pp1", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);
        
            KeyValueResponseType response = await kahuna1.LocateAndCommitTransaction(
                options.UniqueId, 
                transactionId, 
                [new() { Key = "pp1", Durability = KeyValueDurability.Persistent }], 
                [new() { Key = "pp1", Durability = KeyValueDurability.Persistent }], 
                [],
                TestContext.Current.CancellationToken
            );
            Assert.Equal(KeyValueResponseType.Committed, response);
        
            (type, ReadOnlyKeyValueEntry? context) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp1", -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.NotNull(context);
            Assert.NotNull(context.Value);
            Assert.Equal("hello world", Encoding.UTF8.GetString(context.Value));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestExecuteInteractiveTxCommitMultiChange([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            KeyValueTransactionOptions options = new()
            {
                UniqueId = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests        
            (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);
        
            (KeyValueResponseType type, string key, KeyValueDurability durability) resp = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, "pp1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp1", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);
        
            (type, long revision, HLCTimestamp modifiedTime) = await kahuna3.LocateAndTrySetKeyValue(transactionId, "pp1", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);
        
            resp = await kahuna1.LocateAndTryAcquireExclusiveLock(transactionId, "pp2", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp2", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);
        
            (type, revision, modifiedTime) = await kahuna2.LocateAndTrySetKeyValue(transactionId, "pp2", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);
        
            KeyValueResponseType response = await kahuna3.LocateAndCommitTransaction(
                options.UniqueId, 
                transactionId, 
                [new() { Key = "pp1", Durability = KeyValueDurability.Persistent }, new() { Key = "pp2", Durability = KeyValueDurability.Persistent }], 
                [new() { Key = "pp1", Durability = KeyValueDurability.Persistent }, new() { Key = "pp2", Durability = KeyValueDurability.Persistent }], 
                [],
                TestContext.Current.CancellationToken
            );
        
            Assert.Equal(KeyValueResponseType.Committed, response);
        
            (type, ReadOnlyKeyValueEntry? context) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp1", -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.NotNull(context);
            Assert.NotNull(context.Value);
            Assert.Equal("hello world", Encoding.UTF8.GetString(context.Value));
        
            (type, context) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp2", -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.NotNull(context);
            Assert.NotNull(context.Value);
            Assert.Equal("hello world", Encoding.UTF8.GetString(context.Value));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestExecuteInteractiveTxRollbackSingleChange([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            KeyValueTransactionOptions options = new()
            {
                UniqueId = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests        
            (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);

            (KeyValueResponseType type, string key, KeyValueDurability durability) resp = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, "pp1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp1", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);
        
            (type, long revision, HLCTimestamp modifiedTime) = await kahuna3.LocateAndTrySetKeyValue(transactionId, "pp1", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);
        
            KeyValueResponseType response = await kahuna1.LocateAndRollbackTransaction(
                options.UniqueId, 
                transactionId, 
                [new() { Key = "pp1", Durability = KeyValueDurability.Persistent }], 
                [new() { Key = "pp1", Durability = KeyValueDurability.Persistent }], 
                TestContext.Current.CancellationToken
            );
        
            Assert.Equal(KeyValueResponseType.RolledBack, response);
        
            (type, ReadOnlyKeyValueEntry? _) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp1", -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, type);        
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestExecuteInteractiveTxRollbackMultiChange([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            KeyValueTransactionOptions options = new()
            {
                UniqueId = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests        
            (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);
        
            (KeyValueResponseType type, string key, KeyValueDurability durability) resp = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, "pp1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp1", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);
        
            (type, long revision, HLCTimestamp modifiedTime) = await kahuna3.LocateAndTrySetKeyValue(transactionId, "pp1", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);
        
            resp = await kahuna1.LocateAndTryAcquireExclusiveLock(transactionId, "pp2", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp2", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);
        
            (type, revision, modifiedTime) = await kahuna2.LocateAndTrySetKeyValue(transactionId, "pp2", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);
        
            KeyValueResponseType response = await kahuna3.LocateAndRollbackTransaction(
                options.UniqueId, 
                transactionId, 
                [new() { Key = "pp1", Durability = KeyValueDurability.Persistent }, new() { Key = "pp2", Durability = KeyValueDurability.Persistent }], 
                [new() { Key = "pp1", Durability = KeyValueDurability.Persistent }, new() { Key = "pp2", Durability = KeyValueDurability.Persistent }], 
                TestContext.Current.CancellationToken
            );
        
            Assert.Equal(KeyValueResponseType.RolledBack, response);
        
            (type, ReadOnlyKeyValueEntry? _) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp1", -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, type);        
        
            (type, _) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp2", -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, type);

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Drives the 2PC per-key API manually: Prepare then Rollback for a single key.
    /// This is the path that was broken by the rollback routing typo fixed in d852696 —
    /// LocateAndTryRollbackMutations on the local/leader branch was calling TryCommitMutations
    /// instead of TryRollbackMutations, which this test would have caught.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestPrepareAndRollbackSingleMutation([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(
                new() { UniqueId = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic },
                TestContext.Current.CancellationToken
            );
            Assert.Equal(KeyValueResponseType.Set, type);

            (type, _, _) = await kahuna1.LocateAndTryAcquireExclusiveLock(transactionId, "prb1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Locked, type);

            (type, _, _) = await kahuna2.LocateAndTrySetKeyValue(transactionId, "prb1", "original"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, type);

            HLCTimestamp commitId = node1.HybridLogicalClock.SendOrLocalEvent(node1.GetLocalNodeId());
            (KeyValueResponseType prepType, HLCTimestamp ticketId, _, _) = await kahuna3.LocateAndTryPrepareMutations(
                transactionId, commitId, "prb1", KeyValueDurability.Persistent, TestContext.Current.CancellationToken
            );
            Assert.Equal(KeyValueResponseType.Prepared, prepType);

            // This directly exercises LocateAndTryRollbackMutations — the method fixed in d852696.
            (KeyValueResponseType rollbackType, _) = await kahuna1.LocateAndTryRollbackMutations(
                transactionId, "prb1", ticketId, KeyValueDurability.Persistent, TestContext.Current.CancellationToken
            );
            Assert.Equal(KeyValueResponseType.RolledBack, rollbackType);

            (type, ReadOnlyKeyValueEntry? entry) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "prb1", -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, type);
            Assert.Null(entry?.Value);

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Drives the 2PC per-key API manually: Prepare then Commit for a single key.
    /// Ensures LocateAndTryCommitMutations (also untested before) reaches the correct handler.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestPrepareAndCommitSingleMutation([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(
                new() { UniqueId = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic },
                TestContext.Current.CancellationToken
            );
            Assert.Equal(KeyValueResponseType.Set, type);

            (type, _, _) = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, "pcb1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Locked, type);

            (type, _, _) = await kahuna3.LocateAndTrySetKeyValue(transactionId, "pcb1", "committed-value"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, type);

            HLCTimestamp commitId = node1.HybridLogicalClock.SendOrLocalEvent(node1.GetLocalNodeId());
            (KeyValueResponseType prepType, HLCTimestamp ticketId, _, _) = await kahuna1.LocateAndTryPrepareMutations(
                transactionId, commitId, "pcb1", KeyValueDurability.Persistent, TestContext.Current.CancellationToken
            );
            Assert.Equal(KeyValueResponseType.Prepared, prepType);

            (KeyValueResponseType commitType, _) = await kahuna2.LocateAndTryCommitMutations(
                transactionId, "pcb1", ticketId, KeyValueDurability.Persistent, TestContext.Current.CancellationToken
            );
            Assert.Equal(KeyValueResponseType.Committed, commitType);

            (type, ReadOnlyKeyValueEntry? entry) = await kahuna3.LocateAndTryGetValue(HLCTimestamp.Zero, "pcb1", -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.NotNull(entry?.Value);
            Assert.Equal("committed-value", Encoding.UTF8.GetString(entry!.Value!));

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Drives the 2PC per-key API manually: PrepareMany then RollbackMany for two keys.
    /// Covers LocateAndTryRollbackManyMutations, which was also unreachable from existing tests.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestPrepareAndRollbackManyMutations([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(
                new() { UniqueId = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic },
                TestContext.Current.CancellationToken
            );
            Assert.Equal(KeyValueResponseType.Set, type);

            foreach (string key in new[] { "prbm1", "prbm2" })
            {
                (type, _, _) = await kahuna1.LocateAndTryAcquireExclusiveLock(transactionId, key, 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Locked, type);

                (type, _, _) = await kahuna2.LocateAndTrySetKeyValue(transactionId, key, "will-be-rolled-back"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, type);
            }

            HLCTimestamp commitId = node1.HybridLogicalClock.SendOrLocalEvent(node1.GetLocalNodeId());
            List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> prepResponses = await kahuna3.LocateAndTryPrepareManyMutations(
                transactionId, commitId,
                [("prbm1", KeyValueDurability.Persistent), ("prbm2", KeyValueDurability.Persistent)],
                TestContext.Current.CancellationToken
            );
            Assert.All(prepResponses, r => Assert.Equal(KeyValueResponseType.Prepared, r.Item1));

            List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> toRollback =
                prepResponses.Select(r => (r.Item3, r.Item2, r.Item4)).ToList();

            List<(KeyValueResponseType, string, long, KeyValueDurability)> rollbackResponses =
                await kahuna1.LocateAndTryRollbackManyMutations(transactionId, toRollback, TestContext.Current.CancellationToken);
            Assert.All(rollbackResponses, r => Assert.Equal(KeyValueResponseType.RolledBack, r.Item1));

            foreach (string key in new[] { "prbm1", "prbm2" })
            {
                (type, ReadOnlyKeyValueEntry? entry) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.DoesNotExist, type);
                Assert.Null(entry?.Value);
            }

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Drives the 2PC per-key API manually: PrepareMany then CommitMany for two keys.
    /// Covers LocateAndTryCommitManyMutations, which was also unreachable from existing tests.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestPrepareAndCommitManyMutations([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            (KeyValueResponseType type, HLCTimestamp transactionId) = await kahuna1.LocateAndStartTransaction(
                new() { UniqueId = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic },
                TestContext.Current.CancellationToken
            );
            Assert.Equal(KeyValueResponseType.Set, type);

            foreach (string key in new[] { "pcbm1", "pcbm2" })
            {
                (type, _, _) = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, key, 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Locked, type);

                (type, _, _) = await kahuna3.LocateAndTrySetKeyValue(transactionId, key, "committed-many"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, type);
            }

            HLCTimestamp commitId = node1.HybridLogicalClock.SendOrLocalEvent(node1.GetLocalNodeId());
            List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> prepResponses = await kahuna1.LocateAndTryPrepareManyMutations(
                transactionId, commitId,
                [("pcbm1", KeyValueDurability.Persistent), ("pcbm2", KeyValueDurability.Persistent)],
                TestContext.Current.CancellationToken
            );
            Assert.All(prepResponses, r => Assert.Equal(KeyValueResponseType.Prepared, r.Item1));

            List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> toCommit =
                prepResponses.Select(r => (r.Item3, r.Item2, r.Item4)).ToList();

            List<(KeyValueResponseType, string, long, KeyValueDurability)> commitResponses =
                await kahuna2.LocateAndTryCommitManyMutations(transactionId, toCommit, TestContext.Current.CancellationToken);
            Assert.All(commitResponses, r => Assert.Equal(KeyValueResponseType.Committed, r.Item1));

            foreach (string key in new[] { "pcbm1", "pcbm2" })
            {
                (type, ReadOnlyKeyValueEntry? entry) = await kahuna3.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.NotNull(entry?.Value);
                Assert.Equal("committed-many", Encoding.UTF8.GetString(entry!.Value!));
            }

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
