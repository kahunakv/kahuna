
using Kommander;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

[Collection("ClusterTests")]
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

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);
        
            script = "GET pp";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);

            script = "BEGIN ESET pp 'hello world' COMMIT END";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);
        
            script = "EGET pp";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "BEGIN SET pp 'hello world 2' ROLLBACK END";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "GET pp";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "BEGIN SET pp 'hello world 2' END";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "GET pp";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            // Ephemeral
            script = "BEGIN ESET pp 'hello world 2' ROLLBACK END";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "EGET pp";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "BEGIN ESET pp 'hello world 2' END";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "EGET pp";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
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

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);
        
            script = "GET pp1";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "GET pp2";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "BEGIN SET pp1 'hello world 2' SET pp2 'hello world 2' ROLLBACK END";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "GET pp1";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "GET pp2";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "BEGIN SET pp1 'hello world 2' SET pp2 'hello world 2' END";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "GET pp1";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "GET pp2";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            // Ephemeral
            script = "BEGIN ESET pp1 'hello world' ESET pp2 'hello world' COMMIT END";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);
            Assert.Equal(0, resp.Revision);
        
            script = "EGET pp1";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "EGET pp2";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "BEGIN ESET pp1 'hello world 2' ESET pp2 'hello world 2' END";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "EGET pp1";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "EGET pp2";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "BEGIN ESET pp1 'hello world 2' ESET pp1 'hello world 2' ROLLBACK END";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        
            script = "EGET pp1";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = "EGET pp2";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
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
                CoordinatorKey = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests
            (KeyValueResponseType type, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
            HLCTimestamp transactionId = txHandle.TransactionId;

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);

            (KeyValueResponseType response, _) = await kahuna2.LocateAndCommitTransaction(txHandle, TestContext.Current.CancellationToken);
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
                CoordinatorKey = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests
            (KeyValueResponseType type, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
            HLCTimestamp transactionId = txHandle.TransactionId;

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);

            KeyValueResponseType response = await kahuna2.LocateAndRollbackTransaction(txHandle, TestContext.Current.CancellationToken);
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
                CoordinatorKey = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests
            (KeyValueResponseType type, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
            HLCTimestamp transactionId = txHandle.TransactionId;

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);

            (KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp _) resp = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, "pp1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp1", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);

            (type, long revision, HLCTimestamp modifiedTime) = await kahuna3.LocateAndTrySetKeyValue(transactionId, "pp1", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);

            (KeyValueResponseType response, _) = await kahuna1.LocateAndCommitTransaction(txHandle, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Committed, response);
        
            (type, ReadOnlyKeyValueEntry? context) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
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
                CoordinatorKey = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests
            (KeyValueResponseType type, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
            HLCTimestamp transactionId = txHandle.TransactionId;

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);

            (KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp _) resp = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, "pp1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp1", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);

            (type, long revision, HLCTimestamp modifiedTime) = await kahuna3.LocateAndTrySetKeyValue(transactionId, "pp1", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);

            resp = await kahuna1.LocateAndTryAcquireExclusiveLock(transactionId, "pp2", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp2", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);

            (type, revision, modifiedTime) = await kahuna2.LocateAndTrySetKeyValue(transactionId, "pp2", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);

            (KeyValueResponseType response, _) = await kahuna3.LocateAndCommitTransaction(txHandle, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Committed, response);
        
            (type, ReadOnlyKeyValueEntry? context) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.NotNull(context);
            Assert.NotNull(context.Value);
            Assert.Equal("hello world", Encoding.UTF8.GetString(context.Value));
        
            (type, context) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp2", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
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
                CoordinatorKey = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests
            (KeyValueResponseType type, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
            HLCTimestamp transactionId = txHandle.TransactionId;

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);

            (KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp _) resp = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, "pp1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp1", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);

            (type, long revision, HLCTimestamp modifiedTime) = await kahuna3.LocateAndTrySetKeyValue(transactionId, "pp1", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);

            KeyValueResponseType response = await kahuna1.LocateAndRollbackTransaction(txHandle, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.RolledBack, response);

            (type, ReadOnlyKeyValueEntry? _) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
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
                CoordinatorKey = Guid.NewGuid().ToString(),
                Locking = KeyValueTransactionLocking.Pessimistic
            };

            // Persistent tests
            (KeyValueResponseType type, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(options, TestContext.Current.CancellationToken);
            HLCTimestamp transactionId = txHandle.TransactionId;

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(transactionId.L > 0);

            (KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp _) resp = await kahuna2.LocateAndTryAcquireExclusiveLock(transactionId, "pp1", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp1", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);

            (type, long revision, HLCTimestamp modifiedTime) = await kahuna3.LocateAndTrySetKeyValue(transactionId, "pp1", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);

            resp = await kahuna1.LocateAndTryAcquireExclusiveLock(transactionId, "pp2", 5000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Locked, resp.type);
            Assert.Equal("pp2", resp.key);
            Assert.Equal(KeyValueDurability.Persistent, resp.durability);

            (type, revision, modifiedTime) = await kahuna2.LocateAndTrySetKeyValue(transactionId, "pp2", "hello world"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken, coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.Equal(0, revision);
            Assert.True(modifiedTime > transactionId);

            KeyValueResponseType response = await kahuna3.LocateAndRollbackTransaction(txHandle, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.RolledBack, response);
        
            (type, ReadOnlyKeyValueEntry? _) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, type);        
        
            (type, _) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, "pp2", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, type);

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
