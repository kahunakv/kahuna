
using Kommander;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

[Collection("ClusterTests")]
public class TestKeyValueScriptOperators : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptOperators(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }    
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameConditionalOrScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
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
    public async Task TestSetGetSameConditionalAndScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
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

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world 3"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameConditionalParenthesesScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            LET s1 = 'hello world'
            LET s2 = 'hello world 2'
        
            LET s = 'hello world 2'
            IF (s = s1 || s = s2) THEN
                SET pp s1 EX 1000
            ELSE
                SET pp 'hello world 3' EX 1000
            END
        
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = """
              LET s1 = 'hello world'
              LET s2 = 'hello world 2'
          
              LET s = 'hello world 2'
              IF (s = s1 || s = s2) THEN
                  ESET pp s1 EX 1000
              ELSE
                  ESET pp 'hello world 3' EX 1000
              END
          
              EGET pp
              """;

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
    public async Task TestSetGetConditionalNotScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            LET s = false        
            IF NOT s THEN
                SET pp s EX 1000
            ELSE
                SET pp 'hello world 3' EX 1000
            END                               
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
              LET s = false        
              IF NOT s THEN
                  ESET pp s EX 1000
              ELSE
                  ESET pp 'hello world 3' EX 1000
              END                               
              EGET pp
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetAddOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 100 + 50                                               
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("150"u8.ToArray(), resp.Value);
        
            script = """
              ESET pp 100 + 50                                               
              EGET pp
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("150"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestInvalidAddOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 + 'hello'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Equal("Invalid operands: LongType + StringType at line 1", resp.Reason);        
        
            script = "RETURN 100 + null";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Equal("Invalid operands: LongType + NullType at line 1", resp.Reason);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestAddOperatorConversionScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 + '50'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("150", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN 100.5 + '50'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("150.5", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50' + 100";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("150", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50.5' + 100";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("150.5", Encoding.UTF8.GetString(resp.Value ?? []));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSubstractOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 100 - 25                                               
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("75"u8.ToArray(), resp.Value);
        
            script = """
              ESET pp 100 - 25                                       
              EGET pp
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("75"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestInvalidSubOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 - 'hello'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Equal("Invalid operands: LongType - StringType at line 1", resp.Reason);        
        
            script = "RETURN 100 - null";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Equal("Invalid operands: LongType - NullType at line 1", resp.Reason);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSubOperatorConversionScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 - '50'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("50", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN 100.5 - '50'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("50.5", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50' - 100";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("-50", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50.5' - 100";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("-49.5", Encoding.UTF8.GetString(resp.Value ?? []));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetMultOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 100 * 5                                               
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("500"u8.ToArray(), resp.Value);
        
            script = """
              ESET pp 100 * 5                                       
              EGET pp
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("500"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestInvalidMultOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 * 'hello'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Equal("Invalid operands: LongType * StringType at line 1", resp.Reason);        
        
            script = "RETURN 100 * null";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Equal("Invalid operands: LongType * NullType at line 1", resp.Reason);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestMultOperatorConversionScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 * '50'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("5000", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN 100.5 * '50'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("5025", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50' * 100";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("5000", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50.5' * 100";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("5050", Encoding.UTF8.GetString(resp.Value ?? []));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetDivOperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 100 / 5                                               
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("20"u8.ToArray(), resp.Value);
        
            script = """
              ESET pp 100 / 5
              EGET pp
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("20"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestInvalidDivperatorScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 / 'hello'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Equal("Invalid operands: LongType / StringType at line 1", resp.Reason);        
        
            script = "RETURN 100 / null";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Equal("Invalid operands: LongType / NullType at line 1", resp.Reason);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestDivOperatorConversionScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 / '50'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("2", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN 100.5 / '2'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("50.25", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '100' / 50";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("2", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 100 / '0.5'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("200", Encoding.UTF8.GetString(resp.Value ?? []));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetIncrementScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 100                                               
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("100"u8.ToArray(), resp.Value);
        
            // Explicit conversion
            script = """
            LET current_pp = GET pp
            LET current_pp_num = to_int(current_pp)
            SET pp current_pp_num + 1
            GET pp
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("101"u8.ToArray(), resp.Value);
        
            // Implicit conversion
            script = """
             LET current_pp = GET pp
             SET pp current_pp + 1
             GET pp
             """;

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(2, resp.Revision);
            Assert.Equal("102"u8.ToArray(), resp.Value);
        
            script = """
              ESET pp 100
              EGET pp
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("100"u8.ToArray(), resp.Value);
        
            // Explicit conversion
            script = """
             LET current_pp = EGET pp
             LET current_pp_num = to_int(current_pp)
             ESET pp current_pp_num + 1
             EGET pp
             """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("101"u8.ToArray(), resp.Value);
        
            // Implicit conversion
            script = """
             LET current_pp = EGET pp
             ESET pp current_pp + 1
             EGET pp
             """;

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(2, resp.Revision);
            Assert.Equal("102"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetDecrementScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 100                                               
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("100"u8.ToArray(), resp.Value);
        
            // Explicit conversion
            script = """
            LET current_pp = GET pp
            LET current_pp_num = to_int(current_pp)
            SET pp current_pp_num - 1
            GET pp
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("99"u8.ToArray(), resp.Value);
        
            // Implicit conversion
            script = """
             LET current_pp = GET pp
             SET pp current_pp - 1
             GET pp
             """;

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(2, resp.Revision);
            Assert.Equal("98"u8.ToArray(), resp.Value);
        
            script = """
              ESET pp 100
              EGET pp
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("100"u8.ToArray(), resp.Value);
        
            // Explicit conversion
            script = """
             LET current_pp = EGET pp
             LET current_pp_num = to_int(current_pp)
             ESET pp current_pp_num - 1
             EGET pp
             """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("99"u8.ToArray(), resp.Value);
        
            // Implicit conversion
            script = """
             LET current_pp = EGET pp
             ESET pp current_pp - 1
             EGET pp
             """;

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(2, resp.Revision);
            Assert.Equal("98"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestEqualsOperatorNoConversionScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 = 100";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN 100.5 = 50";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 100.5 = 100.5";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 50 = 100";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 50.5 = 100";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 100 == 100";

            resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN 100.5 == 50";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 100.5 == 100.5";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 50 == 100";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 50.5 == 100";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 'hello' = 'hello'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 'not hello' = 'hello'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN true == true";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN true == false";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN null == null";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN null == false";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN null == ''";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN false == null";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 'hello' == null";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestEqualsOperatorConversionScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 = '100'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN 100.5 = '50'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 100.5 = '100.5'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50' = 100";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50.5' = 100";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 100 == '100'";

            resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN 100.5 == '50'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 100.5 == '100.5'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50' == 100";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50.5' == 100";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestEqualsOperatorBytesStringScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // bytes == string: GET returns BytesType; comparing to a string literal hits BytesType/StringType branch
            string script = """
                SET mykey 'hello'
                LET v = GET mykey
                RETURN v = 'hello'
                """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));

            script = """
                SET mykey 'hello'
                LET v = GET mykey
                RETURN v = 'world'
                """;

            resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));

            // string == bytes: reversed operand order hits StringType/BytesType branch
            script = """
                SET mykey 'hello'
                LET v = GET mykey
                RETURN 'hello' = v
                """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));

            script = """
                SET mykey 'hello'
                LET v = GET mykey
                RETURN 'world' = v
                """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));

            // multi-byte UTF-8: each Japanese character is 3 UTF-8 bytes
            const string japanese = "日本語テスト";
            script = $"""
                SET mykey '{japanese}'
                LET v = GET mykey
                RETURN v = '{japanese}'
                """;

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));

            // large string (> 256 bytes) exercises the ArrayPool fallback path
            string large = new string('x', 300);
            script = $"""
                SET mykey '{large}'
                LET v = GET mykey
                RETURN v = '{large}'
                """;

            resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));

            // large string mismatch
            string large2 = new string('y', 300);
            script = $"""
                SET mykey '{large}'
                LET v = GET mykey
                RETURN v = '{large2}'
                """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestLessThanOperatorNoConversionScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 < 100";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN 100.5 < 50";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 100.5 < 100.5";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 50 < 100";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 50.5 < 100";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));               
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestLessThanOperatorConversionScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "RETURN 100 < '100'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));        
        
            script = "RETURN '100.5' < 50";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 100.5 < '100.5'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN '50' < 100";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN 50.5 < '100'";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));               
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameConditionalNotSetScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'hello world'    
            IF NOT SET THEN 
               RETURN false
            END
            RETURN true
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
            ESET pp 'hello world'    
            IF NOT SET THEN 
               RETURN false
            END
            RETURN true
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
            SET pp 'hello world'
            LET x = GET pp    
            IF NOT SET THEN 
               RETURN false
            END
            RETURN true
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
            ESET pp 'hello world'
            LET x = GET pp    
            IF NOT SET THEN 
               RETURN false
            END
            RETURN true
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
            SET pp 'hello world'
            SET pp 'hello world 2' NX
            LET x = GET pp    
            IF NOT SET THEN 
               RETURN false
            END
            RETURN true
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
            ESET pp 'hello world'
            ESET pp 'hello world 2' NX
            LET x = GET pp    
            IF NOT SET THEN 
               RETURN false
            END
            RETURN true
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetGetSameConditionalNotFoundScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'hello world'
            LET pv = GET pp
            IF NOT FOUND THEN 
               RETURN false
            END
            RETURN true
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
            ESET pp 'hello world'
            LET pv = EGET pp
            IF NOT FOUND THEN 
               RETURN false
            END
            RETURN true
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
            SET pp 'hello world'
            LET pv = GET ppn
            IF NOT FOUND THEN 
               RETURN false
            END
            RETURN true
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
            ESET pp 'hello world'
            LET pv = EGET ppn
            IF NOT FOUND THEN 
               RETURN false
            END
            RETURN true
            """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("false", Encoding.UTF8.GetString(resp.Value ?? []));
                      
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestSetGetSameArrayIndexScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            LET x = 1..10
            RETURN x[0]
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("1", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
             LET x = 1..10
             RETURN x[9]
             """;

            resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("10", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
             LET x = 1..10
             LET total = 0
             FOR i IN x DO
                LET total = total + x[i - 1]
             END
             RETURN total
            """;

            resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("55", Encoding.UTF8.GetString(resp.Value ?? []));
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
