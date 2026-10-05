
using System.Diagnostics;
using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

public class TestKeyValueScriptControlStructures : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptControlStructures(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string GetRandomKey()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicIfScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'other world'                
            IF true THEN
                SET pp 'hello world' EX 1000        
            END                               
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
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

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicIfScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            SET pp 'other world'                
            IF false THEN
                SET pp 'hello world' EX 1000        
            END                               
            GET pp
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
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

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(0, resp.Revision);
            Assert.Equal("other world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicIfElseScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
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

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicIfElseScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
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

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("big world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestNestedIfElseScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
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

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestNestedIfElseScript2([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
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

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(1, resp.Revision);
            Assert.Equal("big world"u8.ToArray(), resp.Value);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestLetReturnScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            LET my_var = 'hello world'                               
            RETURN my_var
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("hello world"u8.ToArray(), resp.Value);
        
            script = """
              LET my_var = 'hello world'                               
              LET my_var = 'another world'
              RETURN my_var
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("another world"u8.ToArray(), resp.Value);

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestLetReturnComplexScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            LET my_var = (100 + 50) * 2 - 1                               
            RETURN my_var
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("299", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
              LET my_var = (100 + 50) * 2 - 1                               
              LET my_var = my_var + (100 + 50) * 2 - 1
              RETURN my_var
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("598", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
              LET my_var = true          
              RETURN my_var
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("true", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
              LET my_var = null          
              RETURN my_var
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("", Encoding.UTF8.GetString(resp.Value ?? []));
        
             script = """
              LET my_var = 10.5          
              RETURN my_var
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("10.5", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = """
              LET my_var = 10.5          
              LET my_var2 = 0.5
              RETURN my_var + my_var2
              """;

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("11", Encoding.UTF8.GetString(resp.Value ?? []));
        
            script = "RETURN (100 + 50) * 2 - 1";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("299", Encoding.UTF8.GetString(resp.Value ?? []));

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestThrowScript([CombinatorialValues("memory")] string storage,[CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = "throw 'my exception'";

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);        
            Assert.Equal("my exception at line 1", resp.Reason);

            script = "throw 100";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);        
            Assert.Equal("100 at line 1", resp.Reason);
        
            script = "throw false";

            resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);        
            Assert.Equal("false at line 1", resp.Reason);
        
            script = "throw null";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Errored, resp.Type);        
            Assert.Equal("(null) at line 1", resp.Reason);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestSleepScript([CombinatorialValues("memory")] string storage,[CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);
        
        try
        {
            string script = "sleep 1050 return true";
        
            Stopwatch stopwatch = Stopwatch.StartNew();

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);        
            Assert.True(stopwatch.ElapsedMilliseconds >= 1000);

            stopwatch.Restart();

            script = "sleep 3050 return true";

            resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);        
            Assert.True(stopwatch.ElapsedMilliseconds >= 3000);
        
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicForScript([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Persistent tests
            string script = """
            let total = 0
            for x in 1..10 do
                let total = total + x
            end
            return total
            """;

            KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("55", Encoding.UTF8.GetString(resp.Value ?? []));
        
            // A start above the end is an empty range, so the loop body never runs and the total stays zero.
            // This asserted 10 while the right operand was read as a count: "10..1" then meant one element
            // starting at ten.
            script = """
             let total = 0
             for x in 10..1 do
                 let total = total + x
             end
             return total
             """;

            resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("0", Encoding.UTF8.GetString(resp.Value ?? []));

            // Both bounds are included, so this sums 0 through 9. It asserted 36, the sum of 0 through 8,
            // because a count of nine elements starting at zero stopped one short of the end bound.
            script = """
             let r_start = 0
             let r_end = 9
             let total = 0
             for x in r_start..r_end do
                 let total = total + x
             end
             return total
             """;

            resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(-1, resp.Revision);
            Assert.Equal("45", Encoding.UTF8.GetString(resp.Value ?? []));

        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestSwitchPicksTheMatchingCase([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string script = """
            SWITCH @x
              CASE 1 THEN
                RETURN 'one'
              CASE 2, 3 THEN
                RETURN 'two or three'
              ELSE
                RETURN 'other'
            END
            """;

            // A parameter arrives as a string, and '==' reads a numeric string as a number, so "2" and "2.0" both
            // match CASE 2.
            (string Input, string Expected)[] expectations =
            [
                ("1", "one"),
                ("2", "two or three"),
                ("3", "two or three"),
                ("4", "other"),
                ("2.0", "two or three")
            ];

            IKahuna[] nodes = [kahuna1, kahuna2, kahuna3];

            for (int i = 0; i < expectations.Length; i++)
            {
                (string input, string expected) = expectations[i];

                KeyValueTransactionResult resp = await RetryOnMustRetry(
                    nodes[i % nodes.Length], Encoding.UTF8.GetBytes(script), null, [new() { Key = "@x", Value = input }]);

                Assert.True(resp.Type == KeyValueResponseType.Get, $"{input}: {resp.Type} {resp.Reason}");
                Assert.Equal(expected, Encoding.UTF8.GetString(resp.Value ?? []));
            }

            const string stringScript = """
            SWITCH @x
              CASE 'abc' THEN RETURN 'letters'
              CASE 'ABC' THEN RETURN 'capitals'
              ELSE RETURN 'other'
            END
            """;

            (string Input, string Expected)[] stringExpectations = [("abc", "letters"), ("ABC", "capitals"), ("aBc", "other")];

            foreach ((string input, string expected) in stringExpectations)
            {
                KeyValueTransactionResult resp = await RetryOnMustRetry(
                    kahuna1, Encoding.UTF8.GetBytes(stringScript), null, [new() { Key = "@x", Value = input }]);

                Assert.True(resp.Type == KeyValueResponseType.Get, $"{input}: {resp.Type} {resp.Reason}");
                Assert.Equal(expected, Encoding.UTF8.GetString(resp.Value ?? []));
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestSwitchWritesOnlyFromTheMatchingCase([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Auto-commit, and both locking modes of an explicit transaction: the keys of every CASE are known
            // before a pessimistic transaction starts, while an optimistic one meets them as it runs.
            string[] wrappers =
            [
                "{0}",
                "BEGIN (locking=pessimistic)\n{0}\nCOMMIT\nEND",
                "BEGIN (locking=optimistic)\n{0}\nCOMMIT\nEND"
            ];

            foreach (string wrapper in wrappers)
            {
                string persistent = "switch_p_" + GetRandomKey();
                string ephemeral = "switch_e_" + GetRandomKey();

                string body = $"""
                SET {persistent} 'start'
                ESET {ephemeral} 'start'
                SWITCH 'b'
                  CASE 'a' THEN
                    SET {persistent} 'from a'
                    ESET {ephemeral} 'from a'
                  CASE 'b' THEN
                    SET {persistent} 'from b'
                    ESET {ephemeral} 'from b'
                  ELSE
                    SET {persistent} 'from else'
                    ESET {ephemeral} 'from else'
                END
                """;

                string script = string.Format(wrapper, body);

                KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
                Assert.True(resp.Type == KeyValueResponseType.Set, $"{wrapper}: {resp.Type} {resp.Reason}");

                resp = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes($"GET {persistent}"), null, null);
                Assert.Equal(KeyValueResponseType.Get, resp.Type);
                Assert.Equal("from b"u8.ToArray(), resp.Value);

                resp = await RetryOnMustRetry(kahuna3, Encoding.UTF8.GetBytes($"EGET {ephemeral}"), null, null);
                Assert.Equal(KeyValueResponseType.Get, resp.Type);
                Assert.Equal("from b"u8.ToArray(), resp.Value);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestSwitchSemantics([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            (string Name, string Script, string Expected)[] cases =
            [
                // With no match and no ELSE, nothing runs.
                ("no match, no else", """
                 LET r = 'unchanged'
                 SWITCH 5
                   CASE 1 THEN LET r = 'one'
                 END
                 RETURN r
                 """, "unchanged"),

                // The first matching CASE runs and control never falls through to a later one.
                ("first match wins", """
                 LET r = 0
                 SWITCH 1
                   CASE 1 THEN LET r = r + 1
                   CASE 1 THEN LET r = r + 10
                   ELSE LET r = r + 100
                 END
                 RETURN r
                 """, "1"),

                // Evaluation stops at the match: the division by zero after it, in the same CASE or a later one,
                // is never evaluated.
                ("later values are not evaluated", """
                 LET r = ''
                 SWITCH 1
                   CASE 1, 1 / 0 THEN LET r = 'hit'
                   CASE 1 / 0 THEN LET r = 'never'
                 END
                 RETURN r
                 """, "hit"),

                ("null matches only null", """
                 SWITCH null
                   CASE 0 THEN RETURN 'zero'
                   CASE '' THEN RETURN 'empty'
                   CASE null THEN RETURN 'null'
                 END
                 """, "null"),

                ("expressions as subject and values", """
                 LET n = 3
                 SWITCH n * 2
                   CASE n + 1 THEN RETURN 'n + 1'
                   CASE n + n THEN RETURN 'n + n'
                 END
                 """, "n + n"),

                // A SWITCH nests inside a loop and inside another SWITCH, and LET inside a CASE is visible after it.
                ("nested in a loop", """
                 LET small = 0
                 LET even = 0
                 LET other = 0
                 FOR x IN 1..6 DO
                   SWITCH x
                     CASE 1, 2 THEN
                       LET small = small + 1
                     ELSE
                       SWITCH x
                         CASE 4, 6 THEN LET even = even + 1
                         ELSE LET other = other + 1
                       END
                   END
                 END
                 RETURN small * 100 + even * 10 + other
                 """, "222"),
            ];

            foreach ((string name, string script, string expected) in cases)
            {
                KeyValueTransactionResult resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);

                Assert.True(resp.Type == KeyValueResponseType.Get, $"{name}: {resp.Type} {resp.Reason}");
                Assert.Equal(expected, Encoding.UTF8.GetString(resp.Value ?? []));
            }

            // A CASE value that '==' cannot compare with the subject is the same script error '==' reports.
            KeyValueTransactionResult error = await RetryOnMustRetry(kahuna2, Encoding.UTF8.GetBytes("""
                SWITCH 1
                  CASE 'abc' THEN RETURN 1
                END
                """), null, null);

            Assert.Equal(KeyValueResponseType.Errored, error.Type);
            Assert.Contains("Invalid operands: LongType == StringType", error.Reason, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A pessimistic transaction takes its locks before the script runs, so it cannot know which CASE will match:
    /// the keys of every CASE body and of the ELSE body must be in the lock set, and the CASE values, which are
    /// expressions, add nothing.
    /// </summary>
    [Fact]
    public void TestSwitchLocksTheKeysOfEveryBranch()
    {
        ScriptParserProcessor parser = new(new KahunaConfiguration(), kahunaLogger);

        NodeAst ast = parser.Parse("""
            SWITCH @x
              CASE 1 THEN SET first 'a'
              CASE 2, 3 THEN
                ESET second 'b'
                GET BY BUCKET third
              ELSE
                DELETE fourth
            END
            """);

        HashSet<string> ephemeral = [];
        HashSet<string> persistent = [];
        HashSet<string> ephemeralPrefixes = [];
        HashSet<string> persistentPrefixes = [];

        KeyValueLockHelper.GetLocksToAcquire(new ScriptTransactionContext(), ast, ephemeral, persistent, ephemeralPrefixes, persistentPrefixes);

        Assert.Equal(new[] { "first", "fourth" }, persistent.Order(StringComparer.Ordinal));
        Assert.Equal("second", Assert.Single(ephemeral));
        Assert.Equal("third", Assert.Single(persistentPrefixes));
        Assert.Empty(ephemeralPrefixes);
    }
}
