using Kommander;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// NOT FOUND answers for the last read statement and NOT SET for the last write statement, whatever runs
/// between that statement and the guard, and a batched write prefix answers as its statements run one at a time.
/// </summary>
public class TestKeyValueScriptGuards : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptGuards(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<KeyValueTransactionResult> Run(IKahuna kahuna, string script, List<KeyValueParameter>? parameters = null) =>
        await RetryOnMustRetry(kahuna, Encoding.UTF8.GetBytes(script), null, parameters);

    private static async Task<string> RunForValue(IKahuna kahuna, string script, List<KeyValueParameter>? parameters = null)
    {
        KeyValueTransactionResult result = await Run(kahuna, script, parameters);
        Assert.True(result.Type == KeyValueResponseType.Get, $"script answered {result.Type} ({result.Reason}):\n{script}");
        return Encoding.UTF8.GetString(result.Value ?? []);
    }

    private static async Task Write(IKahuna kahuna, string script)
    {
        KeyValueTransactionResult result = await Run(kahuna, script);
        Assert.True(result.Type is KeyValueResponseType.Set or KeyValueResponseType.Deleted, $"seed answered {result.Type} ({result.Reason}): {script}");
    }

    [Theory, CombinatorialData]
    public async Task TestNotFoundAnswersForTheLastRead([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions, bool ephemeral)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string e = ephemeral ? "E" : "";

        try
        {
            await Write(kahuna1, $"{e}SET nfg_present 'v'");

            // EXISTS of a present key answers Exists, which is found.
            Assert.Equal("found", await RunForValue(kahuna2, $"""
                {e}EXISTS nfg_present
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));

            Assert.Equal("found", await RunForValue(kahuna2, $"""
                LET x = {e}EXISTS nfg_present
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));

            Assert.Equal("notfound", await RunForValue(kahuna2, $"""
                {e}EXISTS nfg_missing
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));

            // A LET between the read and the guard does not decide it, at the top level or inside an IF.
            Assert.Equal("notfound", await RunForValue(kahuna2, $"""
                {e}GET nfg_missing
                LET y = 1
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));

            Assert.Equal("notfound", await RunForValue(kahuna2, $"""
                {e}GET nfg_missing
                IF 1 == 1 THEN LET z = 2 END
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));

            Assert.Equal("found", await RunForValue(kahuna2, $"""
                LET pv = {e}GET nfg_present
                LET y = 1
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));

            // A write between the read and the guard does not decide it either.
            Assert.Equal("found", await RunForValue(kahuna2, $"""
                {e}GET nfg_present
                {e}SET nfg_other 'x'
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));

            Assert.Equal("notfound", await RunForValue(kahuna2, $"""
                {e}GET nfg_missing
                {e}SET nfg_other 'x'
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));

            // The last read wins over an earlier one.
            Assert.Equal("notfound", await RunForValue(kahuna2, $"""
                {e}GET nfg_present
                {e}GET nfg_missing
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));

            Assert.Equal("found", await RunForValue(kahuna2, $"""
                {e}GET nfg_missing
                {e}GET nfg_present
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestNotFoundAfterBucketRead([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string scope = Guid.NewGuid().ToString("N");

            List<KeyValueParameter> parameters =
            [
                new() { Key = "@bucket", Value = scope + "|nfg.bucket/" },
                new() { Key = "@member", Value = scope + "|nfg.bucket/m1" },
                new() { Key = "@other", Value = scope + "|nfg/other" }
            ];

            const string script = """
                LET b = GET BY BUCKET @bucket
                SET @other 'x'
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """;

            Assert.Equal("notfound", await RunForValue(kahuna1, script, parameters));

            KeyValueTransactionResult seed = await Run(kahuna1, "SET @member 'v'", parameters);
            Assert.Equal(KeyValueResponseType.Set, seed.Type);

            Assert.Equal("found", await RunForValue(kahuna2, script, parameters));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestGuardsWithNoEarlierStatementAreErrors([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            KeyValueTransactionResult result = await Run(kahuna1, """
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """);
            Assert.Equal(KeyValueResponseType.Errored, result.Type);
            Assert.Equal("Invalid NOT FOUND expression at line 1", result.Reason);

            // A LET or a write is not a read.
            result = await Run(kahuna1, """
                LET q = 1
                SET nfg_write 'x'
                IF NOT FOUND THEN RETURN 'notfound' END
                RETURN 'found'
                """);
            Assert.Equal(KeyValueResponseType.Errored, result.Type);
            Assert.Equal("Invalid NOT FOUND expression at line 3", result.Reason);

            result = await Run(kahuna1, """
                GET nfg_read
                IF NOT SET THEN RETURN 'notset' END
                RETURN 'set'
                """);
            Assert.Equal(KeyValueResponseType.Errored, result.Type);
            Assert.Equal("Invalid NOT SET expression at line 2", result.Reason);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestNotSetAnswersWhetherTheLastWriteTookEffect([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions, bool ephemeral)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string e = ephemeral ? "E" : "";

        try
        {
            await Write(kahuna1, $"{e}SET nsg_del 'v'");
            await Write(kahuna1, $"{e}SET nsg_ext 'v'");

            Assert.Equal("set", await RunForValue(kahuna2, $"""
                {e}DELETE nsg_del
                IF NOT SET THEN RETURN 'notset' END
                RETURN 'set'
                """));

            Assert.Equal("notset", await RunForValue(kahuna2, $"""
                {e}DELETE nsg_never_written
                IF NOT SET THEN RETURN 'notset' END
                RETURN 'set'
                """));

            Assert.Equal("set", await RunForValue(kahuna2, $"""
                {e}EXTEND nsg_ext 10000
                IF NOT SET THEN RETURN 'notset' END
                RETURN 'set'
                """));

            Assert.Equal("notset", await RunForValue(kahuna2, $"""
                {e}EXTEND nsg_never_written 10000
                IF NOT SET THEN RETURN 'notset' END
                RETURN 'set'
                """));

            // A delete judges the transaction's own view: a key it set is present to its delete, even when the
            // committed entry is only the placeholder its lock created, and a key it already deleted is absent.
            Assert.Equal("set", await RunForValue(kahuna2, $"""
                {e}SET nsg_own 'v'
                {e}DELETE nsg_own
                IF NOT SET THEN RETURN 'notset' END
                RETURN 'set'
                """));

            Assert.Equal(KeyValueResponseType.DoesNotExist, (await Run(kahuna1, $"{e}GET nsg_own")).Type);

            await Write(kahuna1, $"{e}SET nsg_twice 'v'");

            Assert.Equal("notset", await RunForValue(kahuna2, $"""
                {e}DELETE nsg_twice
                LET q = 1
                {e}DELETE nsg_twice
                IF NOT SET THEN RETURN 'notset' END
                RETURN 'set'
                """));

            Assert.Equal(KeyValueResponseType.DoesNotExist, (await Run(kahuna1, $"{e}GET nsg_twice")).Type);

            // A read between the write and the guard does not decide it.
            Assert.Equal("notset", await RunForValue(kahuna2, $"""
                {e}SET nsg_ext 'w' NX
                {e}GET nsg_ext
                IF NOT SET THEN RETURN 'notset' END
                RETURN 'set'
                """));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Two leading SETs over distinct keys run as one batched set-many. A failed conditional set in the batch
    /// wrote nothing, so the other write commits, and NOT SET answers for the batch's last statement whatever
    /// order the fan-out answers in. Many key pairs spread the batches over partitions led by different nodes.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestBatchedSetWithFailedConditionCommits([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions, bool ephemeral)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string e = ephemeral ? "E" : "";

        try
        {
            for (int i = 0; i < 16; i++)
            {
                await Write(kahuna1, $"{e}SET bsg_taken_{i} 'v'");

                // The failed conditional set first, the confirmed write last.
                Assert.Equal("set", await RunForValue(kahuna2, $"""
                    {e}SET bsg_taken_{i} 'y' NX
                    {e}SET bsg_first_{i} 'z'
                    IF NOT SET THEN RETURN 'notset' END
                    RETURN 'set'
                    """));

                // The confirmed write first, the failed conditional set last.
                Assert.Equal("notset", await RunForValue(kahuna2, $"""
                    {e}SET bsg_second_{i} 'z'
                    {e}SET bsg_taken_{i} 'y' NX
                    IF NOT SET THEN RETURN 'notset' END
                    RETURN 'set'
                    """));

                Assert.Equal("v", await RunForValue(kahuna1, $"{e}GET bsg_taken_{i}"));
                Assert.Equal("z", await RunForValue(kahuna1, $"{e}GET bsg_first_{i}"));
                Assert.Equal("z", await RunForValue(kahuna1, $"{e}GET bsg_second_{i}"));
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Two leading DELETEs over distinct keys run as one batched delete-many; NOT SET answers for its last
    /// statement whatever order the fan-out answers in.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestBatchedDeleteAnswersForItsLastStatement([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions, bool ephemeral)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string e = ephemeral ? "E" : "";

        try
        {
            for (int i = 0; i < 16; i++)
            {
                await Write(kahuna1, $"{e}SET bdg_a_{i} 'v'");
                await Write(kahuna1, $"{e}SET bdg_b_{i} 'v'");

                Assert.Equal("notset", await RunForValue(kahuna2, $"""
                    {e}DELETE bdg_a_{i}
                    {e}DELETE bdg_missing_{i}
                    IF NOT SET THEN RETURN 'notset' END
                    RETURN 'set'
                    """));

                Assert.Equal("set", await RunForValue(kahuna2, $"""
                    {e}DELETE bdg_missing_{i}
                    {e}DELETE bdg_b_{i}
                    IF NOT SET THEN RETURN 'notset' END
                    RETURN 'set'
                    """));

                Assert.Equal(KeyValueResponseType.DoesNotExist, (await Run(kahuna1, $"{e}GET bdg_a_{i}")).Type);
                Assert.Equal(KeyValueResponseType.DoesNotExist, (await Run(kahuna1, $"{e}GET bdg_b_{i}")).Type);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestNotDeletedAnswersForTheLastDelete([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions, bool ephemeral)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string e = ephemeral ? "E" : "";

        try
        {
            await Write(kahuna1, $"{e}SET ndg_a 'v'");
            await Write(kahuna1, $"{e}SET ndg_b 'v'");

            Assert.Equal("deleted", await RunForValue(kahuna2, $"""
                {e}DELETE ndg_a
                IF NOT DELETED THEN RETURN 'notdeleted' END
                RETURN 'deleted'
                """));

            Assert.Equal("notdeleted", await RunForValue(kahuna2, $"""
                {e}DELETE ndg_never_written
                IF NOT DELETED THEN RETURN 'notdeleted' END
                RETURN 'deleted'
                """));

            // A write of another kind after the delete does not decide NOT DELETED, though it decides NOT SET.
            Assert.Equal("notdeleted|set", await RunForValue(kahuna2, $"""
                {e}DELETE ndg_never_written
                {e}SET ndg_other 'x'
                LET d = 'deleted'
                LET s = 'set'
                IF NOT DELETED THEN LET d = 'notdeleted' END
                IF NOT SET THEN LET s = 'notset' END
                RETURN concat(concat(d, '|'), s)
                """));

            // A batched delete answers for its last statement.
            Assert.Equal("deleted", await RunForValue(kahuna2, $"""
                {e}DELETE ndg_never_written
                {e}DELETE ndg_b
                IF NOT DELETED THEN RETURN 'notdeleted' END
                RETURN 'deleted'
                """));

            // Any case, and a line break between the two words.
            Assert.Equal("notdeleted", await RunForValue(kahuna2, $"""
                {e}DELETE ndg_never_written
                if not
                   deleted then return 'notdeleted' end
                return 'deleted'
                """));

            KeyValueTransactionResult result = await Run(kahuna2, """
                SET ndg_c 'v'
                IF NOT DELETED THEN RETURN 'notdeleted' END
                RETURN 'deleted'
                """);
            Assert.Equal(KeyValueResponseType.Errored, result.Type);
            Assert.Equal("Invalid NOT DELETED expression at line 2", result.Reason);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestNotExtendedAnswersForTheLastExtend([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions, bool ephemeral)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string e = ephemeral ? "E" : "";

        try
        {
            await Write(kahuna1, $"{e}SET neg_a 'v'");
            await Write(kahuna1, $"{e}SET neg_b 'v'");

            Assert.Equal("extended", await RunForValue(kahuna2, $"""
                {e}EXTEND neg_a 10000
                IF NOT EXTENDED THEN RETURN 'notextended' END
                RETURN 'extended'
                """));

            Assert.Equal("notextended", await RunForValue(kahuna2, $"""
                {e}EXTEND neg_never_written 10000
                IF NOT EXTENDED THEN RETURN 'notextended' END
                RETURN 'extended'
                """));

            // A write of another kind after the extend does not decide NOT EXTENDED, though it decides NOT SET.
            Assert.Equal("notextended|set", await RunForValue(kahuna2, $"""
                {e}EXTEND neg_never_written 10000
                {e}DELETE neg_b
                LET x = 'extended'
                LET s = 'set'
                IF NOT EXTENDED THEN LET x = 'notextended' END
                IF NOT SET THEN LET s = 'notset' END
                RETURN concat(concat(x, '|'), s)
                """));

            Assert.Equal("notextended", await RunForValue(kahuna2, $"""
                {e}EXTEND neg_never_written 10000
                IF Not   Extended THEN RETURN 'notextended' END
                RETURN 'extended'
                """));

            KeyValueTransactionResult result = await Run(kahuna2, """
                DELETE neg_a
                IF NOT EXTENDED THEN RETURN 'notextended' END
                RETURN 'extended'
                """);
            Assert.Equal(KeyValueResponseType.Errored, result.Type);
            Assert.Equal("Invalid NOT EXTENDED expression at line 2", result.Reason);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// NOT DELETED and NOT EXTENDED are each one token, so "deleted" and "extended" alone stay ordinary
    /// identifiers and key names. A boolean variable with one of these names is negated with ! or with parentheses.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestDeletedAndExtendedStayIdentifiers([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            Assert.Equal("3", await RunForValue(kahuna1, """
                LET deleted = 1
                LET extended = 2
                RETURN to_string(deleted + extended)
                """));

            Assert.Equal("v", await RunForValue(kahuna1, """
                SET deleted 'v'
                LET x = GET deleted
                RETURN x
                """));

            Assert.Equal("true|true", await RunForValue(kahuna1, """
                LET deleted = false
                LET extended = false
                RETURN concat(concat(to_string(!deleted), '|'), to_string(NOT (extended)))
                """));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
