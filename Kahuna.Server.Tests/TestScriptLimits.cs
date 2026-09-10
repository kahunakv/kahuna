using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A syntax tree is walked by one call frame per level, and a stack overflow cannot be caught, so an
/// unbounded tree let a single request abort the whole process. Measured on this codebase, that happened at
/// roughly one thousand levels of either hostile shape: a chain of operators, or a flat run of statements,
/// which is left-recursive in the grammar and so is a deep spine too. These tests assert that both shapes now
/// return an error and that the node keeps serving afterwards.
/// </summary>
public class TestScriptLimits : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestScriptLimits(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Theory, CombinatorialData]
    public async Task TestHostileScriptsAreRefusedAndTheNodeSurvives([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // A chain of operators, well past the depth that used to abort the process.
            StringBuilder deepExpression = new();
            deepExpression.Append("RETURN 1");
            for (int i = 0; i < 5000; i++)
                deepExpression.Append("+1");

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(deepExpression.ToString()), null, null);

            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Contains("nested too deeply", resp.Reason ?? "", StringComparison.Ordinal);

            // A flat run of statements. The grammar makes a statement list left-recursive, so this is a deep
            // spine and the same limit catches it.
            StringBuilder manyStatements = new();
            for (int i = 0; i < 5000; i++)
                manyStatements.AppendLine("LET x = 1");
            manyStatements.AppendLine("RETURN x");

            resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(manyStatements.ToString()), null, null);

            Assert.Equal(KeyValueResponseType.Errored, resp.Type);
            Assert.Contains("nested too deeply", resp.Reason ?? "", StringComparison.Ordinal);

            // A body past the length limit is refused before the parse, with a typed refusal.
            string oversize = "RETURN '" + new string('x', 70_000) + "'";

            resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(oversize), null, null);

            Assert.Equal(KeyValueResponseType.InvalidInput, resp.Type);
            Assert.Contains("too long", resp.Reason ?? "", StringComparison.Ordinal);

            // The node still serves after all three.
            KeyValueTransactionResult ok = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes("RETURN 1+1"), null, null);

            Assert.Equal(KeyValueResponseType.Get, ok.Type);
            Assert.Equal("2", Encoding.UTF8.GetString(ok.Value ?? []));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A script of ordinary size and shape must be unaffected by the limits.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestOrdinaryScriptsRunUnderTheLimits([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = Guid.NewGuid().ToString("N")[..10];

            StringBuilder builder = new();
            builder.AppendLine("BEGIN");
            for (int i = 0; i < 100; i++)
                builder.AppendLine($"    SET '{key}/{i}' 'value{i}'");
            builder.AppendLine("    COMMIT");
            builder.AppendLine("END");

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(builder.ToString()), null, null);

            Assert.True(resp.Type is KeyValueResponseType.Set or KeyValueResponseType.Get, resp.Type + " " + resp.Reason);

            KeyValueTransactionResult read = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"GET '{key}/99'"), null, null);

            Assert.Equal(KeyValueResponseType.Get, read.Type);
            Assert.Equal("value99", Encoding.UTF8.GetString(read.Value ?? []));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
