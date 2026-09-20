using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Measures what one script execution allocates, on the shapes where the executor's per-statement cost
/// is visible rather than buried under the write path.
///
/// <para>The expression walk, the literal parse, the set-flag collection and the per-statement result
/// object are all paid per statement and again per loop iteration, so a loop is the only shape where a
/// regression in them shows at all. The two read-only shapes here touch no key, which is what keeps the
/// number attributable: a script that writes spends most of its allocation inside Raft and the
/// persistence backend, where a change to the evaluator cannot be read off the total.</para>
///
/// <para>Only the per-iteration figure is asserted against a ceiling. It is a difference between two
/// measurements of the same script shape, so every fixed cost — cluster, admission, two-phase commit —
/// cancels out of it, which is what makes a threshold on it meaningful. The absolute numbers move with
/// the runtime and with every layer under the executor, so they are reported and left unasserted; what
/// stands behind them is that each measured batch ran the script to a committed outcome fifty times in
/// a row.</para>
/// </summary>
public class TestScriptExecutionAllocation : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly ITestOutputHelper output;

    public TestScriptExecutionAllocation(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
        output = outputHelper;
    }

    /// <summary>
    /// A guard and an arithmetic assignment, once and then sixty-four times. The difference between the
    /// two divided by the iteration count is what one loop iteration of that body costs.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestGuardedLoopAllocationPerIteration([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            byte[] once = Encoding.UTF8.GetBytes(
                """
                BEGIN
                  LET x = 7
                  LET y = 3
                  LET total = 0
                  FOR i IN 0..1 DO
                    IF x > 0 && y < 10 THEN
                      LET total = total + i
                    END
                  END
                  COMMIT
                END
                """);

            byte[] many = Encoding.UTF8.GetBytes(
                """
                BEGIN
                  LET x = 7
                  LET y = 3
                  LET total = 0
                  FOR i IN 0..64 DO
                    IF x > 0 && y < 10 THEN
                      LET total = total + i
                    END
                  END
                  COMMIT
                END
                """);

            long onceBytes = await Measure(kahuna1, once, null);
            long manyBytes = await Measure(kahuna1, many, null);

            // 0..1 yields two elements and 0..64 yields sixty-five, so the extra work is 63 iterations.
            const int extraIterations = 63;

            double perIteration = (manyBytes - onceBytes) / (double)extraIterations;

            output.WriteLine($"guard loop, 2 iterations:  {onceBytes} B/execution");
            output.WriteLine($"guard loop, 65 iterations: {manyBytes} B/execution");
            output.WriteLine($"per iteration:             {perIteration:F1} B");

            // The loop body must cost something, or the measurement is not reaching the executor.
            Assert.True(manyBytes > onceBytes, $"expected the longer loop to allocate more: {manyBytes} vs {onceBytes}");

            // This body — two comparisons, a logical operator, an addition and an assignment — costs about
            // 870 B per iteration, down from about 1,530 B before the expression results, the literals, the
            // statement results and the batch probe stopped being rebuilt per iteration. The ceiling leaves
            // room for the runtime to move without leaving room for those to come back.
            const int perIterationCeiling = 1200;

            Assert.True(
                perIteration < perIterationCeiling,
                $"a loop iteration allocates {perIteration:F0} B, over the {perIterationCeiling} B ceiling");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// One placeholder referenced from several statements inside a loop. Placeholder resolution walks the
    /// parameter list per reference, so this shape is where that walk is paid most often.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestParameterizedLoopAllocation([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            byte[] script = Encoding.UTF8.GetBytes(
                """
                BEGIN
                  LET total = 0
                  FOR i IN 0..64 DO
                    IF @amount > 0 THEN
                      LET total = total + @amount
                    END
                  END
                  COMMIT
                END
                """);

            List<KeyValueParameter> parameters =
            [
                new() { Key = "@unused1", Value = "1" },
                new() { Key = "@unused2", Value = "2" },
                new() { Key = "@unused3", Value = "3" },
                new() { Key = "@amount", Value = "42" }
            ];

            long bytes = await Measure(kahuna1, script, parameters);

            // Reported only. Measure asserts the script committed on every run of the measured batch,
            // which is the assertion that matters here; the absolute figure is for comparison over time.
            output.WriteLine($"parameterized loop, 65 iterations: {bytes} B/execution");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A guarded write with set flags inside a loop, once per locking mode. The absolute number is
    /// dominated by replication and persistence, so the two modes are only comparable with each other —
    /// the optimistic one skips the whole lock-acquisition prologue.
    ///
    /// <para>These two figures have no earlier baseline to compare against. An initial measurement of this
    /// shape averaged retry paths in, because the helper below did not yet require every run in the batch
    /// to have committed; the numbers it produced were roughly three times these and are not a comparison.
    /// The read-only shapes above were unaffected, which is how that was established: their numbers moved
    /// by less than a hundredth of a percent when the helper was tightened.</para>
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestWriteLoopAllocationByLockingMode([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string tag = Guid.NewGuid().ToString("N")[..8];

            byte[] optimistic = Encoding.UTF8.GetBytes(
                $"""
                 BEGIN (locking=optimistic)
                   FOR i IN 0..8 DO
                     SET `alloc/{tag}/opt` 'v' EX 60000
                   END
                   COMMIT
                 END
                 """);

            byte[] pessimistic = Encoding.UTF8.GetBytes(
                $"""
                 BEGIN (locking=pessimistic)
                   FOR i IN 0..8 DO
                     SET `alloc/{tag}/pes` 'v' EX 60000
                   END
                   COMMIT
                 END
                 """);

            long optimisticBytes = await Measure(kahuna1, optimistic, null);
            long pessimisticBytes = await Measure(kahuna1, pessimistic, null);

            // Reported only, for the same reason as the shape above.
            output.WriteLine($"write loop, optimistic:  {optimisticBytes} B/execution");
            output.WriteLine($"write loop, pessimistic: {pessimisticBytes} B/execution");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Runs the script until the parse cache and the JIT are warm, then reports the mean allocation of a
    /// measured batch in which every execution ran to a committed outcome.
    ///
    /// <para>A script that met a leader change answers <c>MustRetry</c>, and a retried script allocates
    /// along a different path, so a batch containing one would not measure what this test is about. The
    /// batch is therefore taken again, up to a bounded number of times, until one of them is clean.</para>
    ///
    /// <para>The counter is per thread and the executor awaits, so work can finish on a different thread
    /// than the one that started it. The number is a lower bound on the true total, not an exact figure.
    /// It is still the right measurement for this purpose: the executor's own statement loop runs on the
    /// calling thread, which is where every allocation in this feature's scope is made.</para>
    /// </summary>
    private static async Task<long> Measure(IKahuna kahuna, byte[] script, List<KeyValueParameter>? parameters)
    {
        const int warmup = 20;
        const int measured = 50;
        const int attempts = 5;

        for (int i = 0; i < warmup; i++)
            await kahuna.TryExecuteTransactionScript(script, null, parameters);

        KeyValueResponseType lastType = KeyValueResponseType.Errored;
        string? lastReason = null;

        for (int attempt = 0; attempt < attempts; attempt++)
        {
            bool clean = true;

            long before = GC.GetAllocatedBytesForCurrentThread();

            for (int i = 0; i < measured; i++)
            {
                KeyValueTransactionResult result = await kahuna.TryExecuteTransactionScript(script, null, parameters);

                lastType = result.Type;
                lastReason = result.Reason;

                if (result.Type is not (KeyValueResponseType.Set or KeyValueResponseType.Get))
                    clean = false;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            if (clean)
                return allocated / measured;
        }

        Assert.Fail($"the script never ran a clean batch; last outcome was {lastType} {lastReason}");

        return 0;
    }
}
