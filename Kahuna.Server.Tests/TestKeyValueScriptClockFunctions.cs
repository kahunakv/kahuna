using System.Globalization;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Execution-level tests for the cluster clock functions, hlc() and hlc_counter().
///
/// <para>What they have to prove is the property current_time() cannot offer: a value a script may
/// compare across nodes. So the readings are taken on different nodes of one cluster, and on both
/// sides of a script boundary, not only twice inside one expression.</para>
/// </summary>
public class TestKeyValueScriptClockFunctions : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptClockFunctions(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<string> Run(IKahuna kahuna, string script)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.True(resp.Type == KeyValueResponseType.Get, $"expected Get for \"{script}\", got {resp.Type}: {resp.Reason}");

        return Encoding.UTF8.GetString(resp.Value ?? []);
    }

    private static async Task RunSet(IKahuna kahuna, string script)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.True(resp.Type == KeyValueResponseType.Set, $"expected Set for \"{script}\", got {resp.Type}: {resp.Reason}");
    }

    private static async Task<long> RunLong(IKahuna kahuna, string script)
    {
        string text = await Run(kahuna, script);

        return long.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture);
    }

    private static async Task<string> AssertErrored(IKahuna kahuna, string script)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.Equal(KeyValueResponseType.Errored, resp.Type);
        Assert.False(string.IsNullOrEmpty(resp.Reason), "an errored script must carry a reason");

        return resp.Reason!;
    }

    /// <summary>
    /// The physical component is unix milliseconds, the same scale current_time() reports. A reading on
    /// another scale would compare against a stored deadline as a number and be wrong by years, which no
    /// assertion on the reading alone would catch.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestHlcIsUnixMilliseconds([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            long reading = await RunLong(kahuna1, "RETURN hlc()");

            long after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // The hybrid logical clock may run ahead of the wall clock, which is what its logical component
            // exists to allow, so the upper bound carries a minute of slack. The lower bound does not: a
            // reading below the wall clock of a moment ago is not a clock this cluster kept.
            Assert.True(reading >= before, $"hlc() returned {reading}, below the wall clock reading {before}");
            Assert.True(reading <= after + 60_000, $"hlc() returned {reading}, far above the wall clock reading {after}");

            long counter = await RunLong(kahuna1, "RETURN hlc_counter()");

            Assert.True(counter >= 0, $"hlc_counter() returned {counter}");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// One script execution observes one reading. That is what makes hlc() and hlc_counter() describe the
    /// same instant: an author who reads both must not pair the milliseconds of one timestamp with the
    /// counter of another.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestOneScriptSeesOneReading([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string script = """
            LET first = hlc()
            LET second = hlc()
            RETURN second >= first && second = first
            """;

            Assert.Equal("true", await Run(kahuna1, script));

            // The same holds for a script that writes, where the reading is taken while the transaction
            // runs its two-phase commit rather than on the single-command path.
            string transactional = $"""
            SET k{Guid.NewGuid().ToString("N")[..10]} 'v'
            LET first = hlc()
            LET second = hlc()
            RETURN second = first
            """;

            Assert.Equal("true", await Run(kahuna1, transactional));

            // A loop calls it many times and still sees the one reading.
            string looped = """
            LET stamp = hlc()
            LET changed = false
            FOR i IN 0..9 DO
                IF hlc() <> stamp THEN
                    LET changed = true
                END
            END
            RETURN changed
            """;

            Assert.Equal("false", await Run(kahuna1, looped));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Successive executions never go backwards, on one node or across two of them. This is the property
    /// current_time() cannot promise, because it reads the wall clock of whichever node answered.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestReadingsNeverGoBackwards([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            long first = await RunLong(kahuna1, "RETURN hlc()");
            long second = await RunLong(kahuna1, "RETURN hlc()");

            Assert.True(second >= first, $"second reading {second} is below the first reading {first}");

            // Across nodes, which is the case the function exists for.
            long onNodeTwo = await RunLong(kahuna2, "RETURN hlc()");

            Assert.True(onNodeTwo >= second, $"node 2 read {onNodeTwo}, below node 1's reading {second}");

            long onNodeThree = await RunLong(kahuna3, "RETURN hlc()");

            Assert.True(onNodeThree >= onNodeTwo, $"node 3 read {onNodeThree}, below node 2's reading {onNodeTwo}");

            long backOnNodeOne = await RunLong(kahuna1, "RETURN hlc()");

            Assert.True(backOnNodeOne >= onNodeThree, $"node 1 read {backOnNodeOne}, below node 3's reading {onNodeThree}");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The reading is installed in the clock, not only observed. A stamp a node hands out without
    /// recording it could be minted a second time, and two events that share one timestamp cannot be
    /// ordered. The transaction identity of a later transaction is minted from the same clock, so it is
    /// what shows the reading landed.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestReadingAdvancesTheClock([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            long reading = await RunLong(kahuna1, "RETURN hlc()");

            Kommander.Time.HLCTimestamp afterwards = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

            // Strictly greater: the clock cannot hand out the reading the script already took.
            Assert.True(afterwards.L > reading || (afterwards.L == reading && afterwards.C > 0),
                $"the clock is at ({afterwards.L}, {afterwards.C}) after a script read {reading}");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestClockFunctionsTakeNoArgument([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string reason = await AssertErrored(kahuna1, "RETURN hlc(1)");
            Assert.Contains("Invalid number of arguments for 'hlc' function", reason, StringComparison.Ordinal);
            Assert.Contains("at line 1", reason, StringComparison.Ordinal);

            string counterReason = await AssertErrored(kahuna1, "RETURN hlc_counter('x')");
            Assert.Contains("Invalid number of arguments for 'hlc_counter' function", counterReason, StringComparison.Ordinal);
            Assert.Contains("at line 1", counterReason, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The shape the functions exist for: a deadline written by one node and read by another. Both sides
    /// read the same clock, so the comparison means the same thing wherever the script runs.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestDeadlineWrittenOnOneNodeAndReadOnAnother([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = "k" + Guid.NewGuid().ToString("N")[..10];

            await RunSet(kahuna1, $"SET {key} hlc() + 60000");

            string check = $"""
            LET raw = GET {key}
            LET deadline = to_int(raw)
            IF deadline > hlc() THEN
                RETURN 'open'
            END
            RETURN 'expired'
            """;

            Assert.Equal("open", await Run(kahuna2, check));

            await RunSet(kahuna1, $"SET {key} hlc() - 60000");

            Assert.Equal("expired", await Run(kahuna2, check));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// current_time() keeps working and keeps its meaning. Removing it would break every script that
    /// writes a human-readable stamp, and it is still the right answer for one.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestCurrentTimeIsUnchanged([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            long stamp = await RunLong(kahuna1, "RETURN current_time()");

            long after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            Assert.InRange(stamp, before, after);

            string reason = await AssertErrored(kahuna1, "RETURN current_time(1)");
            Assert.Contains("Invalid number of arguments for 'current_time' function", reason, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
