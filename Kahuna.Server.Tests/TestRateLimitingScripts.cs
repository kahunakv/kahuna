using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Drives the admission counter that a rate limiter is built from, in the form
/// <c>kahuna-bench --workload rate-limit</c> sends it.
///
/// The whole decision is one script transaction: read the counter, refuse the caller if it already
/// reached its budget, otherwise write the counter back. A read followed by a separate write would
/// let two callers observe the same count and both be admitted, so the properties worth testing are
/// the ones a single caller never exercises:
///
/// - The budget is shared. Requests that arrive through different nodes spend one budget between
///   them, not one budget each.
/// - The budget holds under a rush. Callers racing a counter that is one request below its budget
///   admit exactly one of themselves.
/// - A fixed window is fixed. The counter of a window that filled up stays full for the rest of that
///   window, while the next window starts from zero.
/// - A sliding counter extends. Every admitted request pushes the expiry out, so a caller that keeps
///   sending never gets the reset a fixed window would have given it.
///
/// Two statements here differ from the published recipe, because the script engine rejects the
/// published form. The condition compares against <c>null</c> rather than writing <c>NOT current</c>:
/// the language has one strict truthiness model, and <c>NOT</c> demands a boolean operand rather
/// than reading an absent value as false. The expiry and the budget are wrapped in <c>to_int</c>:
/// a script parameter always arrives as a string, and <c>EX</c> demands a number.
/// </summary>
public class TestRateLimitingScripts : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRateLimitingScripts(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// The counter over the ephemeral key space, which is what a rate limiter wants: the counter is
    /// temporary state that nothing needs to survive a restart.
    /// </summary>
    private const string EphemeralCounterScript = """
    LET current = EGET @counter_key
    IF current = null THEN
      ESET @counter_key 1 EX to_int(@expires_ms)
      RETURN 1
    END
    LET count = to_int(current)
    IF count >= to_int(@limit) THEN
      RETURN 0
    END
    ESET @counter_key count + 1 EX to_int(@expires_ms)
    RETURN 1
    """;

    /// <summary>
    /// The same counter over the persistent key space, for a limit that must survive a process
    /// restart. Every write is replicated and persisted, so this costs far more per request.
    /// </summary>
    private const string PersistentCounterScript = """
    LET current = GET @counter_key
    IF current = null THEN
      SET @counter_key 1 EX to_int(@expires_ms)
      RETURN 1
    END
    LET count = to_int(current)
    IF count >= to_int(@limit) THEN
      RETURN 0
    END
    SET @counter_key count + 1 EX to_int(@expires_ms)
    RETURN 1
    """;

    private const string Allowed = "1";

    private const string Refused = "0";

    private static string CounterScript(KeyValueDurability durability) =>
        durability == KeyValueDurability.Ephemeral ? EphemeralCounterScript : PersistentCounterScript;

    private static List<KeyValueParameter> Parameters(string counterKey, int budget, int expiresMs) =>
    [
        new() { Key = "@counter_key", Value = counterKey },
        new() { Key = "@limit", Value = budget.ToString() },
        new() { Key = "@expires_ms", Value = expiresMs.ToString() }
    ];

    /// <summary>
    /// Neither MustRetry nor Aborted admitted the caller or moved the counter, so both are safe to
    /// run again. A retried attempt cannot spend two units of the budget.
    /// </summary>
    private static async Task<KeyValueTransactionResult> RunRetrying(
        IKahuna kahuna, string script, List<KeyValueParameter> parameters)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(script);

        KeyValueTransactionResult result = await kahuna.TryExecuteTransactionScript(bytes, null, parameters);

        for (int attempt = 1; attempt < 60; attempt++)
        {
            if (result.Type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted))
                break;

            await Task.Delay(Math.Min(5 * attempt, 50));
            result = await kahuna.TryExecuteTransactionScript(bytes, null, parameters);
        }

        return result;
    }

    /// <summary>
    /// Runs one admission attempt and returns the verdict, failing the test if the script did not
    /// finish. A script that finishes always answers "1" or "0".
    /// </summary>
    private static async Task<string> Admit(
        IKahuna kahuna, KeyValueDurability durability, string counterKey, int budget, int expiresMs)
    {
        KeyValueTransactionResult result = await RunRetrying(
            kahuna, CounterScript(durability), Parameters(counterKey, budget, expiresMs));

        Assert.True(result.Type == KeyValueResponseType.Get, $"{result.Type}: {result.Reason}");

        return Encoding.UTF8.GetString(result.Value ?? []);
    }

    private static string Subject() => "rl" + Guid.NewGuid().ToString("N")[..8];

    /// <summary>
    /// Twelve requests for one subject, spread over three nodes, against a budget of five. Exactly
    /// five are admitted and seven are refused. A limiter that kept a counter on each node would
    /// admit five on every node and let fifteen through.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestBudgetIsSharedAcrossNodes(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            IKahuna[] fleet = [kahuna1, kahuna2, kahuna3];
            string counterKey = "rate-limit/" + Subject() + "/0";

            int admitted = 0;
            int refused = 0;

            for (int i = 0; i < 12; i++)
            {
                string verdict = await Admit(fleet[i % fleet.Length], durability, counterKey, 5, 60_000);

                if (verdict == Allowed)
                    admitted++;
                else if (verdict == Refused)
                    refused++;
                else
                    Assert.Fail($"Unexpected verdict '{verdict}'");
            }

            Assert.Equal(5, admitted);
            Assert.Equal(7, refused);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Twenty callers spread over three nodes rush a counter that already spent four units of a
    /// budget of five. Exactly one of them is admitted. Deciding inside one transaction is what makes
    /// this hold: a read, then a write, would let every caller read the same four and write five.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestLastUnitOfBudgetGoesToOneCallerUnderContention(
        [CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            IKahuna[] fleet = [kahuna1, kahuna2, kahuna3];
            string counterKey = "rate-limit/" + Subject() + "/0";

            for (int i = 0; i < 4; i++)
                Assert.Equal(Allowed, await Admit(kahuna1, KeyValueDurability.Ephemeral, counterKey, 5, 60_000));

            Task<string>[] rush = new Task<string>[20];

            for (int i = 0; i < rush.Length; i++)
                rush[i] = Admit(fleet[i % fleet.Length], KeyValueDurability.Ephemeral, counterKey, 5, 60_000);

            string[] verdicts = await Task.WhenAll(rush);

            int admitted = 0;

            foreach (string verdict in verdicts)
            {
                Assert.True(verdict is Allowed or Refused, $"Unexpected verdict '{verdict}'");

                if (verdict == Allowed)
                    admitted++;
            }

            Assert.Equal(1, admitted);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A window that filled up stays full, and the next window starts from zero.
    ///
    /// The window start is part of the key, so the two windows count on two counters and the request
    /// that opens the second one cannot touch the first. That is the property that separates this
    /// from a sliding counter, and it is why a fixed window must be given the time left in the
    /// current window as its expiry rather than the whole window length.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestFixedWindowCountsPerWindow(
        [CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string subject = Subject();
            string firstWindow = $"rate-limit/{subject}/1000";
            string secondWindow = $"rate-limit/{subject}/2000";

            for (int i = 0; i < 3; i++)
                Assert.Equal(Allowed, await Admit(kahuna1, KeyValueDurability.Ephemeral, firstWindow, 3, 60_000));

            Assert.Equal(Refused, await Admit(kahuna2, KeyValueDurability.Ephemeral, firstWindow, 3, 60_000));

            // The next window is a different counter, so the same subject is admitted again.
            Assert.Equal(Allowed, await Admit(kahuna3, KeyValueDurability.Ephemeral, secondWindow, 3, 60_000));

            // The window that filled up is still full. A refused request writes nothing, so it can
            // neither raise the count nor extend the counter's life.
            Assert.Equal(Refused, await Admit(kahuna1, KeyValueDurability.Ephemeral, firstWindow, 3, 60_000));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A sliding counter outlives its own expiry while the caller keeps sending.
    ///
    /// Four requests spaced 600 ms apart span 1 800 ms, which is longer than the 1 500 ms expiry each
    /// of them writes. They all land on one counter anyway, because every admitted request pushes the
    /// expiry out, so the fifth request is refused. A fixed window of the same length would have
    /// reset in the middle and admitted it.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestSlidingCounterExtendsWhileTheCallerKeepsSending(
        [CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // A sliding counter has no window in its key: one subject, one counter.
            string counterKey = "rate-limit/" + Subject();
            const int ttlMs = 1500;

            for (int i = 0; i < 4; i++)
            {
                if (i > 0)
                    await Task.Delay(600, TestContext.Current.CancellationToken);

                Assert.Equal(Allowed, await Admit(kahuna1, KeyValueDurability.Ephemeral, counterKey, 4, ttlMs));
            }

            Assert.Equal(Refused, await Admit(kahuna2, KeyValueDurability.Ephemeral, counterKey, 4, ttlMs));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A counter whose expiry elapses with the caller idle is gone, and the budget starts over.
    ///
    /// This is the other half of the sliding behaviour: the extension only lasts while requests keep
    /// arriving. It also shows that a refused request does not extend anything — the counter is
    /// refusing when the wait starts and still expires on schedule.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestCounterExpiresWhileTheCallerIsIdle(
        [CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string counterKey = "rate-limit/" + Subject();
            const int ttlMs = 1000;

            Assert.Equal(Allowed, await Admit(kahuna1, KeyValueDurability.Ephemeral, counterKey, 2, ttlMs));
            Assert.Equal(Allowed, await Admit(kahuna2, KeyValueDurability.Ephemeral, counterKey, 2, ttlMs));
            Assert.Equal(Refused, await Admit(kahuna3, KeyValueDurability.Ephemeral, counterKey, 2, ttlMs));

            await Task.Delay(ttlMs + 500, TestContext.Current.CancellationToken);

            Assert.Equal(Allowed, await Admit(kahuna1, KeyValueDurability.Ephemeral, counterKey, 2, ttlMs));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
