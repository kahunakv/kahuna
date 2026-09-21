using System.Text;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// An auto-commit script whose only key is one ephemeral key led by the coordinating node runs inside a single
/// turn of the actor that owns the key. The turn sends the same requests to the same handlers in the same
/// order as the general path, so it may change what a script costs and nothing else. Every scenario here runs
/// on a node with turns on and on a node with turns off, and the two must agree on the answer, the reason, the
/// stored value and the revision. The counters say which path actually ran, so a scenario cannot pass by
/// quietly taking the other one.
/// </summary>
public sealed class TestScriptActorTurns : BaseCluster
{
    private const string CounterScript = """
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

    private readonly ILoggerFactory loggerFactory;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestScriptActorTurns(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, bool turns, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            ScriptActorTurns = turns
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("turns/probe", ct);

        return node;
    }

    private static List<KeyValueParameter> CounterParameters(string counterKey, int budget) =>
    [
        new() { Key = "@counter_key", Value = counterKey },
        new() { Key = "@limit", Value = budget.ToString() },
        new() { Key = "@expires_ms", Value = "60000" }
    ];

    private static List<KeyValueParameter> KeyParameter(string key) => [new() { Key = "@k", Value = key }];

    private static Task<KeyValueTransactionResult> Run(IKahuna kahuna, string script, List<KeyValueParameter>? parameters = null) =>
        kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, parameters);

    private static async Task<KeyValueTransactionResult> RunRetrying(IKahuna kahuna, string script, List<KeyValueParameter> parameters)
    {
        KeyValueTransactionResult result = await Run(kahuna, script, parameters);

        for (int attempt = 1; attempt < 80; attempt++)
        {
            if (result.Type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted))
                break;

            await Task.Delay(Math.Min(5 * attempt, 40));
            result = await Run(kahuna, script, parameters);
        }

        return result;
    }

    private static async Task<(KeyValueResponseType Type, string? Value, long Revision)> Read(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct);

        return (type, entry?.Value is null ? null : Encoding.UTF8.GetString(entry.Value), entry?.Revision ?? -1);
    }

    /// <summary>What a caller can observe of one script run.</summary>
    private static (KeyValueResponseType Type, string? Reason, string? Value, long Revision) Observed(KeyValueTransactionResult result) =>
        (result.Type, result.Reason, result.Value is null ? null : Encoding.UTF8.GetString(result.Value), result.Revision);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Counter_SameVerdicts_SameValue_SameRevision(bool turns)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, turns, ct);

        string key = "turns/counter/" + Guid.NewGuid().ToString("N")[..8];
        long before = DurableTransactionMetrics.ScriptActorTurnsCount;

        List<string> verdicts = [];

        for (int i = 0; i < 6; i++)
        {
            KeyValueTransactionResult result = await RunRetrying(node.Kahuna, CounterScript, CounterParameters(key, 4));

            Assert.True(result.Type == KeyValueResponseType.Get, $"{result.Type}: {result.Reason}");
            verdicts.Add(Encoding.UTF8.GetString(result.Value ?? []));
        }

        Assert.Equal(["1", "1", "1", "1", "0", "0"], verdicts);
        Assert.Equal((KeyValueResponseType.Get, "4", 3L), await Read(node.Kahuna, key, ct));

        // Writers and refusals alike ran in a turn. The counter is process-wide and other classes run beside
        // this one, so it bounds from below.
        if (turns)
            Assert.True(DurableTransactionMetrics.ScriptActorTurnsCount - before >= 6);
    }

    /// <summary>
    /// The scripts a turn can run, side by side on a node with turns and a node without: everything a caller can
    /// observe is the same, and so is what is left on the key.
    /// </summary>
    [Fact]
    public async Task EveryObservableOutcome_IsTheSameWithAndWithoutTurns()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode withTurns = await StartNode(loggerFactory, turns: true, ct);
        await using EmbeddedKahunaNode without = await StartNode(loggerFactory, turns: false, ct);

        string key = "turns/parity/" + Guid.NewGuid().ToString("N")[..8];

        (string Name, string Script)[] steps =
        [
            ("read of a key that is not there", "LET v = EGET @k\nRETURN v"),
            ("exists on a key that is not there", "LET e = EEXISTS @k\nRETURN e"),
            ("conditional create", "ESET @k 'one' NX\nRETURN 1"),
            ("conditional create that must not overwrite", "LET r = ESET @k 'two' NX\nRETURN 2"),
            ("read back", "LET v = EGET @k\nRETURN v"),
            ("two writes in one script", "ESET @k 'three'\nESET @k 'four'\nRETURN 4"),
            ("write, then a script error", "ESET @k 'lost'\nLET n = to_int('not a number')\nRETURN n"),
            ("write, then throw", "ESET @k 'lost too'\nTHROW 'stop here'"),
            ("the failed writes left nothing", "LET v = EGET @k\nRETURN v"),
            ("extend", "EEXTEND @k 60000\nRETURN 5"),
            ("delete", "EDELETE @k\nRETURN 6"),
            ("read after delete", "LET v = EGET @k\nRETURN v"),
            ("write after delete", "ESET @k 'again'\nRETURN 7")
        ];

        long turnsBefore = DurableTransactionMetrics.ScriptActorTurnsCount;
        long escapesBefore = DurableTransactionMetrics.ScriptActorTurnEscapesCount;
        List<string> notInATurn = [];

        foreach ((string name, string script) in steps)
        {
            long turnsBeforeStep = DurableTransactionMetrics.ScriptActorTurnsCount;

            KeyValueTransactionResult a = await Run(withTurns.Kahuna, script, KeyParameter(key));

            if (DurableTransactionMetrics.ScriptActorTurnsCount == turnsBeforeStep)
                notInATurn.Add(name);

            KeyValueTransactionResult b = await Run(without.Kahuna, script, KeyParameter(key));

            Assert.True(Observed(a) == Observed(b), $"'{name}' differs: turns {Observed(a)} vs general {Observed(b)}");

            (KeyValueResponseType Type, string? Value, long Revision) storedA = await Read(withTurns.Kahuna, key, ct);
            (KeyValueResponseType Type, string? Value, long Revision) storedB = await Read(without.Kahuna, key, ct);

            Assert.True(storedA == storedB, $"after '{name}' the key differs: turns {storedA} vs general {storedB}");
        }

        long ranInTurns = DurableTransactionMetrics.ScriptActorTurnsCount - turnsBefore;
        long escaped = DurableTransactionMetrics.ScriptActorTurnEscapesCount - escapesBefore;

        // Not every step is a script a turn takes: a turn needs the lock analysis to name exactly one key, and
        // that analysis names none for a script that only asks whether a key exists, or that writes only inside
        // a LET. Those run on the general path on both nodes, which is parity too. The point of the count is
        // that the comparison above was, for most steps, a comparison between the two paths.
        Assert.True(
            ranInTurns >= 10,
            $"expected most scripts to run inside actor turns, saw {ranInTurns} of {steps.Length} ({escaped} escapes); not in a turn: {string.Join(" | ", notInATurn)}");
    }

    /// <summary>
    /// Callers racing one counter admit exactly the budget. A turn serializes each of them on the actor; the
    /// general path serializes them with the key's lock. Either way the count is exact.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RacingCallers_AdmitExactlyTheBudget(bool turns)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, turns, ct);

        string key = "turns/race/" + Guid.NewGuid().ToString("N")[..8];

        const int callers = 40;
        const int budget = 11;

        Task<KeyValueTransactionResult>[] attempts = new Task<KeyValueTransactionResult>[callers];

        for (int i = 0; i < callers; i++)
            attempts[i] = Task.Run(() => RunRetrying(node.Kahuna, CounterScript, CounterParameters(key, budget)), ct);

        KeyValueTransactionResult[] results = await Task.WhenAll(attempts);

        Assert.All(results, r => Assert.True(r.Type == KeyValueResponseType.Get, $"{r.Type}: {r.Reason}"));
        Assert.Equal(budget, results.Count(r => Encoding.UTF8.GetString(r.Value ?? []) == "1"));
        Assert.Equal((KeyValueResponseType.Get, budget.ToString(), (long)budget - 1), await Read(node.Kahuna, key, ct));
    }

    /// <summary>
    /// A turn running beside general-path transactions on the same key — an explicit BEGIN … COMMIT is never
    /// run in a turn — still shares one budget with them: the turn takes the same lock they take.
    /// </summary>
    [Fact]
    public async Task TurnsAndGeneralPathTransactions_OnOneKey_ShareOneBudget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, turns: true, ct);

        string key = "turns/mixed/" + Guid.NewGuid().ToString("N")[..8];

        // The same decision written as an explicit transaction, which never runs in a turn. RETURN would end an
        // explicit transaction without committing it, so the branches are spelled out instead.
        const string explicitCounter = """
        BEGIN
          LET current = EGET @counter_key
          IF current = null THEN
            ESET @counter_key 1 EX to_int(@expires_ms)
          ELSE
            LET count = to_int(current)
            IF count < to_int(@limit) THEN
              ESET @counter_key count + 1 EX to_int(@expires_ms)
            END
          END
          COMMIT
        END
        """;

        const int budget = 9;

        List<Task<KeyValueTransactionResult>> attempts = [];

        for (int i = 0; i < 16; i++)
        {
            attempts.Add(Task.Run(() => RunRetrying(node.Kahuna, CounterScript, CounterParameters(key, budget)), ct));
            attempts.Add(Task.Run(() => RunRetrying(node.Kahuna, explicitCounter, CounterParameters(key, budget)), ct));
        }

        await Task.WhenAll(attempts);

        // Thirty-two callers, a budget of nine: whoever was admitted, the counter stops at the budget.
        Assert.Equal((KeyValueResponseType.Get, budget.ToString(), (long)budget - 1), await Read(node.Kahuna, key, ct));
    }

    /// <summary>
    /// A key whose exclusive lock another transaction holds refuses the script with the lock's own reason on
    /// either path, and takes the script once the lock is gone.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task KeyLockedByAnotherTransaction_AbortsWithTheLockReason(bool turns)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, turns, ct);

        string key = "turns/locked/" + Guid.NewGuid().ToString("N")[..8];
        HLCTimestamp holder = new(0, 100, 0);

        (KeyValueResponseType locked, _, _, _) = await node.Kahuna.LocateAndTryAcquireExclusiveLock(
            holder, key, 30_000, KeyValueDurability.Ephemeral, ct);

        Assert.Equal(KeyValueResponseType.Locked, locked);

        KeyValueTransactionResult refused = await Run(node.Kahuna, "ESET @k 'mine'\nRETURN 1", KeyParameter(key));

        Assert.Equal(KeyValueResponseType.Aborted, refused.Type);
        Assert.Equal($"Failed to acquire lock: {key} Ephemeral", refused.Reason);

        (KeyValueResponseType released, _) = await node.Kahuna.LocateAndTryReleaseExclusiveLock(holder, key, KeyValueDurability.Ephemeral, ct);
        Assert.Equal(KeyValueResponseType.Unlocked, released);

        KeyValueTransactionResult admitted = await Run(node.Kahuna, "ESET @k 'mine'\nRETURN 1", KeyParameter(key));

        Assert.True(admitted.Type == KeyValueResponseType.Get, $"{admitted.Type}: {admitted.Reason}");
        Assert.Equal((KeyValueResponseType.Get, "mine", 0L), await Read(node.Kahuna, key, ct));
    }

    /// <summary>
    /// A range lock held by another transaction over the key refuses the write where the write is staged, with
    /// the same answer on either path, and leaves the key free.
    /// </summary>
    [Fact]
    public async Task KeyUnderAForeignRangeLock_IsRefusedTheSameWay()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode withTurns = await StartNode(loggerFactory, turns: true, ct);
        await using EmbeddedKahunaNode without = await StartNode(loggerFactory, turns: false, ct);

        string prefix = "turns-rl-" + Guid.NewGuid().ToString("N")[..8];
        string key = prefix + "/25";
        HLCTimestamp foreign = new(0, 100, 0);

        foreach (EmbeddedKahunaNode node in new[] { withTurns, without })
        {
            (KeyValueResponseType locked, _) = await node.Kahuna.LocateAndTryAcquireRangeLock(
                foreign, prefix, prefix + "/10", true, prefix + "/50", false, 30_000,
                KeyValueDurability.Ephemeral, RangeLockMode.Exclusive, ct);

            Assert.Equal(KeyValueResponseType.Locked, locked);
        }

        KeyValueTransactionResult a = await Run(withTurns.Kahuna, "ESET @k 'phantom'\nRETURN 1", KeyParameter(key));
        KeyValueTransactionResult b = await Run(without.Kahuna, "ESET @k 'phantom'\nRETURN 1", KeyParameter(key));

        Assert.True(Observed(a) == Observed(b), $"turns {Observed(a)} vs general {Observed(b)}");
        Assert.NotEqual(KeyValueResponseType.Get, a.Type);

        foreach (EmbeddedKahunaNode node in new[] { withTurns, without })
        {
            await node.Kahuna.LocateAndTryReleaseExclusiveRangeLock(
                foreign, prefix, prefix + "/10", true, prefix + "/50", false, KeyValueDurability.Ephemeral, ct);

            KeyValueTransactionResult after = await RunRetrying(node.Kahuna, "ESET @k 'real'\nRETURN 1", KeyParameter(key));

            Assert.True(after.Type == KeyValueResponseType.Get, $"{after.Type}: {after.Reason}");
            Assert.Equal((KeyValueResponseType.Get, "real", 0L), await Read(node.Kahuna, key, ct));
        }
    }

    /// <summary>
    /// Scripts a turn must not run keep the general path and still work with turns switched on: a script that
    /// sleeps, one that loops, one over two keys, one over a persistent key, and an explicit transaction.
    /// </summary>
    [Fact]
    public async Task ScriptsThatDoNotFitATurn_TakeTheGeneralPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, turns: true, ct);

        string stem = "turns/shapes/" + Guid.NewGuid().ToString("N")[..8];

        KeyValueTransactionResult sleeping = await Run(node.Kahuna, $"ESET `{stem}/s` 'a'\nSLEEP 20\nRETURN 1");
        Assert.True(sleeping.Type == KeyValueResponseType.Get, $"{sleeping.Type}: {sleeping.Reason}");

        KeyValueTransactionResult looping = await Run(node.Kahuna, $"FOR i IN 1..3 DO\n ESET `{stem}/l` i\nEND\nRETURN 1");
        Assert.True(looping.Type == KeyValueResponseType.Get, $"{looping.Type}: {looping.Reason}");

        KeyValueTransactionResult twoKeys = await Run(node.Kahuna, $"ESET `{stem}/a` '1'\nLET x = EGET `{stem}/a`\nESET `{stem}/b` x\nRETURN 1");
        Assert.True(twoKeys.Type == KeyValueResponseType.Get, $"{twoKeys.Type}: {twoKeys.Reason}");

        KeyValueTransactionResult persistent = await RetryOnMustRetry(
            node.Kahuna, Encoding.UTF8.GetBytes($"SET `{stem}/p` '3'\nLET y = GET `{stem}/p`\nRETURN y"), null, null);
        Assert.True(persistent.Type == KeyValueResponseType.Get, $"{persistent.Type}: {persistent.Reason}");

        KeyValueTransactionResult explicitTx = await Run(node.Kahuna, $"BEGIN ESET `{stem}/e` 'x' COMMIT END");
        Assert.True(explicitTx.Type == KeyValueResponseType.Set, $"{explicitTx.Type}: {explicitTx.Reason}");

        Assert.Equal((KeyValueResponseType.Get, "a", 0L), await Read(node.Kahuna, stem + "/s", ct));
        Assert.Equal((KeyValueResponseType.Get, "1", 0L), await Read(node.Kahuna, stem + "/b", ct));
        Assert.Equal((KeyValueResponseType.Get, "x", 0L), await Read(node.Kahuna, stem + "/e", ct));
    }

    /// <summary>
    /// On three nodes the key is led by one of them. The leader runs the script in a turn; a follower cannot,
    /// and sends the general path's messages to the leader. One counter, one budget, whichever node is asked.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ThreeNodes_OneBudget_WhicheverNodeIsAsked(bool turns)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, c => c.ScriptActorTurns = turns);

        try
        {
            IKahuna[] fleet = [kahuna1, kahuna2, kahuna3];
            string key = "turns/fleet/" + Guid.NewGuid().ToString("N")[..8];

            List<Task<KeyValueTransactionResult>> attempts = [];

            for (int i = 0; i < 30; i++)
            {
                IKahuna kahuna = fleet[i % fleet.Length];
                attempts.Add(Task.Run(() => RunRetrying(kahuna, CounterScript, CounterParameters(key, 8))));
            }

            KeyValueTransactionResult[] results = await Task.WhenAll(attempts);

            Assert.All(results, r => Assert.True(r.Type == KeyValueResponseType.Get, $"{r.Type}: {r.Reason}"));
            Assert.Equal(8, results.Count(r => Encoding.UTF8.GetString(r.Value ?? []) == "1"));

            foreach (IKahuna kahuna in fleet)
                Assert.Equal((KeyValueResponseType.Get, "8", 7L), await Read(kahuna, key, TestContext.Current.CancellationToken));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
