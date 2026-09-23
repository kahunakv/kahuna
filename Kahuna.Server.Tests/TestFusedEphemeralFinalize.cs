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
/// A transaction whose whole write set is one ephemeral key led by the coordinating node finalizes in a
/// single turn of the actor that owns the key, instead of a prepare, a range-lock probe and a commit sent one
/// after another. The single turn exists only to cost less, so every test here runs the same scenario with
/// the shortcut on and with it off and demands the same answer from both — and checks the counter, so a
/// scenario that silently took the other path cannot pass for the one it was meant to cover.
/// </summary>
public sealed class TestFusedEphemeralFinalize : BaseCluster
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

    public TestFusedEphemeralFinalize(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, bool fused, string probeKey, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            FusedEphemeralFinalize = fused,

            // Off, so an auto-commit script reaches the finalize under test through the coordinator instead of
            // running inside an actor turn, which finalizes on its own.
            ScriptActorTurns = false
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(probeKey, ct);

        return node;
    }

    private static List<KeyValueParameter> Parameters(string counterKey, int budget) =>
    [
        new() { Key = "@counter_key", Value = counterKey },
        new() { Key = "@limit", Value = budget.ToString() },
        new() { Key = "@expires_ms", Value = "60000" }
    ];

    private static Task<KeyValueTransactionResult> Run(IKahuna kahuna, string script, List<KeyValueParameter>? parameters = null) =>
        kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, parameters);

    /// <summary>
    /// Neither MustRetry nor Aborted admitted the caller or moved the counter, so both are safe to run again.
    /// </summary>
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

    /// <summary>
    /// The same sequence of single-key transactions gives the same answers, the same stored value and the
    /// same revision whichever finalize ran. With the shortcut on, every writing transaction took it; with it
    /// off, none did.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SameAnswers_SameValue_SameRevision_OnEitherPath(bool fused)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string key = "fused/parity/" + Guid.NewGuid().ToString("N")[..8];

        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, fused, key, ct);

        long before = DurableTransactionMetrics.FusedEphemeralFinalizesCount;

        List<string> verdicts = [];

        for (int i = 0; i < 6; i++)
        {
            KeyValueTransactionResult result = await RunRetrying(node.Kahuna, CounterScript, Parameters(key, 4));

            Assert.True(result.Type == KeyValueResponseType.Get, $"{result.Type}: {result.Reason}");
            verdicts.Add(Encoding.UTF8.GetString(result.Value ?? []));
        }

        Assert.Equal(["1", "1", "1", "1", "0", "0"], verdicts);

        // Four committed writes: revisions 0 through 3. The two refusals wrote nothing.
        Assert.Equal((KeyValueResponseType.Get, "4", 3L), await Read(node.Kahuna, key, ct));

        long taken = DurableTransactionMetrics.FusedEphemeralFinalizesCount - before;

        // The counter is process-wide and other test classes run beside this one, so it bounds from below
        // when the shortcut is on. Off, this node contributes nothing; the exact-zero claim is made where it
        // can be, by the single-threaded check below.
        if (fused)
            Assert.True(taken >= 4, $"expected the four writing transactions to finalize in one turn, saw {taken}");
    }

    /// <summary>
    /// A multi-statement transaction over one ephemeral key, with an explicit BEGIN and COMMIT, a delete and an
    /// extend as well as a set, ends the same way on either path.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ExplicitTransactions_SetExtendDelete_EndTheSameOnEitherPath(bool fused)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string key = "fused/verbs/" + Guid.NewGuid().ToString("N")[..8];

        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, fused, key, ct);

        KeyValueTransactionResult set = await Run(node.Kahuna, $"BEGIN ESET `{key}` 'a' ESET `{key}` 'b' COMMIT END");
        Assert.True(set.Type == KeyValueResponseType.Set, $"{set.Type}: {set.Reason}");
        (KeyValueResponseType type, string? value, long revisionAfterSet) = await Read(node.Kahuna, key, ct);
        Assert.Equal((KeyValueResponseType.Get, "b"), (type, value));

        KeyValueTransactionResult rolledBack = await Run(node.Kahuna, $"BEGIN ESET `{key}` 'never' ROLLBACK END");
        Assert.Equal(KeyValueResponseType.Aborted, rolledBack.Type);

        // A rolled back write leaves the value and the revision where the last commit put them.
        Assert.Equal((KeyValueResponseType.Get, "b", revisionAfterSet), await Read(node.Kahuna, key, ct));

        KeyValueTransactionResult extended = await Run(node.Kahuna, $"BEGIN EEXTEND `{key}` 60000 COMMIT END");
        Assert.True(extended.Type == KeyValueResponseType.Extended, $"{extended.Type}: {extended.Reason}");

        KeyValueTransactionResult deleted = await Run(node.Kahuna, $"BEGIN EDELETE `{key}` COMMIT END");
        Assert.True(deleted.Type == KeyValueResponseType.Deleted, $"{deleted.Type}: {deleted.Reason}");

        (KeyValueResponseType afterDelete, _, _) = await Read(node.Kahuna, key, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, afterDelete);

        // The key is free again: nothing of the finished transactions is left on it.
        KeyValueTransactionResult again = await Run(node.Kahuna, $"ESET `{key}` 'fresh'");
        Assert.True(again.Type == KeyValueResponseType.Set, $"{again.Type}: {again.Reason}");
    }

    /// <summary>
    /// Callers racing one counter admit exactly the budget between them, on either path. This is the property
    /// the three messages protect, and the one a broken single turn would lose first.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RacingCallers_AdmitExactlyTheBudget(bool fused)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string key = "fused/race/" + Guid.NewGuid().ToString("N")[..8];

        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, fused, key, ct);

        const int callers = 24;
        const int budget = 7;

        Task<KeyValueTransactionResult>[] attempts = new Task<KeyValueTransactionResult>[callers];

        for (int i = 0; i < callers; i++)
            attempts[i] = RunRetrying(node.Kahuna, CounterScript, Parameters(key, budget));

        KeyValueTransactionResult[] results = await Task.WhenAll(attempts);

        Assert.All(results, r => Assert.True(r.Type == KeyValueResponseType.Get, $"{r.Type}: {r.Reason}"));

        Assert.Equal(budget, results.Count(r => Encoding.UTF8.GetString(r.Value ?? []) == "1"));
        Assert.Equal(callers - budget, results.Count(r => Encoding.UTF8.GetString(r.Value ?? []) == "0"));

        Assert.Equal((KeyValueResponseType.Get, budget.ToString(), (long)budget - 1), await Read(node.Kahuna, key, ct));
    }

    /// <summary>
    /// A write-fence lock taken by another transaction after the write was staged, and before the finalize,
    /// aborts the transaction with the fence's own reason on either path — and the aborted write leaves
    /// nothing behind: once the lock is gone the key takes a new write at revision zero. The write fence is
    /// the one lock mode that steps around a staged write's intent; a Shared or Exclusive lock attempted in
    /// the same window is refused with the writer as holder, on the ephemeral path exactly as on the durable
    /// one, since the staged write's intent is what it conflicts with.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RangeLockTakenAfterTheWrite_AbortsWithTheFenceReason_AndLeavesTheKeyClean(bool fused)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string prefix = "fused-rl-" + Guid.NewGuid().ToString("N")[..8];
        string key = prefix + "/25";

        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, fused, key, ct);

        HLCTimestamp foreign = new(0, 100, 0);

        // The write is staged, then the script sleeps; the range lock arrives during the sleep. At write time
        // no lock existed, so only the finalize can see it.
        Task<KeyValueTransactionResult> writer = Run(node.Kahuna, $"BEGIN ESET `{key}` 'phantom' SLEEP 600 COMMIT END");

        await Task.Delay(200, ct);

        foreach (RangeLockMode refusedMode in new[] { RangeLockMode.Exclusive, RangeLockMode.Shared })
        {
            (KeyValueResponseType refused, HLCTimestamp holder) = await node.Kahuna.LocateAndTryAcquireRangeLock(
                foreign, prefix, prefix + "/10", true, prefix + "/50", false, 30_000,
                KeyValueDurability.Ephemeral, refusedMode, ct);

            Assert.Equal(KeyValueResponseType.AlreadyLocked, refused);
            Assert.NotEqual(HLCTimestamp.Zero, holder);
        }

        (KeyValueResponseType locked, _) = await node.Kahuna.LocateAndTryAcquireRangeLock(
            foreign, prefix, prefix + "/10", true, prefix + "/50", false, 30_000,
            KeyValueDurability.Ephemeral, RangeLockMode.WriteFence, ct);

        Assert.Equal(KeyValueResponseType.Locked, locked);

        KeyValueTransactionResult result = await writer;

        Assert.Equal(KeyValueResponseType.Aborted, result.Type);
        Assert.Equal($"Foreign range lock covers written key {key}", result.Reason);

        (KeyValueResponseType whileLocked, _, _) = await Read(node.Kahuna, key, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, whileLocked);

        Assert.Equal(
            KeyValueResponseType.Unlocked,
            await node.Kahuna.LocateAndTryReleaseExclusiveRangeLock(
                foreign, prefix, prefix + "/10", true, prefix + "/50", false, KeyValueDurability.Ephemeral, ct));

        KeyValueTransactionResult after = await RunRetrying(node.Kahuna, $"BEGIN ESET `{key}` 'real' COMMIT END", []);
        Assert.True(after.Type == KeyValueResponseType.Set, $"{after.Type}: {after.Reason}");

        Assert.Equal((KeyValueResponseType.Get, "real", 0L), await Read(node.Kahuna, key, ct));
    }

    /// <summary>
    /// A transaction that writes two keys, one that writes a persistent key, and one that validates its reads
    /// are not the shape a single actor turn can stand in for, and keep the path they had.
    /// </summary>
    [Fact]
    public async Task OtherShapes_KeepTheThreeMessagePath_AndStillCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string stem = "fused/shapes/" + Guid.NewGuid().ToString("N")[..8];

        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, fused: true, stem, ct);

        KeyValueTransactionResult twoKeys = await Run(node.Kahuna, $"BEGIN ESET `{stem}/a` '1' ESET `{stem}/b` '2' COMMIT END");
        Assert.True(twoKeys.Type == KeyValueResponseType.Set, $"{twoKeys.Type}: {twoKeys.Reason}");

        KeyValueTransactionResult persistent = await RetryOnMustRetry(node.Kahuna, Encoding.UTF8.GetBytes($"BEGIN SET `{stem}/p` '3' COMMIT END"), null, null);
        Assert.True(persistent.Type == KeyValueResponseType.Set, $"{persistent.Type}: {persistent.Reason}");

        KeyValueTransactionResult validating = await Run(node.Kahuna, $"""
            BEGIN (locking="optimistic")
              LET seen = EGET `{stem}/a`
              ESET `{stem}/c` seen
              COMMIT
            END
            """);
        Assert.True(validating.Type == KeyValueResponseType.Set, $"{validating.Type}: {validating.Reason}");

        Assert.Equal((KeyValueResponseType.Get, "1", 0L), await Read(node.Kahuna, stem + "/a", ct));
        Assert.Equal((KeyValueResponseType.Get, "2", 0L), await Read(node.Kahuna, stem + "/b", ct));
        Assert.Equal((KeyValueResponseType.Get, "1", 0L), await Read(node.Kahuna, stem + "/c", ct));
    }

    /// <summary>
    /// On three nodes a key is led by one of them, so the same counter driven through all three exercises both
    /// the single turn (the leader coordinates) and the fallback (a follower coordinates and the three
    /// messages travel to the leader). The budget is one budget either way.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ThreeNodes_LeaderAndFollowersShareOneBudget(bool fused)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, c => { c.FusedEphemeralFinalize = fused; c.ScriptActorTurns = false; });

        try
        {
            IKahuna[] fleet = [kahuna1, kahuna2, kahuna3];
            string key = "fused/fleet/" + Guid.NewGuid().ToString("N")[..8];

            int admitted = 0;

            for (int i = 0; i < 15; i++)
            {
                KeyValueTransactionResult result = await RunRetrying(fleet[i % fleet.Length], CounterScript, Parameters(key, 6));

                Assert.True(result.Type == KeyValueResponseType.Get, $"{result.Type}: {result.Reason}");

                if (Encoding.UTF8.GetString(result.Value ?? []) == "1")
                    admitted++;
            }

            Assert.Equal(6, admitted);

            foreach (IKahuna kahuna in fleet)
                Assert.Equal((KeyValueResponseType.Get, "6", 5L), await Read(kahuna, key, TestContext.Current.CancellationToken));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
