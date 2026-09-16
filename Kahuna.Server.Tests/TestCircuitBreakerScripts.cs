using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Drives a shared-state circuit breaker built entirely out of script transactions, the way a
/// consumer of the cluster would write one. Three scripts hold the whole breaker: one records an
/// outcome into a shared window, one decides whether a call may proceed, and one settles a probe.
///
/// The point of these tests is the properties a fleet of callers depends on, none of which a
/// single-threaded script exercises:
///
/// - One window is shared, so outcomes recorded through different nodes accumulate together.
/// - An epoch tag freezes the window the moment the breaker opens, so a late outcome cannot
///   reopen or close a breaker that already moved on.
/// - The probe budget is global. Many callers racing a freshly half-open breaker admit exactly
///   the configured number of probes between them, because noticing the open period elapsed and
///   claiming a slot happen inside one transaction.
/// - A probe claim is a key with an expiry rather than a counter, so a caller that dies without
///   settling does not hold its slot forever.
///
/// Every key of one breaker shares the placement group "&lt;scope&gt;", so the whole breaker resolves to
/// one partition and one leader. That is what lets the scripts read one clock through current_time().
/// </summary>
public class TestCircuitBreakerScripts : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestCircuitBreakerScripts(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// Records one outcome and opens the breaker when the window crosses the threshold.
    ///
    /// The threshold is an integer numerator of thousandths and the test multiplies rather than
    /// divides: a ratio written as fails / total would meet a floating-point boundary at exactly
    /// the point the breaker must act on, and would raise a division error on an empty window.
    ///
    /// The window is counted before the new observation is written, then that observation is added
    /// in arithmetic, because a bucket read does not return the transaction's own uncommitted write.
    ///
    /// A non-closed breaker records nothing, so the window that tripped it stays frozen.
    /// </summary>
    private const string RecordScript = """
    BEGIN (locking=pessimistic, timeout=20000)
      LET st = GET @state
      LET g = GET @gen
      LET gen = 0
      IF g != null THEN
        LET gen = to_int(g)
      END
      LET state = "closed"
      IF st != null THEN
        LET state = to_string(st)
      END
      LET verdict = "ignored"
      IF state == "closed" THEN
        LET fmark = concat(to_string(gen), ":f")
        LET smark = concat(to_string(gen), ":s")
        LET mark = smark
        IF @outcome == "f" THEN
          LET mark = fmark
        END
        LET w = GET BY BUCKET @obs
        LET total = 1
        LET fails = 0
        IF @outcome == "f" THEN
          LET fails = 1
        END
        SET @obskey mark EX 60000
        FOR v IN w DO
          IF v == fmark THEN
            LET fails = fails + 1
            LET total = total + 1
          END
          IF v == smark THEN
            LET total = total + 1
          END
        END
        LET verdict = concat("recorded:", concat(to_string(fails), concat("/", to_string(total))))
        IF total >= 4 && fails * 1000 >= 500 * total THEN
          SET @state "open"
          SET @gen to_string(gen + 1)
          SET @openedat to_string(current_time())
          LET verdict = "opened"
        END
      END
      LET answer = verdict
      COMMIT
    END
    """;

    /// <summary>
    /// Decides whether one call may proceed, and answers "closed", "probe" or "rejected".
    ///
    /// The move out of open is lazy: the call that observes the open period elapsed is the call that
    /// takes the first probe slot, with no gap in between for another caller to arrive. A scheduled
    /// transition would let every caller learn about the new half-open state independently and race
    /// for the slots together, which is the herd the half-open state exists to prevent.
    ///
    /// The budget is a set of claim keys rather than a counter. A counter cannot recover from a
    /// caller that claims a slot and then dies, because the decrement never arrives; a claim key
    /// carries its own lease and expires on its own.
    ///
    /// A trailing LET carries the verdict out. RETURN would stop the script before COMMIT ran, and
    /// the transaction would abort.
    /// </summary>
    private const string AdmitScript = """
    BEGIN (locking=pessimistic, timeout=20000)
      LET st = GET @state
      LET state = "closed"
      IF st != null THEN
        LET state = to_string(st)
      END
      LET verdict = "closed"
      IF state == "open" THEN
        LET oa = GET @openedat
        LET openedAt = 0
        IF oa != null THEN
          LET openedAt = to_int(oa)
        END
        IF current_time() - openedAt >= 500 THEN
          SET @state "half"
          SET @probeok "0"
          LET state = "half"
        ELSE
          LET verdict = "rejected"
        END
      END
      IF state == "half" THEN
        LET p = GET BY BUCKET @probes
        IF count(p) < 2 THEN
          SET @probekey "1" EX 5000
          LET verdict = "probe"
        ELSE
          LET verdict = "rejected"
        END
      END
      LET answer = verdict
      COMMIT
    END
    """;

    /// <summary>
    /// Settles one probe: enough successes close the breaker, and a single failure reopens it.
    ///
    /// A missing claim key is the staleness check. A probe that was slow rather than dead loses its
    /// slot when the lease expires, and its result then belongs to a slot someone else now holds.
    /// Counting it would act on a probe that the budget no longer considers running.
    ///
    /// A probe that settles normally releases its slot immediately rather than holding it until the
    /// lease expires: the lease is a backstop for a probe that never reports back, not a timer for
    /// one that takes a while.
    /// </summary>
    private const string SettleScript = """
    BEGIN (locking=pessimistic, timeout=20000)
      LET st = GET @state
      LET state = "closed"
      IF st != null THEN
        LET state = to_string(st)
      END
      LET verdict = "ignored"
      IF state == "half" THEN
        LET claim = GET @probekey
        IF claim == null THEN
          LET verdict = "stale"
        ELSE
          DELETE @probekey
          IF @outcome == "f" THEN
            LET g = GET @gen
            LET gen = 0
            IF g != null THEN
              LET gen = to_int(g)
            END
            SET @state "open"
            SET @gen to_string(gen + 1)
            SET @openedat to_string(current_time())
            LET verdict = "reopened"
          ELSE
            LET ok = GET @probeok
            LET successes = 1
            IF ok != null THEN
              LET successes = to_int(ok) + 1
            END
            IF successes >= 2 THEN
              SET @state "closed"
              SET @probeok "0"
              LET verdict = "recovered"
            ELSE
              SET @probeok to_string(successes)
              LET verdict = "progress"
            END
          END
        END
      END
      LET answer = verdict
      COMMIT
    END
    """;

    /// <summary>
    /// Counts the probe claims that are still live, which is what the admit script compares against
    /// the budget.
    /// </summary>
    private const string ProbeClaimCountScript = """
    BEGIN (locking=pessimistic, timeout=20000)
      LET p = GET BY BUCKET @probes
      LET answer = to_string(count(p))
      COMMIT
    END
    """;

    /// <summary>
    /// Every key of one breaker sits in the placement group named by the scope, so all of them
    /// resolve to one partition. The two buckets stay separate key spaces.
    /// </summary>
    private static List<KeyValueParameter> Keys(string scope, string? obsKey = null, string? probeKey = null, string? outcome = null)
    {
        List<KeyValueParameter> parameters =
        [
            new() { Key = "@state", Value = scope + "|cb/state" },
            new() { Key = "@gen", Value = scope + "|cb/gen" },
            new() { Key = "@openedat", Value = scope + "|cb/openedat" },
            new() { Key = "@probeok", Value = scope + "|cb/probeok" },
            new() { Key = "@obs", Value = scope + "|cb.obs/" },
            new() { Key = "@probes", Value = scope + "|cb.probe/" }
        ];

        // A script cannot build a key name from an expression, so the caller names every key the
        // script touches and gives each observation and each claim its own identifier.
        if (obsKey is not null)
            parameters.Add(new() { Key = "@obskey", Value = scope + "|cb.obs/" + obsKey });

        if (probeKey is not null)
            parameters.Add(new() { Key = "@probekey", Value = scope + "|cb.probe/" + probeKey });

        if (outcome is not null)
            parameters.Add(new() { Key = "@outcome", Value = outcome });

        return parameters;
    }

    /// <summary>
    /// Puts the breaker in the open state with its open period already behind it, so the next
    /// admission attempt is the one that moves it to half-open.
    /// </summary>
    private static async Task SeedOpenAndElapsed(IKahuna kahuna, string scope)
    {
        const string seed = """
        BEGIN (locking=pessimistic, timeout=20000)
          SET @state "open"
          SET @gen "1"
          SET @openedat @openedAtValue
          SET @probeok "0"
          COMMIT
        END
        """;

        List<KeyValueParameter> parameters = Keys(scope);
        parameters.Add(new()
        {
            Key = "@openedAtValue",
            Value = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 5000).ToString()
        });

        KeyValueTransactionResult result = await RunRetrying(kahuna, seed, parameters);
        Assert.True(result.Type == KeyValueResponseType.Set, $"{result.Type}: {result.Reason}");
    }

    private static async Task AssertProbeClaims(IKahuna kahuna, string scope, int expected)
    {
        KeyValueTransactionResult result = await RunRetrying(kahuna, ProbeClaimCountScript, Keys(scope));

        Assert.True(result.Type == KeyValueResponseType.Get, $"{result.Type}: {result.Reason}");
        Assert.Equal(expected.ToString(), Text(result));
    }

    /// <summary>
    /// Neither MustRetry nor Aborted commits anything, so both are safe to run again. The claim key
    /// stays the same across attempts, so a retried admission cannot take two slots.
    /// </summary>
    private static async Task<KeyValueTransactionResult> RunRetrying(IKahuna kahuna, string script, List<KeyValueParameter> parameters)
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

    private static string Text(KeyValueTransactionResult result) => Encoding.UTF8.GetString(result.Value ?? []);

    /// <summary>
    /// Outcomes recorded through three different nodes land in one window, and the breaker opens on
    /// the ratio that window holds rather than on any one node's view of it. Once it opens, the
    /// epoch moves and the frozen window ignores everything that arrives afterwards.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestWindowOpensTheBreaker([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string scope = "svc" + Guid.NewGuid().ToString("N")[..8];
            IKahuna[] fleet = [kahuna1, kahuna2, kahuna3];

            // Three successes and one failure, spread across the fleet. One failure in four is below
            // the threshold, and a breaker with a window of its own on each node would not even have
            // enough observations to evaluate.
            string[] outcomes = ["s", "s", "s", "f"];
            KeyValueTransactionResult result = new() { Type = KeyValueResponseType.Errored };

            for (int i = 0; i < outcomes.Length; i++)
            {
                result = await RunRetrying(
                    fleet[i % fleet.Length],
                    RecordScript,
                    Keys(scope, obsKey: Guid.NewGuid().ToString("N"), outcome: outcomes[i])
                );

                Assert.True(result.Type == KeyValueResponseType.Get, $"{result.Type}: {result.Reason}");
            }

            Assert.Equal("recorded:1/4", Text(result));

            // Two more failures bring the window to three in six, which reaches the threshold exactly.
            result = await RunRetrying(kahuna2, RecordScript, Keys(scope, obsKey: Guid.NewGuid().ToString("N"), outcome: "f"));
            Assert.Equal("recorded:2/5", Text(result));

            result = await RunRetrying(kahuna3, RecordScript, Keys(scope, obsKey: Guid.NewGuid().ToString("N"), outcome: "f"));
            Assert.Equal("opened", Text(result));

            // An open breaker rejects without calling the dependency, and records nothing, so the
            // observations that tripped it cannot be diluted while it waits.
            result = await RunRetrying(kahuna1, AdmitScript, Keys(scope, probeKey: Guid.NewGuid().ToString("N")));
            Assert.Equal("rejected", Text(result));

            result = await RunRetrying(kahuna2, RecordScript, Keys(scope, obsKey: Guid.NewGuid().ToString("N"), outcome: "f"));
            Assert.Equal("ignored", Text(result));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Twenty callers spread over three nodes rush a breaker whose open period just elapsed. The
    /// budget is two, so exactly two probes reach the dependency and the other eighteen are turned
    /// away. A breaker that enforced the budget per node would admit two on every node.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestProbeBudgetIsGlobal([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string scope = "svc" + Guid.NewGuid().ToString("N")[..8];
            IKahuna[] fleet = [kahuna1, kahuna2, kahuna3];

            await SeedOpenAndElapsed(kahuna1, scope);

            Task<KeyValueTransactionResult>[] rush = new Task<KeyValueTransactionResult>[20];

            for (int i = 0; i < rush.Length; i++)
            {
                IKahuna node = fleet[i % fleet.Length];
                rush[i] = RunRetrying(node, AdmitScript, Keys(scope, probeKey: Guid.NewGuid().ToString("N")));
            }

            KeyValueTransactionResult[] results = await Task.WhenAll(rush);

            int admitted = 0;
            int rejected = 0;

            foreach (KeyValueTransactionResult result in results)
            {
                Assert.True(result.Type == KeyValueResponseType.Get, $"{result.Type}: {result.Reason}");

                string verdict = Text(result);

                if (verdict == "probe")
                    admitted++;
                else if (verdict == "rejected")
                    rejected++;
                else
                    Assert.Fail("Unexpected verdict: " + verdict);
            }

            Assert.Equal(2, admitted);
            Assert.Equal(18, rejected);

            // Only the admitted callers hold a slot.
            await AssertProbeClaims(kahuna1, scope, 2);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Walks the recovery path end to end: two claims fill the budget, a claim nobody owns settles
    /// as stale, two successes close the breaker, and a single failure sends a later recovery
    /// attempt straight back to open.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestProbeSettlementClosesAndReopens([CombinatorialValues("memory")] string storage, [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string scope = "svc" + Guid.NewGuid().ToString("N")[..8];

            await SeedOpenAndElapsed(kahuna1, scope);

            // Two slots, two claims, and the third caller finds the budget full.
            string firstProbe = Guid.NewGuid().ToString("N");
            string secondProbe = Guid.NewGuid().ToString("N");

            KeyValueTransactionResult result = await RunRetrying(kahuna1, AdmitScript, Keys(scope, probeKey: firstProbe));
            Assert.Equal("probe", Text(result));

            result = await RunRetrying(kahuna2, AdmitScript, Keys(scope, probeKey: secondProbe));
            Assert.Equal("probe", Text(result));

            result = await RunRetrying(kahuna3, AdmitScript, Keys(scope, probeKey: Guid.NewGuid().ToString("N")));
            Assert.Equal("rejected", Text(result));

            await AssertProbeClaims(kahuna1, scope, 2);

            // A result carrying a claim the budget no longer holds makes no progress toward closing.
            result = await RunRetrying(kahuna3, SettleScript, Keys(scope, probeKey: Guid.NewGuid().ToString("N"), outcome: "s"));
            Assert.Equal("stale", Text(result));

            // One success is progress, and settling hands the slot straight back.
            result = await RunRetrying(kahuna1, SettleScript, Keys(scope, probeKey: firstProbe, outcome: "s"));
            Assert.Equal("progress", Text(result));

            // The second success closes the breaker.
            result = await RunRetrying(kahuna2, SettleScript, Keys(scope, probeKey: secondProbe, outcome: "s"));
            Assert.Equal("recovered", Text(result));

            // Both settled probes released their slots the moment their transactions committed.
            await AssertProbeClaims(kahuna1, scope, 0);

            // A closed breaker admits without taking a slot, and records again into a window that
            // the epoch bump left empty.
            result = await RunRetrying(kahuna3, AdmitScript, Keys(scope, probeKey: Guid.NewGuid().ToString("N")));
            Assert.Equal("closed", Text(result));

            result = await RunRetrying(kahuna3, RecordScript, Keys(scope, obsKey: Guid.NewGuid().ToString("N"), outcome: "s"));
            Assert.Equal("recorded:0/1", Text(result));

            // A single probe failure sends a recovering breaker straight back to open, and the
            // reopened breaker turns the next caller away. The same scope carries this, so the
            // budget it works against is the one the two released slots left empty.
            await SeedOpenAndElapsed(kahuna1, scope);

            string failingProbe = Guid.NewGuid().ToString("N");

            result = await RunRetrying(kahuna1, AdmitScript, Keys(scope, probeKey: failingProbe));
            Assert.Equal("probe", Text(result));

            result = await RunRetrying(kahuna1, SettleScript, Keys(scope, probeKey: failingProbe, outcome: "f"));
            Assert.Equal("reopened", Text(result));

            result = await RunRetrying(kahuna2, AdmitScript, Keys(scope, probeKey: Guid.NewGuid().ToString("N")));
            Assert.Equal("rejected", Text(result));

            await AssertProbeClaims(kahuna1, scope, 0);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
