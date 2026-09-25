using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A pessimistic script that scans a bucket and then writes one member of that bucket must commit
/// when nothing competes with it, and exactly one of two such scripts racing over the same bucket
/// must commit.
/// </summary>
public class TestPessimisticBucketScanThenWrite : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestPessimisticBucketScanThenWrite(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private const string Script = """
    BEGIN (locking="pessimistic")
     LET oncall = GET BY BUCKET @prefix
     SLEEP 200
     IF count(oncall) = 2 THEN
      IF to_bool(oncall[0]) THEN
       IF to_bool(oncall[1]) THEN
        SET @doctor false EX 120000
       END
      END
     END
     COMMIT
    END
    """;

    private static List<KeyValueParameter> Parameters(string prefix, string doctor) =>
    [
        new() { Key = "@prefix", Value = prefix },
        new() { Key = "@doctor", Value = doctor }
    ];

    private static async Task Seed(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "true"u8.ToArray(), null, -1, KeyValueFlags.None, 120_000, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task<string?> ReadText(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Get, type);

        return entry?.Value is null ? null : Encoding.UTF8.GetString(entry.Value);
    }

    [Theory, CombinatorialData]
    public async Task TestLoneScriptCommits([CombinatorialValues("memory", "rocksdb")] string storage)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, 4, raftLogger, kahunaLogger);

        try
        {
            IKahuna[] fleet = [kahuna1, kahuna2, kahuna3];

            for (int round = 0; round < 6; round++)
            {
                string prefix = $"tx-anomaly/{Guid.NewGuid():N}/doctors";
                string alice = prefix + "/alice";
                string bob = prefix + "/bob";

                await Seed(kahuna1, alice, ct);
                await Seed(kahuna1, bob, ct);

                KeyValueTransactionResult result = await fleet[round % fleet.Length].TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes(Script), null, Parameters(prefix, alice));

                Assert.True(
                    result.Type is not (KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry or KeyValueResponseType.Errored),
                    $"round {round}: {result.Type}: {result.Reason}");

                Assert.Equal("false", await ReadText(kahuna1, alice, ct));
                Assert.Equal("true", await ReadText(kahuna1, bob, ct));
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestRacingScriptsOneCommits([CombinatorialValues("memory", "rocksdb")] string storage)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, 4, raftLogger, kahunaLogger);

        try
        {
            for (int round = 0; round < 6; round++)
            {
                string prefix = $"tx-anomaly/{Guid.NewGuid():N}/doctors";
                string alice = prefix + "/alice";
                string bob = prefix + "/bob";

                await Seed(kahuna1, alice, ct);
                await Seed(kahuna1, bob, ct);

                Task<KeyValueTransactionResult> aliceTask = kahuna2.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes(Script), null, Parameters(prefix, alice));
                Task<KeyValueTransactionResult> bobTask = kahuna3.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes(Script), null, Parameters(prefix, bob));

                KeyValueTransactionResult[] results = await Task.WhenAll(aliceTask, bobTask);

                int committed = results.Count(r => r.Type is not (KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry or KeyValueResponseType.Errored));

                string? aliceValue = await ReadText(kahuna1, alice, ct);
                string? bobValue = await ReadText(kahuna1, bob, ct);

                Assert.True(aliceValue == "true" || bobValue == "true", $"round {round}: both off call");
                Assert.True(committed >= 1,
                    $"round {round}: nobody committed; alice {results[0].Type}: {results[0].Reason}; bob {results[1].Type}: {results[1].Reason}");
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
