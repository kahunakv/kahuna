using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A bucket scan must agree with a point read of the same key inside one transaction, and it must
/// stop returning a member the moment the transaction that deleted it commits.
///
/// The shape that broke this: a scan runs while the members exist and caches their rows, then two
/// transactions delete members, then the next scan still returns them. The scan was routed by the
/// prefix string with its trailing slash while the members' point operations were routed by the key
/// space without it, so the scan ran on an actor that never received the deletes' commit
/// notifications and kept serving its own stale copies. Every transaction here also writes
/// neighbouring keys of the same key space, as the original consumer did.
/// </summary>
public class TestBucketScanDeleteVisibility : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestBucketScanDeleteVisibility(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>Writes one member under the bucket and two neighbouring keys in the same transaction.</summary>
    private const string ClaimScript = """
    BEGIN (locking=pessimistic, timeout=20000)
      LET st = GET @state
      SET @state "half"
      SET @gen "1"
      SET @member @marker EX 5000
      LET answer = "claimed"
      COMMIT
    END
    """;

    /// <summary>Scans the bucket and reports the member count, the way a budget check does.</summary>
    private const string CountScript = """
    BEGIN (locking=pessimistic, timeout=20000)
      LET p = GET BY BUCKET @bucket
      LET answer = to_string(count(p))
      COMMIT
    END
    """;

    /// <summary>Deletes one member and writes two neighbouring keys in the same transaction.</summary>
    private const string ReleaseScript = """
    BEGIN (locking=pessimistic, timeout=20000)
      LET st = GET @state
      LET claim = GET @member
      LET answer = "stale"
      IF claim != null THEN
        DELETE @member
        SET @probeok "1"
        SET @state "half"
        LET answer = "released"
      END
      COMMIT
    END
    """;

    /// <summary>Scans the bucket and point-reads two named members in the same transaction.</summary>
    private const string ScanAndReadScript = """
    BEGIN (locking=pessimistic, timeout=20000)
      LET p = GET BY BUCKET @bucket
      LET a = GET @first
      LET b = GET @second
      LET av = "gone"
      IF a != null THEN
        LET av = to_string(a)
      END
      LET bv = "gone"
      IF b != null THEN
        LET bv = to_string(b)
      END
      LET members = ""
      FOR v IN p DO
        LET members = concat(members, concat(to_string(v), ","))
      END
      LET answer = concat(to_string(count(p)), concat("|", concat(av, concat("|", concat(bv, concat("|", members))))))
      COMMIT
    END
    """;

    private static List<KeyValueParameter> Keys(string scope, string bucketSuffix, string? member = null, string? marker = null, string? first = null, string? second = null)
    {
        List<KeyValueParameter> parameters =
        [
            new() { Key = "@state", Value = scope + "|cb/state" },
            new() { Key = "@gen", Value = scope + "|cb/gen" },
            new() { Key = "@probeok", Value = scope + "|cb/probeok" },
            new() { Key = "@bucket", Value = scope + bucketSuffix }
        ];

        if (member is not null)
            parameters.Add(new() { Key = "@member", Value = scope + "|cb.probe/" + member });

        if (marker is not null)
            parameters.Add(new() { Key = "@marker", Value = marker });

        if (first is not null)
            parameters.Add(new() { Key = "@first", Value = scope + "|cb.probe/" + first });

        if (second is not null)
            parameters.Add(new() { Key = "@second", Value = scope + "|cb.probe/" + second });

        return parameters;
    }

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
    /// Four members are claimed through transactions that also write neighbouring keys, a scan counts
    /// them, two of them are released the same way, and the very next transaction scans the bucket and
    /// point-reads one released and one surviving member. The scan must return exactly the two survivors,
    /// and the point reads in that same transaction must agree with it.
    ///
    /// The bucket is spelled three ways: the key space with a trailing slash, the bare key space, and a
    /// partial prefix that names no key space at all. The first two must route to the actor that owns the
    /// members; the third routes elsewhere and must not cache rows it does not own.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestScanExcludesDeletedMembersAndAgreesWithPointReads(
        [CombinatorialValues("memory", "rocksdb")] string storage,
        [CombinatorialValues("|cb.probe/", "|cb.probe", "|cb.pro")] string bucketSuffix)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, 4, raftLogger, kahunaLogger);

        try
        {
            IKahuna[] fleet = [kahuna1, kahuna2, kahuna3];

            // Each round uses a fresh scope, so the prefix and the key space land on different actors
            // in most rounds when routing disagrees, and the failure cannot hide behind one lucky hash.
            for (int round = 0; round < 4; round++)
            {
                string scope = "svc" + Guid.NewGuid().ToString("N")[..8];
                string[] members = new string[4];

                for (int i = 0; i < members.Length; i++)
                {
                    members[i] = Guid.NewGuid().ToString("N");

                    KeyValueTransactionResult claim = await RunRetrying(
                        fleet[i % fleet.Length], ClaimScript, Keys(scope, bucketSuffix, member: members[i], marker: "m" + i));

                    Assert.True(claim.Type == KeyValueResponseType.Get, $"{claim.Type}: {claim.Reason}");
                    Assert.Equal("claimed", Text(claim));
                }

                // The scan that caches the members' rows on whichever actor serves the prefix.
                KeyValueTransactionResult count = await RunRetrying(kahuna1, CountScript, Keys(scope, bucketSuffix));
                Assert.True(count.Type == KeyValueResponseType.Get, $"{count.Type}: {count.Reason}");
                Assert.Equal("4", Text(count));

                // Release two members through two different nodes, then scan at once.
                KeyValueTransactionResult release = await RunRetrying(kahuna2, ReleaseScript, Keys(scope, bucketSuffix, member: members[0]));
                Assert.Equal("released", Text(release));

                release = await RunRetrying(kahuna3, ReleaseScript, Keys(scope, bucketSuffix, member: members[1]));
                Assert.Equal("released", Text(release));

                KeyValueTransactionResult scan = await RunRetrying(
                    kahuna1, ScanAndReadScript, Keys(scope, bucketSuffix, first: members[0], second: members[2]));

                Assert.True(scan.Type == KeyValueResponseType.Get, $"{scan.Type}: {scan.Reason}");

                string[] parts = Text(scan).Split('|');

                Assert.Equal("gone", parts[1]);
                Assert.Equal("m2", parts[2]);
                Assert.True(parts[0] == "2", $"round {round}: scan returned {parts[0]} members ({parts[3]}) but only m2,m3 survive");
                Assert.DoesNotContain("m0,", parts[3]);
                Assert.DoesNotContain("m1,", parts[3]);

                // A plain count right after agrees too, from every node.
                foreach (IKahuna node in fleet)
                {
                    count = await RunRetrying(node, CountScript, Keys(scope, bucketSuffix));
                    Assert.Equal("2", Text(count));
                }
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
