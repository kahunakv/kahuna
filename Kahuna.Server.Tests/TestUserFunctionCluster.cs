using System.Text;

using Kommander;
using Microsoft.Extensions.Logging;

using Kahuna.Extensibility;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The cluster half of the user-defined function contract: the registry is per process and is never
/// replicated, so these tests pin down what a node that does not have a function can and cannot do.
///
/// <para>The answer the design turns on is that such a node is still a correct follower. What Raft
/// carries is the value the function produced, never the call, so every node applies the write and
/// serves the result. A node only refuses to <em>coordinate</em> a script that calls what it lacks.
/// If that ever stopped being true, a rolling deployment that adds a function would corrupt data
/// instead of returning an error.</para>
/// </summary>
public class TestUserFunctionCluster : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestUserFunctionCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string RandomKey() => "fn" + Guid.NewGuid().ToString("N")[..10];

    private static async Task<KeyValueTransactionResult> RunAsync(IKahuna kahuna, string script)
    {
        return await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
    }

    private static string TextOf(KeyValueTransactionResult result) => Encoding.UTF8.GetString(result.Value ?? []);

    [Fact]
    public async Task TestOnlyTheRegisteringNodeCoordinatesButEveryNodeServesTheResult()
    {
        // Each node is built with its own KahunaConfiguration, and the three are built in order, so
        // the first invocation of this callback is node 1. Only it gets the function.
        int built = 0;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, configuration =>
            {
                if (Interlocked.Increment(ref built) == 1)
                    configuration.Functions.Register(
                        "acme_stamp",
                        static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From("stamped:" + args[0].AsString()),
                        1,
                        1);
            });

        try
        {
            string key = RandomKey();

            KeyValueTransactionResult written = await RunAsync(kahuna1, $"BEGIN SET `{key}` acme_stamp('one') COMMIT END");

            Assert.True(written.Type == KeyValueResponseType.Set, $"{written.Type}: {written.Reason}");

            // Node 2 and node 3 never registered the function, yet they read the value it produced:
            // replication carried the result, not the call.
            Assert.Equal("stamped:one", TextOf(await RunAsync(kahuna1, $"GET `{key}`")));
            Assert.Equal("stamped:one", TextOf(await RunAsync(kahuna2, $"GET `{key}`")));
            Assert.Equal("stamped:one", TextOf(await RunAsync(kahuna3, $"GET `{key}`")));

            // Coordinating the same script on a node without the function is a deterministic Errored,
            // and the message names the node and its fingerprint so the wrong node is obvious.
            KeyValueTransactionResult refused = await RunAsync(kahuna2, $"BEGIN SET `{key}` acme_stamp('two') COMMIT END");

            Assert.Equal(KeyValueResponseType.Errored, refused.Type);
            Assert.Contains("acme_stamp", refused.Reason ?? "", StringComparison.Ordinal);
            Assert.Contains("kahuna2", refused.Reason ?? "", StringComparison.Ordinal);

            // Never MustRetry: a client must not spin on a failure that repeats on every attempt.
            Assert.NotEqual(KeyValueResponseType.MustRetry, refused.Type);
            Assert.NotEqual(KeyValueResponseType.Aborted, refused.Type);

            // The refusal wrote nothing.
            Assert.Equal("stamped:one", TextOf(await RunAsync(kahuna1, $"GET `{key}`")));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Fact]
    public async Task TestNodesWithDifferentRegistriesReportDifferentFingerprints()
    {
        int built = 0;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, configuration =>
            {
                if (Interlocked.Increment(ref built) <= 2)
                    configuration.Functions.Register(
                        "acme_stamp",
                        static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From(args[0].AsString()),
                        1,
                        1);
            });

        try
        {
            string one = ((KahunaManager)kahuna1).UserFunctionFingerprint;
            string two = ((KahunaManager)kahuna2).UserFunctionFingerprint;
            string three = ((KahunaManager)kahuna3).UserFunctionFingerprint;

            // Two nodes registered the same surface, so they agree. The third did not, so it differs.
            // This is the whole point of the fingerprint: an operator diffs it instead of guessing.
            Assert.Equal(one, two);
            Assert.NotEqual(one, three);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Fact]
    public async Task TestANodeWithoutTheFunctionStillServesEveryBuiltIn()
    {
        int built = 0;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, configuration =>
            {
                if (Interlocked.Increment(ref built) == 1)
                    configuration.Functions.Register(
                        "acme_noop",
                        static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.Null);
            });

        try
        {
            // A node whose table holds a custom entry and one whose table holds none must answer a
            // built-in identically: merging the tables must not change what a built-in does.
            Assert.Equal("5", TextOf(await RunAsync(kahuna1, "RETURN abs(-5)")));
            Assert.Equal("5", TextOf(await RunAsync(kahuna2, "RETURN abs(-5)")));
            Assert.Equal("AB", TextOf(await RunAsync(kahuna1, "RETURN upper(concat('a', 'b'))")));
            Assert.Equal("AB", TextOf(await RunAsync(kahuna2, "RETURN upper(concat('a', 'b'))")));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
