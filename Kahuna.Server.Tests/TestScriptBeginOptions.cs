using System.Text;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The BEGIN option list of a script transaction, driven through the real script entry point: which names it
/// accepts, the policies it can set beyond locking and timing, and the server limits it is held to.
/// </summary>
public sealed class TestScriptBeginOptions
{
    private readonly ILoggerFactory loggerFactory;

    public TestScriptBeginOptions(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaOptions BaseOptions() => new()
    {
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        PartitionExecutorPoolSize = 1,
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 4
    };

    private static Task<KeyValueTransactionResult> Run(EmbeddedKahunaNode node, string script)
        => node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

    private static async Task<string> AssertErrored(EmbeddedKahunaNode node, string script)
    {
        KeyValueTransactionResult resp = await Run(node, script);

        Assert.Equal(KeyValueResponseType.Errored, resp.Type);
        Assert.False(string.IsNullOrEmpty(resp.Reason), "an errored script must carry a reason");

        return resp.Reason!;
    }

    private static async Task AssertValue(EmbeddedKahunaNode node, string key, string expected)
    {
        KeyValueTransactionResult resp = await Run(node, $"GET `{key}`");

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(expected, Encoding.UTF8.GetString(resp.Value ?? []));
    }

    /// <summary>
    /// A name no option answers to used to be dropped, so a typo or a wrong case ran the transaction on the
    /// default for that option with no sign anything was wrong.
    /// </summary>
    [Fact]
    public async Task UnknownOptionNames_AreRefused()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(BaseOptions(), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("names/k", ct);

        string reason = await AssertErrored(node, "BEGIN (lockng=optimistic) SET `names/k` 'v' COMMIT END");
        Assert.Contains("Unknown BEGIN option: lockng", reason, StringComparison.Ordinal);

        // Names are case-sensitive, like their values, so a miscased name is unknown rather than a match.
        reason = await AssertErrored(node, "BEGIN (Locking=optimistic) SET `names/k` 'v' COMMIT END");
        Assert.Contains("Unknown BEGIN option: Locking", reason, StringComparison.Ordinal);

        // An unknown name in a later position is refused too, not only the first one.
        reason = await AssertErrored(node, "BEGIN (timeout=5000, conflictPolicy=yield) SET `names/k` 'v' COMMIT END");
        Assert.Contains("Unknown BEGIN option: conflictPolicy", reason, StringComparison.Ordinal);

        // A refused script wrote nothing.
        KeyValueTransactionResult resp = await Run(node, "GET `names/k`");
        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);

        // Every name the executor reads is accepted in one list.
        resp = await Run(node,
            "BEGIN (locking=pessimistic, autoCommit=true, asyncRelease=false, timeout=5000, admissionWait=1000, " +
            "priority=normal, readValidation=trackAndValidate, decisionDurability=durable) SET `names/k` 'v' END");
        Assert.Equal(KeyValueResponseType.Set, resp.Type);

        await AssertValue(node, "names/k", "v");
    }

    /// <summary>
    /// readValidation reaches the transaction: both values run, a bad value is refused, and the combination the
    /// interactive path refuses is refused here as well.
    /// </summary>
    [Fact]
    public async Task ReadValidation_IsAcceptedAndValidated()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(BaseOptions(), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("rv/a", ct);

        Assert.Equal(KeyValueResponseType.Set, (await Run(node, "SET `rv/a` 'base'")).Type);

        foreach (string value in new[] { "none", "trackAndValidate" })
        {
            foreach (string locking in new[] { "pessimistic", "optimistic" })
            {
                KeyValueTransactionResult resp = await Run(node, $"""
                    BEGIN (locking={locking}, readValidation={value})
                     LET a = GET `rv/a`
                     SET `rv/b` a
                     COMMIT
                    END
                    """);

                Assert.Equal(KeyValueResponseType.Set, resp.Type);
            }
        }

        await AssertValue(node, "rv/b", "base");

        string reason = await AssertErrored(node, "BEGIN (readValidation=always) SET `rv/b` 'v' COMMIT END");
        Assert.Contains("Unsupported readValidation option: always", reason, StringComparison.Ordinal);

        // A read pinned to a past snapshot cannot detect a later write, so validating it would be a false
        // promise.
        reason = await AssertErrored(node, "BEGIN (snapshot=1, readValidation=trackAndValidate) GET `rv/a` END");
        Assert.Contains("snapshot cannot be combined with readValidation=trackAndValidate", reason, StringComparison.Ordinal);

        // With validation off the same snapshot is still accepted.
        Assert.NotEqual(KeyValueResponseType.Errored, (await Run(node, "BEGIN (snapshot=1, readValidation=none) GET `rv/a` END")).Type);
    }

    /// <summary>
    /// decisionDurability reaches the transaction. A durable decision cannot cover an ephemeral write, so the
    /// refusal of an ephemeral write proves the option took effect, where a plain commit alone would not.
    /// </summary>
    [Fact]
    public async Task DecisionDurability_IsAcceptedAndReachesTheCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(BaseOptions(), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("dd/k", ct);

        KeyValueTransactionResult resp = await Run(node, "BEGIN (decisionDurability=durable) SET `dd/k` 'durable' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
        await AssertValue(node, "dd/k", "durable");

        resp = await Run(node, "BEGIN (decisionDurability=durable) ESET `dd/e` 'v' COMMIT END");
        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        Assert.Contains("A durable transaction cannot modify an ephemeral key", resp.Reason ?? "", StringComparison.Ordinal);

        // The default, and the explicit best-effort value, keep the ephemeral write.
        resp = await Run(node, "BEGIN (decisionDurability=bestEffort) ESET `dd/e` 'v' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, resp.Type);

        resp = await Run(node, "BEGIN ESET `dd/e` 'v2' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, resp.Type);

        string reason = await AssertErrored(node, "BEGIN (decisionDurability=strict) SET `dd/k` 'v' COMMIT END");
        Assert.Contains("Unsupported decisionDurability option: strict", reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// A script timeout used to run for whatever it asked, so a script could hold its locks and admission slot
    /// far beyond the limit an interactive transaction is held to.
    /// </summary>
    [Fact]
    public async Task Timeout_IsClampedToTheServerMaximum()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        EmbeddedKahunaOptions options = BaseOptions();
        options.DefaultTransactionTimeout = 300;
        options.MaxTransactionTimeout = 300;

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("clamp/k", ct);

        // Asks for a minute, sleeps well past the maximum, and is ended by the maximum.
        KeyValueTransactionResult resp = await Run(node, "BEGIN (timeout=60000) SLEEP 1500 SET `clamp/k` 'v' COMMIT END");

        Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
        Assert.Equal("Transaction aborted by timeout", resp.Reason);

        // A transaction that fits under the maximum still commits.
        resp = await Run(node, "BEGIN (timeout=60000) SET `clamp/k` 'v' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, resp.Type);
    }
}
