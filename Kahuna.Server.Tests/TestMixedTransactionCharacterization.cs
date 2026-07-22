using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Characterizes a transaction that mixes a persistent write (<c>SET</c>) and an ephemeral write (<c>ESET</c>) in
/// one <c>BEGIN…COMMIT</c> — the case that still routes its persistent key through the manual ticket path. These
/// tests lock the observable commit/rollback behavior as the safety net for migrating that persistent subset onto
/// the durable-intent path (so the ticket path can then be retired). Persistent effects survive via the persistent
/// store (GET); ephemeral effects via the ephemeral store (EGET); a rollback discards both.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestMixedTransactionCharacterization
{
    private readonly ILoggerFactory loggerFactory;

    public TestMixedTransactionCharacterization(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("mix/p", ct);
        return node;
    }

    private static async Task<KeyValueTransactionResult> Run(EmbeddedKahunaNode node, string script) =>
        await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

    [Fact]
    public async Task MixedTransaction_Commit_AppliesBothPersistentAndEphemeral()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        KeyValueTransactionResult commit = await Run(node,
            "BEGIN SET `mix/p` 'pv' ESET `mix/e` 'ev' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, commit.Type);

        // Persistent key committed (read through the persistent store).
        KeyValueTransactionResult p = await Run(node, "GET `mix/p`");
        Assert.Equal(KeyValueResponseType.Get, p.Type);
        Assert.Equal("pv"u8.ToArray(), p.Values![0].Value);

        // Ephemeral key committed (read through the ephemeral store).
        KeyValueTransactionResult e = await Run(node, "EGET `mix/e`");
        Assert.Equal(KeyValueResponseType.Get, e.Type);
        Assert.Equal("ev"u8.ToArray(), e.Values![0].Value);
    }

    [Fact]
    public async Task MixedTransaction_Rollback_DiscardsBoth()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        KeyValueTransactionResult rolled = await Run(node,
            "BEGIN SET `mix/rp` 'pv' ESET `mix/re` 'ev' ROLLBACK END");
        Assert.Equal(KeyValueResponseType.Aborted, rolled.Type); // a script ROLLBACK reports Aborted

        KeyValueTransactionResult p = await Run(node, "GET `mix/rp`");
        Assert.Equal(KeyValueResponseType.DoesNotExist, p.Type);

        KeyValueTransactionResult e = await Run(node, "EGET `mix/re`");
        Assert.Equal(KeyValueResponseType.DoesNotExist, e.Type);
    }
}
