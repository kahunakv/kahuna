using System.Text;
using Kahuna;
using Kahuna.Client;
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
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
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

    private static KahunaClient Connect(EmbeddedKahunaNode node) =>
        new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

    [Fact]
    public async Task MixedTransaction_DurableConflict_AbortsAndRollsBackEphemeral()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaClient client = Connect(node);

        string read = "mix/occ/read/" + Guid.NewGuid().ToString("N")[..8];
        string pWrite = "mix/occ/p/" + Guid.NewGuid().ToString("N")[..8];
        string eWrite = "mix/occ/e/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue(read, "v0", cancellationToken: ct);

        await using KahunaTransactionSession tx = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

        // A latest read records a validated read dependency for the optimistic transaction.
        KahunaKeyValue observed = await tx.GetKeyValue(read, KeyValueDurability.Persistent, ct);
        Assert.Equal("v0", observed.ValueAsString());

        // A concurrent committed write invalidates that dependency.
        await client.SetKeyValue(read, "v1", cancellationToken: ct);

        // Mixed writes: a persistent key (durable) and an ephemeral key.
        await tx.SetKeyValue(pWrite, "pw", durability: KeyValueDurability.Persistent, cancellationToken: ct);
        await tx.SetKeyValue(eWrite, "ew", durability: KeyValueDurability.Ephemeral, cancellationToken: ct);

        // Commit: the persistent subset's durable finalize fails read-set validation → conflict abort, which must
        // drive the ephemeral subset's rollback — neither key commits.
        KahunaException aborted = await Assert.ThrowsAsync<KahunaException>(async () => await tx.Commit(ct));
        Assert.Equal(KeyValueResponseType.Aborted, aborted.KeyValueErrorCode);

        (KeyValueResponseType pType, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, pWrite, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, pType);

        (KeyValueResponseType eType, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, eWrite, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, eType);
    }

    [Fact]
    public async Task MixedTransaction_Commit_AppliesBothPersistentAndEphemeral()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        KeyValueTransactionResult commit = await Run(node,
            "BEGIN SET `mix/p` 'pv' ESET `mix/e` 'ev' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, commit.Type);

        // The persistent subset was finalized through the durable-intent path (a canonical record exists — the
        // ticket path leaves none), while the ephemeral key committed in memory.
        Assert.True(((KahunaManager)node.Kahuna).DurableTransactionRecordStore.Count > 0);

        // Persistent key committed (read through the persistent store).
        KeyValueTransactionResult p = await Run(node, "GET `mix/p`");
        Assert.Equal(KeyValueResponseType.Get, p.Type);
        Assert.Equal("pv"u8.ToArray(), p.Values![0].Value);

        // Ephemeral key committed (read through the ephemeral store).
        KeyValueTransactionResult e = await Run(node, "EGET `mix/e`");
        Assert.Equal(KeyValueResponseType.Get, e.Type);
        Assert.Equal("ev"u8.ToArray(), e.Values![0].Value);

        // Locks on both key classes were released by the commit — a fresh transaction writing each succeeds
        // (a held lock on either would block or abort it).
        KeyValueTransactionResult reP = await Run(node, "BEGIN SET `mix/p` 'pv2' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, reP.Type);
        KeyValueTransactionResult reE = await Run(node, "BEGIN ESET `mix/e` 'ev2' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, reE.Type);
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
