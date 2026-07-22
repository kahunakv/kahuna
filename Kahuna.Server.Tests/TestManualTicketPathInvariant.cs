using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Guards the invariant that no persistent-bearing transaction settles through the manual two-phase-commit
/// ticket path (the actor <c>TryCommit</c>/<c>TryRollback</c> handlers that issue <c>CommitLogs</c>/
/// <c>RollbackLogs</c>). A crash-atomic (persistent) mutation is finalized through the durable-intent
/// canonical-record path instead, so the node's manual-ticket persistent-settlement count must stay at zero
/// across an all-persistent or mixed transaction — commit and rollback alike. This is the acceptance gate for
/// removing the persistent ticket path: while it stays green, that code is dead for persistent keys.
///
/// A positive-control case drives the manual commit entry point directly with a persistent key and asserts the
/// counter <em>does</em> advance, so a zero elsewhere is proof the path was avoided, not proof the counter is
/// inert.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestManualTicketPathInvariant
{
    private readonly ILoggerFactory loggerFactory;

    public TestManualTicketPathInvariant(ITestOutputHelper outputHelper)
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
        await node.WaitForLeaderForKeyAsync("tk/p", ct);
        return node;
    }

    private static async Task<KeyValueTransactionResult> Run(EmbeddedKahunaNode node, string script) =>
        await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

    private static long ManualSettlements(EmbeddedKahunaNode node) =>
        ((KahunaManager)node.Kahuna).ManualTicketPersistentSettlementCount;

    [Fact]
    public async Task AllPersistentTransaction_Commit_NeverTakesManualTicketPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        long before = ManualSettlements(node);

        // Two persistent keys in one partition and one in another exercises both the single- and many-key
        // commit entries; every one must be finalized through the durable-intent path.
        KeyValueTransactionResult commit = await Run(node,
            "BEGIN SET `orders/a` 'va' SET `orders/b` 'vb' SET `invoices/c` 'vc' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, commit.Type);

        // Proof it took the durable path (a canonical record exists) and never the manual ticket path.
        Assert.True(((KahunaManager)node.Kahuna).DurableTransactionRecordStore.Count > 0);
        Assert.Equal(before, ManualSettlements(node));
    }

    [Fact]
    public async Task AllPersistentTransaction_Rollback_NeverTakesManualTicketPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        long before = ManualSettlements(node);

        KeyValueTransactionResult rolled = await Run(node,
            "BEGIN SET `orders/ra` 'va' SET `orders/rb` 'vb' ROLLBACK END");
        Assert.Equal(KeyValueResponseType.Aborted, rolled.Type); // a script ROLLBACK reports Aborted

        Assert.Equal(before, ManualSettlements(node));

        // The rollback discarded both writes.
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Run(node, "GET `orders/ra`")).Type);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await Run(node, "GET `orders/rb`")).Type);
    }

    [Fact]
    public async Task MixedTransaction_Commit_PersistentSubsetNeverTakesManualTicketPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        long before = ManualSettlements(node);

        // Persistent + ephemeral in one transaction: the persistent key goes durable, the ephemeral key commits
        // in memory — neither touches the manual ticket path.
        KeyValueTransactionResult commit = await Run(node,
            "BEGIN SET `tk/mp` 'pv' ESET `tk/me` 'ev' COMMIT END");
        Assert.Equal(KeyValueResponseType.Set, commit.Type);

        Assert.Equal(before, ManualSettlements(node));
    }

    [Fact]
    public async Task MixedTransaction_Rollback_NeverTakesManualTicketPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        long before = ManualSettlements(node);

        KeyValueTransactionResult rolled = await Run(node,
            "BEGIN SET `tk/rmp` 'pv' ESET `tk/rme` 'ev' ROLLBACK END");
        Assert.Equal(KeyValueResponseType.Aborted, rolled.Type);

        Assert.Equal(before, ManualSettlements(node));
    }

    [Fact]
    public async Task ManualCommitEntry_WithPersistentKey_AdvancesTheCounter()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        long before = ManualSettlements(node);

        // Positive control: drive the manual commit entry directly with a persistent key. There is no prepared
        // state, so the commit does not succeed — but the entry is counted, proving the counter is live and a
        // zero in the cases above is a real absence of the manual path, not an inert probe.
        await node.Kahuna.LocateAndTryCommitMutations(
            HLCTimestamp.Zero, "tk/control", HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.True(ManualSettlements(node) > before);
    }
}
