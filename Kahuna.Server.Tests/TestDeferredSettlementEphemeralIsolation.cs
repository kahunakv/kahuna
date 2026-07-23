using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Guards ephemeral/persistent keyspace isolation under deferred settlement. The ephemeral and persistent keyspaces
/// are independent even when a key name is reused across them; a durable transaction can never modify an ephemeral
/// key, so an ephemeral write must ignore any durable prepared intent covering the same key name. Under deferred
/// settlement a persistent commit's value lingers as a committed-but-unsettled prepared intent, and before this fix
/// the shared prepared-intent store let an ephemeral write to the same key name materialize that foreign persistent
/// intent into its own entry and derive its next revision from it — reporting revision 1 for a first ephemeral write
/// that must be revision 0. Ephemeral actors now carry no durable-intent store, so the ephemeral path never consults
/// it. The anomaly only appears while the persistent intent is unsettled, so it exercises deferred settlement.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDeferredSettlementEphemeralIsolation
{
    private readonly ILoggerFactory loggerFactory;

    public TestDeferredSettlementEphemeralIsolation(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, bool deferred, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            DurableDeferredSettlement = deferred
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("pp", ct);
        return node;
    }

    private static async Task<KeyValueTransactionResult> Run(EmbeddedKahunaNode node, string script) =>
        await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

    // Both the synchronous default (deferred = false) and deferred settlement must give the ephemeral first write
    // revision 0, regardless of a same-named persistent key's still-unsettled committed intent.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task EphemeralWrite_SameKeyNameAsUnsettledPersistentIntent_StartsAtRevisionZero(bool deferred)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, deferred, ct);

        // A persistent write commits pp at revision 0. Under deferred settlement its value is a prepared intent that
        // has not settled into base MVCC when the ephemeral write to the same key name runs immediately after.
        KeyValueTransactionResult persistent = await Run(node, "SET pp 'other world'\nGET pp");
        Assert.Equal(KeyValueResponseType.Get, persistent.Type);
        Assert.Equal(0, persistent.Revision);

        // The ephemeral keyspace is independent: a first ephemeral write to the same key name is revision 0. Before
        // the fix it derived revision 1 from the lingering foreign persistent intent (only while deferred-on).
        KeyValueTransactionResult ephemeral = await Run(node, "ESET pp 'other world'\nEGET pp");
        Assert.Equal(KeyValueResponseType.Get, ephemeral.Type);
        Assert.Equal(0, ephemeral.Revision);
        Assert.Equal("other world"u8.ToArray(), ephemeral.Value);
    }
}
