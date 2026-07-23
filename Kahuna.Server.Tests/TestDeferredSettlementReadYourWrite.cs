using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Guards read-your-own-write over a committed-but-unsettled foreign intent under deferred settlement. When a
/// transaction commits with deferred settlement, its value lingers as a prepared intent until it settles in the
/// background. A later transaction that reads that key, writes it, then reads it again must observe its own staged
/// write — not the foreign committed-but-unsettled intent. The read paths consult a foreign intent before the
/// ordinary MVCC read, so without an explicit guard the foreign intent preempts the reader's own write and serves
/// the prior value (a read-your-own-write violation seen only while the earlier commit is still unsettled).
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDeferredSettlementReadYourWrite
{
    private readonly ILoggerFactory loggerFactory;

    public TestDeferredSettlementReadYourWrite(ITestOutputHelper outputHelper)
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
            InitialPartitions = 4,
            // The scenario only manifests while a committed value is still unsettled, so exercise deferred settlement
            // explicitly rather than depending on the node default.
            DurableDeferredSettlement = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("ryow", ct);
        return node;
    }

    // A read-modify-write over a same-key committed-but-unsettled intent can transiently return the retryable
    // MustRetry (its prepare meets the prior commit's still-live intent) until settlement frees the key; the write
    // lands on retry. Retry the whole self-contained script, keeping the caller's strict assertions intact.
    private static async Task<KeyValueTransactionResult> Run(EmbeddedKahunaNode node, string script)
    {
        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        for (int attempt = 1; result.Type == KeyValueResponseType.MustRetry && attempt < 40; attempt++)
        {
            await Task.Delay(Math.Min(5 * attempt, 50));
            result = await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
        }

        return result;
    }

    [Fact]
    public async Task ReadModifyWrite_OverUnsettledCommit_SeesOwnWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        // First transaction commits ryow=100 at revision 0; under deferred settlement its value is a prepared intent
        // that has not settled into base MVCC yet when the next transaction runs immediately after.
        KeyValueTransactionResult first = await Run(node, "SET ryow 100\nGET ryow");
        Assert.Equal(KeyValueResponseType.Get, first.Type);
        Assert.Equal(0, first.Revision);

        // Read-modify-write in one transaction: read 100, write 101, read again. The final read must return this
        // transaction's own write (revision 1, value 101), not the still-unsettled prior commit (revision 0, 100).
        KeyValueTransactionResult second = await Run(node, "LET c = GET ryow\nLET n = to_int(c)\nSET ryow n + 1\nGET ryow");
        Assert.Equal(KeyValueResponseType.Get, second.Type);
        Assert.Equal(1, second.Revision);
        Assert.Equal("101"u8.ToArray(), second.Value);

        // The write is durable and settles: a fresh read returns the same committed value.
        KeyValueTransactionResult reread = await Run(node, "GET ryow");
        Assert.Equal(KeyValueResponseType.Get, reread.Type);
        Assert.Equal(1, reread.Revision);
        Assert.Equal("101"u8.ToArray(), reread.Value);
    }

    [Fact]
    public async Task Exists_AfterOwnWrite_OverUnsettledCommit_ReflectsOwnWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        // Commit a value, then in one transaction overwrite it and assert the transaction sees its own write via
        // both GET and EXISTS while the prior commit is still unsettled.
        Assert.Equal(KeyValueResponseType.Get, (await Run(node, "SET ryx 'a'\nGET ryx")).Type);

        KeyValueTransactionResult r = await Run(node, "SET ryx 'b'\nLET e = EXISTS ryx\nGET ryx");
        Assert.Equal(KeyValueResponseType.Get, r.Type);
        Assert.Equal("b"u8.ToArray(), r.Value);
    }
}
