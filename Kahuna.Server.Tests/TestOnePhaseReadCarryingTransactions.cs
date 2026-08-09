using System.Diagnostics.Metrics;
using Kahuna;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Pins the single-process one-phase eligibility rule: on an embedded standalone node
/// (<c>KahunaConfiguration.SingleProcessRaftGroup</c>, set by the in-process constructor), an
/// optimistic transaction whose read set reaches beyond its written keys still commits through the
/// one-phase bundled fast path. On a multi-node group such a transaction must run 2PC — a stalled
/// bundle after a leader change would decide on a long-stale read validation — but in a
/// single-process group that stall cannot exist: an in-flight proposal cannot outlive the process,
/// and a committed bundle replays during restore ahead of any later conflicting write.
///
/// Read validation itself is NOT waived — it still runs before the bundle is proposed, and
/// <c>TestTransactionConcurrencyPolicy.TrackedRead_InvalidatedByConcurrentWrite_AbortsCommit</c>
/// pins that a moved read dependency still aborts the commit on this same topology.
/// </summary>
public sealed class TestOnePhaseReadCarryingTransactions
{
    private readonly ILoggerFactory loggerFactory;

    public TestOnePhaseReadCarryingTransactions(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task ReadCarryingOptimisticTransaction_CommitsOnePhase_OnSingleProcessNode()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string readKey = "op1/read/" + Guid.NewGuid().ToString("N")[..8];
        string writeKey = "op1/write/" + Guid.NewGuid().ToString("N")[..8];
        await client.SetKeyValue(readKey, "v0", cancellationToken: ct);

        // The counters are process-global statics shared with any concurrently running test class,
        // which can only ADD to them — so the assertion is a lower bound over this test's own
        // transactions, never an exact count.
        long onePhase = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (inst, l) =>
        {
            if (inst.Meter.Name == "Kahuna" && inst.Name == "kahuna.durable_tx.one_phase_commits")
                l.EnableMeasurementEvents(inst);
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref onePhase, value));
        listener.Start();

        const int transactions = 8;
        for (int i = 0; i < transactions; i++)
        {
            await using KahunaTransactionSession tx = await client.StartTransactionSession(
                new() { Locking = KeyValueTransactionLocking.Optimistic, ReadValidation = ReadValidation.TrackAndValidate }, ct);

            // A tracked read of a key the transaction never writes: the read set extends beyond the
            // written keys, which on a multi-node group forces 2PC. The write makes the commit durable.
            KahunaKeyValue observed = await tx.GetKeyValue(readKey, KeyValueDurability.Persistent, ct);
            Assert.True(observed.Success);

            await tx.SetKeyValue(writeKey + "/" + i, "w" + i, durability: KeyValueDurability.Persistent, cancellationToken: ct);
            await tx.Commit(ct);
        }

        Assert.True(Interlocked.Read(ref onePhase) >= transactions,
            $"expected at least {transactions} one-phase commits for read-carrying transactions on a single-process node, observed {Interlocked.Read(ref onePhase)} — read-carrying transactions appear to be falling back to 2PC");

        // The committed writes are all readable — the fast path really committed them.
        for (int i = 0; i < transactions; i++)
        {
            KahunaKeyValue after = await client.GetKeyValue(writeKey + "/" + i, KeyValueDurability.Persistent, cancellationToken: ct);
            Assert.True(after.Success);
            Assert.Equal("w" + i, after.ValueAsString());
        }
    }
}
