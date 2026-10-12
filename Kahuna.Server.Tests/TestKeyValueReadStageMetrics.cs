using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Pins the point-read stage histogram: a get outside a session records a <c>direct</c> total with the
/// leadership check and the actor round trip beneath it; a get inside a session records the registration and
/// the completion too, under a <c>registered</c> total. On a single-process node the leader's lease answers
/// every check synchronously once the node has applied its own commits, so the <c>confirm</c> stage must show
/// up tagged as a lease hit.
/// </summary>
public sealed class TestKeyValueReadStageMetrics
{
    private const string Stage = "kahuna.kv.read.stage";

    private readonly ILoggerFactory loggerFactory;

    public TestKeyValueReadStageMetrics(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task DirectGet_RecordsConfirmActorAndDirectTotal()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "readstage/direct/" + Guid.NewGuid().ToString("N")[..8];
        await client.SetKeyValue(key, "v0", cancellationToken: ct);

        using MetricCapture byStage = new("stage", Stage);
        using MetricCapture byPath = new("path", Stage);
        using MetricCapture byLease = new("lease", Stage);

        const int reads = 5;
        for (int i = 0; i < reads; i++)
        {
            KahunaKeyValue got = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: ct);
            Assert.True(got.Success);
        }

        // The meter is process-wide, so other test classes can add samples; every bound is a lower bound.
        Assert.True(byStage.Samples(Stage, "confirm").Count >= reads);
        Assert.True(byStage.Samples(Stage, "actor").Count >= reads);
        Assert.True(byPath.Samples(Stage, "direct").Count >= reads);

        // A direct read registers nothing on a coordinator, so it must not produce the registration stages
        // unless another test's session reads did. Pin the shape on the lease instead: a standalone node that
        // has applied its own commits answers the check from the lease, synchronously.
        Assert.True(byLease.Samples(Stage, "hit").Count >= 1,
            "expected at least one leadership check answered synchronously by the lease on a standalone node");

        foreach (double ms in byStage.Samples(Stage, "total"))
            Assert.True(ms >= 0 && ms < 10_000, $"a stage sample is wall time in milliseconds, got {ms}");
    }

    [Fact]
    public async Task SessionGet_RecordsRegisterCompleteAndRegisteredTotal()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "readstage/session/" + Guid.NewGuid().ToString("N")[..8];
        await client.SetKeyValue(key, "v0", cancellationToken: ct);

        using MetricCapture byStage = new("stage", Stage);
        using MetricCapture byPath = new("path", Stage);

        const int sessions = 3;
        const int readsPerSession = 2;
        for (int i = 0; i < sessions; i++)
        {
            await using KahunaTransactionSession tx = await client.StartTransactionSession(
                new() { Locking = KeyValueTransactionLocking.Optimistic, AutoCommit = false }, ct);

            for (int j = 0; j < readsPerSession; j++)
            {
                KahunaKeyValue got = await tx.GetKeyValue(key, KeyValueDurability.Persistent, ct);
                Assert.True(got.Success);
            }

            Assert.True(await tx.Rollback(ct));
        }

        const int reads = sessions * readsPerSession;
        Assert.True(byStage.Samples(Stage, "register").Count >= reads);
        Assert.True(byStage.Samples(Stage, "complete").Count >= reads);
        Assert.True(byStage.Samples(Stage, "confirm").Count >= reads);
        Assert.True(byStage.Samples(Stage, "actor").Count >= reads);
        Assert.True(byPath.Samples(Stage, "registered").Count >= reads);
    }
}
