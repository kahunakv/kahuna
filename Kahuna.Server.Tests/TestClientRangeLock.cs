
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

[Collection("ClusterTests")]
public sealed class TestClientRangeLock
{
    private readonly ILoggerFactory loggerFactory;

    public TestClientRangeLock(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaNode CreateNode(ILoggerFactory lf) => new(new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    }, lf);

    /// <summary>
    /// Two concurrent pessimistic transactions can both acquire a shared range lock over the same range.
    /// </summary>
    [Fact]
    public async Task AcquireRangeLock_SharedMode_AllowsConcurrentShared()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        InProcessKahunaCommunication comm = new(node.Kahuna);
        KahunaClient client = new("http://localhost", communication: comm);

        string space = "rl/shared/" + Guid.NewGuid().ToString("N")[..8];
        await client.RegisterKeyRange(space, TestContext.Current.CancellationToken);

        await using KahunaTransactionSession tx1 = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            TestContext.Current.CancellationToken);

        await using KahunaTransactionSession tx2 = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            TestContext.Current.CancellationToken);

        // Both shared locks should succeed without throwing
        await tx1.GetByRange(space, null, true, null, false,
            lockMode: RangeLockMode.Shared,
            cancellationToken: TestContext.Current.CancellationToken);

        await tx2.GetByRange(space, null, true, null, false,
            lockMode: RangeLockMode.Shared,
            cancellationToken: TestContext.Current.CancellationToken);

        await tx1.Commit(TestContext.Current.CancellationToken);
        await tx2.Commit(TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// An exclusive range lock conflicts with a concurrent shared lock (second acquire throws).
    /// </summary>
    [Fact]
    public async Task AcquireRangeLock_ExclusiveConflictsWithShared()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        InProcessKahunaCommunication comm = new(node.Kahuna);
        KahunaClient client = new("http://localhost", communication: comm);

        string space = "rl/excl/" + Guid.NewGuid().ToString("N")[..8];
        await client.RegisterKeyRange(space, TestContext.Current.CancellationToken);

        await using KahunaTransactionSession tx1 = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            TestContext.Current.CancellationToken);

        await using KahunaTransactionSession tx2 = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            TestContext.Current.CancellationToken);

        // tx1 takes exclusive lock
        await tx1.GetByRange(space, null, true, null, false,
            lockMode: RangeLockMode.Exclusive,
            cancellationToken: TestContext.Current.CancellationToken);

        // tx2 attempting shared lock while tx1 holds exclusive must fail
        await Assert.ThrowsAsync<KahunaException>(() =>
            tx2.GetByRange(space, null, true, null, false,
                lockMode: RangeLockMode.Shared,
                cancellationToken: TestContext.Current.CancellationToken));

        await tx1.Commit(TestContext.Current.CancellationToken);
    }
}
