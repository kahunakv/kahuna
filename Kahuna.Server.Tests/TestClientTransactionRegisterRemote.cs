
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Drives a real client transaction session end to end over the in-process transport. Every
/// point operation issued inside a session now carries a coordinator key and a per-operation id
/// and is routed through the register-remote path: the key's partition leader records the
/// operation's confirmed effect on the coordinator before returning. These tests confirm that
/// threading is correct — writes are observable within the session (read-your-writes), the
/// coordinator's working set drives the commit, and the committed state is visible afterwards.
/// The wire serialization of the operation identity is only exercisable against the Docker
/// cluster; here the identity flows through the in-process IKahuna, so the routing and recording
/// logic is covered without the gRPC codec.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestClientTransactionRegisterRemote
{
    private readonly ILoggerFactory loggerFactory;

    public TestClientTransactionRegisterRemote(ITestOutputHelper outputHelper)
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
    /// A write issued inside a session is visible to a later read in the same session, and the
    /// value survives commit — proving the register-remote operation recorded the mutation on the
    /// coordinator and the coordinator's working set carried it through 2PC.
    /// </summary>
    [Fact]
    public async Task SessionSetThenGet_CommitsAndPersistsThroughRegisterRemote()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "rr/set-get/" + Guid.NewGuid().ToString("N")[..8];

        await using (KahunaTransactionSession tx = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic },
                         TestContext.Current.CancellationToken))
        {
            await tx.SetKeyValue(key, "written-in-tx", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);

            // Read-your-writes inside the same session.
            KahunaKeyValue inTx = await tx.GetKeyValue(key, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.True(inTx.Success);
            Assert.Equal("written-in-tx", inTx.ValueAsString());

            Assert.True(await tx.Commit(TestContext.Current.CancellationToken));
        }

        // The committed value is visible outside the transaction.
        KahunaKeyValue after = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(after.Success);
        Assert.Equal("written-in-tx", after.ValueAsString());
    }

    /// <summary>
    /// A session that rolls back leaves no committed state — the register-remote mutation was
    /// recorded on the coordinator but never applied, so the key remains absent afterwards.
    /// </summary>
    [Fact]
    public async Task SessionSetThenRollback_LeavesNoCommittedState()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "rr/rollback/" + Guid.NewGuid().ToString("N")[..8];

        await using (KahunaTransactionSession tx = await client.StartTransactionSession(
                         new() { Locking = KeyValueTransactionLocking.Pessimistic },
                         TestContext.Current.CancellationToken))
        {
            await tx.SetKeyValue(key, "discarded", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(await tx.Rollback(TestContext.Current.CancellationToken));
        }

        KahunaKeyValue after = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(after.Success);
    }
}
