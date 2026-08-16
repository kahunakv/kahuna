using System.Reflection;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

using Kommander;

using Microsoft.Extensions.Logging.Abstractions;

using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the node composition order. These invariants are ordering, not behavior: they are
/// invisible to functional tests, and the cost of breaking one is a stale read (a doubly wrapped
/// backend) or a node that half-builds itself before rejecting its own configuration.
/// </summary>
public sealed class TestKahunaNodeComposition
{
    private static KahunaConfiguration Configuration() => new()
    {
        Storage = "memory",
        StoragePath = Path.Combine(Path.GetTempPath(), "kahuna-composition-" + Guid.NewGuid().ToString("N")[..8]),
        StorageRevision = "v1"
    };

    [Fact]
    public async Task Backend_IsWrappedInTheUnflushedOverlayExactlyOnce()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        });
        await node.StartAsync(TestContext.Current.CancellationToken);

        IPersistenceBackend backend = ((KahunaManager)node.Kahuna).PersistenceBackend;

        // Every consumer must read through the overlay, or a committed-but-unflushed key answers
        // DoesNotExist on a freshly promoted leader.
        Assert.Equal("UnflushedOverlayPersistenceBackend", backend.GetType().Name);

        // ...and through exactly one of them. A second wrap would give the node two independent
        // unflushed-write indexes: producers would record into one while readers consulted the
        // other, which reintroduces the very stale read the overlay exists to prevent.
        FieldInfo innerField = backend.GetType().GetField("inner", BindingFlags.Instance | BindingFlags.NonPublic)!;
        object? inner = innerField.GetValue(backend);

        Assert.NotNull(inner);
        Assert.NotEqual("UnflushedOverlayPersistenceBackend", inner.GetType().Name);
    }

    [Fact]
    public void InvalidSchedulerConfiguration_IsRejectedBeforeTheGraphIsBuilt()
    {
        using ActorSystem actorSystem = new(serviceProvider: null, NullLogger<IRaft>.Instance);

        KahunaConfiguration configuration = Configuration();
        configuration.BackendReadQueueDepth = 0;   // would reject every backend I/O operation

        MemoryInterNodeCommmunication interNode = new();

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => new KahunaManager(
            // Raft is never touched before the configuration is validated, so the graph the manager
            // would otherwise build over it is not needed to prove the ordering.
            actorSystem, null!, configuration, interNode, NullLogger<IKahuna>.Instance));

        Assert.Contains("BackendReadQueueDepth", ex.Message, StringComparison.Ordinal);

        // Configuration is validated before anything is constructed, so the rejected node must not
        // have spawned the background writer — a half-built node would leave an actor (and the
        // stores it holds open) behind with no owner to dispose it.
        Assert.Null(actorSystem.Get<BackgroundWriterActor, BackgroundWriteRequest>("background-writer"));
    }
}
