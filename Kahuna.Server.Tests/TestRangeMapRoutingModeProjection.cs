using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication;
using Kommander.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The per-node routing-mode registry is a projection of the installed range map, and the store keeps the
/// two in step on every install: a key space is key-range routed from the moment its descriptor is visible
/// in <see cref="RangeMapStore.Current"/>, and hash routed again from the moment its descriptors are gone.
/// A route reads the mode and then the map, so the mode must be flipped before the map is published: a
/// node that showed the descriptor of a freshly seeded index space while it still hash-routed the space
/// sent a key of that space to the hash partition, while every other node routed it by range.
/// </summary>
public sealed class TestRangeMapRoutingModeProjection : IDisposable
{
    private readonly string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-rangemap-modes-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(storagePath))
                Directory.Delete(storagePath, recursive: true);
        }
        catch
        {
            // Best effort temp cleanup.
        }
    }

    private static RangeMapStore NewStore(ProposeOutcomeStubRaft raft, KeySpaceRegistry? registry, string? path = null) =>
        new(raft, path, path is null ? null : "n1", NullLogger<IKahuna>.Instance, checkpointEveryMutations: 0, routingModes: registry);

    private static RangeDescriptor FullRange(string keySpace, int partitionId) => new()
    {
        KeySpace = keySpace,
        StartKey = null,
        EndKey = null,
        PartitionId = partitionId,
        Generation = 1
    };

    private static RaftLog CommittedEntry(long logId, byte[] payload) => new()
    {
        Id = logId,
        Term = 2,
        Type = RaftLogType.Committed,
        LogType = ReplicationTypes.RangeMap,
        LogData = payload
    };

    private static byte[] Encode(params RangeDescriptor[] descriptors)
    {
        // Round-trip through a scratch store so the payload is produced by the same codec the leader
        // uses when it proposes.
        ProposeOutcomeStubRaft raft = new();
        using RangeMapStore scratch = NewStore(raft, registry: null);
        Assert.True(scratch.MutateMapAsync(_ => new RangeMap(descriptors)).GetAwaiter().GetResult());
        return raft.ProposedPayloads[^1];
    }

    /// <summary>A committed entry flips the spaces it seeds on, and the spaces it drops off, as it installs.</summary>
    [Fact]
    public void Apply_FlipsTheRoutingMode_WithTheMap()
    {
        KeySpaceRegistry registry = new();
        using RangeMapStore store = NewStore(new ProposeOutcomeStubRaft(), registry);

        Assert.Equal(RoutingMode.Hash, registry.GetMode("t:r"));

        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(1, Encode(FullRange("t:r", 2)))));
        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:r"));
        Assert.Equal(2, store.Current.Find("t:r", "t:r/x")!.PartitionId);

        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(2, Encode(FullRange("t:r", 2), FullRange("t:r|i:pk", 2)))));
        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:r"));
        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:r|i:pk"));

        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(3, Encode(FullRange("t:r", 2)))));
        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:r"));
        Assert.Equal(RoutingMode.Hash, registry.GetMode("t:r|i:pk"));

        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(4, Encode())));
        Assert.Equal(RoutingMode.Hash, registry.GetMode("t:r"));
        Assert.Empty(store.Current.Descriptors);
    }

    /// <summary>An entry the index guard ignores leaves the registry exactly as the newer map left it.</summary>
    [Fact]
    public void StaleEcho_LeavesTheRoutingModesOfTheNewerMap()
    {
        KeySpaceRegistry registry = new();
        using RangeMapStore store = NewStore(new ProposeOutcomeStubRaft(), registry);

        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(5, Encode(FullRange("a", 1), FullRange("b", 2)))));
        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(4, Encode(FullRange("a", 1)))));

        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("a"));
        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("b"));
        Assert.Equal(2, store.Current.Find("b", "b/x")!.PartitionId);
    }

    /// <summary>The leader's own install flips the mode before the mutation reports committed, with no echo needed.</summary>
    [Fact]
    public async Task LeaderInstall_FlipsTheRoutingMode_BeforeTheMutationReturns()
    {
        KeySpaceRegistry registry = new();
        using RangeMapStore store = NewStore(new ProposeOutcomeStubRaft(), registry);
        CancellationToken ct = TestContext.Current.CancellationToken;

        Assert.True(await store.MutateAsync(current => [.. current, FullRange("t:r", 2)], ct));
        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:r"));

        Assert.True(await store.MutateAsync(current => [.. current.Where(d => d.KeySpace != "t:r")], ct));
        Assert.Equal(RoutingMode.Hash, registry.GetMode("t:r"));
    }

    /// <summary>A whole-partition state transfer installs its map and the routing modes of exactly that map.</summary>
    [Fact]
    public void StateTransfer_ReconcilesTheRoutingModes()
    {
        KeySpaceRegistry registry = new();
        using RangeMapStore store = NewStore(new ProposeOutcomeStubRaft(), registry);

        registry.RegisterKeyRange("stale");

        store.CommitState(store.ParseState(Encode(FullRange("t:r", 2))));

        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:r"));
        Assert.Equal(RoutingMode.Hash, registry.GetMode("stale"));
    }

    /// <summary>The durable snapshot a restart loads flips its spaces on before any entry is replayed.</summary>
    [Fact]
    public async Task DurableSnapshot_ReconcilesTheRoutingModes_AtStartup()
    {
        Directory.CreateDirectory(storagePath);
        CancellationToken ct = TestContext.Current.CancellationToken;

        using (RangeMapStore store = NewStore(new ProposeOutcomeStubRaft(), new KeySpaceRegistry(), storagePath))
            Assert.True(await store.MutateAsync(current => [.. current, FullRange("t:r", 2)], ct));

        KeySpaceRegistry reopenedRegistry = new();
        using RangeMapStore reopened = NewStore(new ProposeOutcomeStubRaft(), reopenedRegistry, storagePath);

        Assert.Equal(2, reopened.Current.Find("t:r", "t:r/x")!.PartitionId);
        Assert.Equal(RoutingMode.KeyRange, reopenedRegistry.GetMode("t:r"));
    }

    /// <summary>
    /// Under a stream of installs that seed new spaces, a reader that takes a map snapshot and then routes a key of
    /// a space the snapshot covers always routes it by key range, to the snapshot's partition — never by hash.
    /// </summary>
    [Fact]
    public async Task AReaderThatSeesADescriptor_NeverHashRoutesItsSpace()
    {
        const int installs = 400;
        const int poolSize = 4;

        KeySpaceRegistry registry = new();
        using RangeMapStore store = NewStore(new ProposeOutcomeStubRaft(), registry);
        DataPartitionRouter router = new(poolSize);

        List<RangeDescriptor> seeded = new(installs);
        byte[][] payloads = new byte[installs][];

        for (int i = 0; i < installs; i++)
        {
            seeded.Add(FullRange($"space-{i:D4}:r", 1 + (i % poolSize)));
            payloads[i] = Encode([.. seeded]);
        }

        using ManualResetEventSlim start = new(false);

        Task writer = Task.Run(() =>
        {
            start.Wait();

            for (int i = 0; i < installs; i++)
                Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, CommittedEntry(i + 1, payloads[i])));
        }, TestContext.Current.CancellationToken);

        long snapshotsRead = 0;
        long routesChecked = 0;

        start.Set();

        while (!writer.IsCompleted)
        {
            RangeMap map = store.Current;
            snapshotsRead++;

            foreach (RangeDescriptor descriptor in map.Descriptors)
            {
                // The route reads the mode first and the map second, exactly as the locator does.
                (int partitionId, long generation, bool isKeyRange, RangeDescriptor? routed) =
                    RangeRouting.LocateWithMode(registry, map, router, descriptor.KeySpace + "/k");

                Assert.True(isKeyRange, $"{descriptor.KeySpace} is visible in the map but routes by hash");
                Assert.Equal(descriptor.PartitionId, partitionId);
                Assert.Equal(descriptor.Generation, generation);
                Assert.Same(descriptor, routed);
                routesChecked++;
            }
        }

        await writer;

        Assert.True(snapshotsRead > 0);
        Assert.True(routesChecked > 0);
        Assert.Equal(installs, store.Current.Descriptors.Count);

        foreach (RangeDescriptor descriptor in seeded)
            Assert.Equal(RoutingMode.KeyRange, registry.GetMode(descriptor.KeySpace));
    }
}
