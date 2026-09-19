using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kahuna.Server.Communication.Internode;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna;

/// <summary>
/// Several embedded Kahuna nodes in one process, wired through the in-memory transports. Every
/// member is a full node with its own storage, Raft log and actors, so the cluster replicates,
/// elects leaders and fails over as a networked one does; only the transport is in memory.
/// <para>
/// The cluster works in both builds. In the thread-free (browser) build every node, partition and
/// timer shares the one event loop, so the Raft timings must leave room for all of them; see
/// <see cref="CreateInMemoryAsync"/>.
/// </para>
/// <para>
/// A stopped node is cut off from the Raft transport and removed from the Kahuna transport, as if its
/// host had crashed. A Kahuna call that another node forwards to it in that window fails with a
/// <see cref="KahunaServerException"/>; the caller retries, and the retry routes to the new leader once
/// one is elected. A restarted node starts with empty storage and log under the same endpoint and
/// catches up from the others.
/// </para>
/// <para>
/// The members are not thread-safe against one another's lifecycle: stop, restart and dispose are
/// serialized by the cluster, and a node returned by <see cref="GetNode"/> must not be used after it
/// is stopped.
/// </para>
/// </summary>
public sealed class EmbeddedKahunaCluster : IAsyncDisposable
{
    /// <summary>Port of the first member when the base options leave the port unset.</summary>
    private const int DefaultBasePort = 7000;

    /// <summary>Pause between polls while waiting for a leader.</summary>
    private static readonly TimeSpan LeaderPollInterval = TimeSpan.FromMilliseconds(20);

    private readonly EmbeddedKahunaOptions[] nodeOptions;

    private readonly string[] endpoints;

    private readonly EmbeddedKahunaNode?[] nodes;

    private readonly MemoryInterNodeCommmunication interNode = new();

    private readonly InMemoryCommunication raftTransport = new();

    private readonly ILoggerFactory loggerFactory;

    private readonly SemaphoreSlim lifecycle = new(1, 1);

    private bool disposed;

    private EmbeddedKahunaCluster(EmbeddedKahunaOptions[] nodeOptions, ILoggerFactory loggerFactory)
    {
        this.nodeOptions = nodeOptions;
        this.loggerFactory = loggerFactory;

        endpoints = new string[nodeOptions.Length];
        for (int i = 0; i < nodeOptions.Length; i++)
            endpoints[i] = nodeOptions[i].Host + ":" + nodeOptions[i].Port;

        nodes = new EmbeddedKahunaNode?[nodeOptions.Length];
    }

    /// <summary>The shared inter-node transport; each node calls through its own view of it.</summary>
    internal MemoryInterNodeCommmunication InterNode => interNode;

    /// <summary>Number of members, running or stopped.</summary>
    public int NodeCount => nodes.Length;

    /// <summary>Number of partitions each node creates, including the meta partition 0.</summary>
    public int PartitionCount => nodeOptions[0].InitialPartitions + 1;

    /// <summary>The Raft endpoint of the member at <paramref name="index"/>.</summary>
    public string GetEndpoint(int index) => endpoints[CheckIndex(index)];

    /// <summary>True when the member at <paramref name="index"/> is running.</summary>
    public bool IsRunning(int index) => nodes[CheckIndex(index)] is not null;

    /// <summary>The running member at <paramref name="index"/>.</summary>
    /// <exception cref="InvalidOperationException">The member is stopped.</exception>
    public EmbeddedKahunaNode GetNode(int index)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        return nodes[CheckIndex(index)] ?? throw new InvalidOperationException($"Node {index} ({endpoints[index]}) is stopped.");
    }

    /// <summary>
    /// Creates <paramref name="nodeCount"/> nodes from <paramref name="baseOptions"/>, wires them through
    /// one shared in-memory transport of each kind, starts them together, and returns once every
    /// partition has a leader.
    /// <para>
    /// Each member gets a copy of the base options with a distinct node id (1..N), node name
    /// (<c>{NodeName}-{id}</c>) and port (<c>Port + index</c>, or 7000 + index when the port is 0), so
    /// every member has a distinct endpoint. Storage and write-ahead log must be <c>memory</c>: a
    /// restarted member starts empty and catches up from the others.
    /// </para>
    /// <para>
    /// Timings: keep the embedded defaults (100 ms heartbeat, 500 to 1500 ms election timeout). With
    /// three nodes on one browser event loop, the longest measured gap between two heartbeats of a
    /// leader was about 275 ms, so the shortest election timeout is about twice that gap. Shorter
    /// timings speed up the first election, but a heartbeat that runs late then starts a needless
    /// election. docs/thread-free-embedded-mode-guide.md has the measurements.
    /// </para>
    /// </summary>
    public static async Task<EmbeddedKahunaCluster> CreateInMemoryAsync(
        int nodeCount,
        EmbeddedKahunaOptions baseOptions,
        ILoggerFactory? loggerFactory = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(baseOptions);

        if (nodeCount < 1)
            throw new ArgumentOutOfRangeException(nameof(nodeCount), nodeCount, "An embedded cluster needs at least one node.");

        if (baseOptions.Storage != "memory" || baseOptions.WalStorage != "memory")
            throw new ArgumentException(
                "An in-memory embedded cluster needs Storage and WalStorage set to 'memory': a stopped member restarts empty and catches up from the others.",
                nameof(baseOptions));

        if (baseOptions.JoinExistingSeeds is { Count: > 0 })
            throw new ArgumentException(
                $"{nameof(baseOptions.JoinExistingSeeds)} is not supported for an in-memory embedded cluster: its members boot from a static roster of each other.",
                nameof(baseOptions));

        int basePort = baseOptions.Port == 0 ? DefaultBasePort : baseOptions.Port;

        EmbeddedKahunaOptions[] nodeOptions = new EmbeddedKahunaOptions[nodeCount];

        for (int i = 0; i < nodeCount; i++)
        {
            EmbeddedKahunaOptions options = baseOptions.Copy();
            options.NodeId = i + 1;
            options.NodeName = $"{baseOptions.NodeName}-{i + 1}";
            options.Port = basePort + i;
            nodeOptions[i] = options;
        }

        EmbeddedKahunaCluster cluster = new(nodeOptions, loggerFactory ?? NullLoggerFactory.Instance);

        try
        {
            for (int i = 0; i < nodeCount; i++)
                cluster.nodes[i] = cluster.BuildNode(i);

            cluster.PublishRoutes();

            Task[] starts = new Task[nodeCount];
            for (int i = 0; i < nodeCount; i++)
                starts[i] = cluster.nodes[i]!.StartAsync(cancellationToken);

            await Task.WhenAll(starts).ConfigureAwait(false);

            for (int partitionId = 0; partitionId < cluster.PartitionCount; partitionId++)
                await cluster.GetLeaderIndexAsync(partitionId, cancellationToken).ConfigureAwait(false);

            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// The index of a running member that believes it leads <paramref name="partitionId"/>, polling until
    /// one does. The answer is each node's local belief: right after a failover the old leader can still
    /// believe it leads until it hears the new term, so treat the result as a routing hint.
    /// </summary>
    public async Task<int> GetLeaderIndexAsync(int partitionId, CancellationToken cancellationToken = default)
    {
        while (true)
        {
            ObjectDisposedException.ThrowIf(disposed, this);

            for (int i = 0; i < nodes.Length; i++)
            {
                EmbeddedKahunaNode? node = nodes[i];
                if (node is null)
                    continue;

                try
                {
                    if (await node.Raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
                        return i;
                }
                catch (Exception ex) when (ex is RaftException or PartitionNotHostedException or ObjectDisposedException)
                {
                    // A member still rejoining, one that does not host the partition, or one stopped
                    // while this poll ran: none of them leads it.
                }
            }

            await Task.Delay(LeaderPollInterval, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Drops the traffic from the member at <paramref name="from"/> to the member at
    /// <paramref name="to"/>, on both transports. Raft messages that need no reply are dropped only in
    /// this direction; every call that waits for a reply, on either transport, fails in both
    /// directions, because a call needs its reply. The members keep running, so an isolated member
    /// keeps its timers and keeps campaigning.
    /// <para>
    /// Use it to show a network partition: block the links between the two sides of the cluster. The
    /// minority side cannot commit; the majority side elects a leader and serves writes.
    /// </para>
    /// </summary>
    public void BlockLink(int from, int to)
    {
        CheckIndex(from);
        CheckIndex(to);

        if (from == to)
            throw new ArgumentException("A member has no link to itself.", nameof(to));

        raftTransport.BlockLink(endpoints[from], endpoints[to]);
        interNode.BlockLink(endpoints[from], endpoints[to]);
    }

    /// <summary>Restores the traffic from the member at <paramref name="from"/> to the member at <paramref name="to"/>.</summary>
    public void UnblockLink(int from, int to)
    {
        CheckIndex(from);
        CheckIndex(to);

        raftTransport.UnblockLink(endpoints[from], endpoints[to]);
        interNode.UnblockLink(endpoints[from], endpoints[to]);
    }

    /// <summary>Drops the traffic between two members in both directions.</summary>
    public void BlockLinkBothWays(int a, int b)
    {
        BlockLink(a, b);
        BlockLink(b, a);
    }

    /// <summary>Restores the traffic between two members in both directions.</summary>
    public void UnblockLinkBothWays(int a, int b)
    {
        UnblockLink(a, b);
        UnblockLink(b, a);
    }

    /// <summary>
    /// Cuts the member at <paramref name="index"/> off from every other member, in both directions. The
    /// member keeps running: it keeps its timers and keeps campaigning, unlike a member that
    /// <see cref="StopNodeAsync"/> stopped.
    /// </summary>
    public void IsolateNode(int index)
    {
        CheckIndex(index);

        for (int i = 0; i < nodes.Length; i++)
        {
            if (i != index)
                BlockLinkBothWays(index, i);
        }
    }

    /// <summary>
    /// Restores every link between the members. A member that <see cref="StopNodeAsync"/> stopped stays
    /// stopped and stays cut off: only a link block is removed here.
    /// </summary>
    public void UnblockAllLinks()
    {
        for (int from = 0; from < nodes.Length; from++)
        {
            for (int to = 0; to < nodes.Length; to++)
            {
                if (from != to)
                    raftTransport.UnblockLink(endpoints[from], endpoints[to]);
            }
        }

        interNode.UnblockAllLinks();
    }

    /// <summary>True when the traffic from the member at <paramref name="from"/> to the member at <paramref name="to"/> is dropped.</summary>
    public bool IsLinkBlocked(int from, int to) =>
        raftTransport.IsDeliveryBlocked(endpoints[CheckIndex(from)], endpoints[CheckIndex(to)]);

    /// <summary>
    /// Stops the member at <paramref name="index"/> as if its host crashed: its Raft traffic is cut, it
    /// leaves the Kahuna transport, and the node is disposed. The other members elect new leaders for
    /// the partitions it led.
    /// </summary>
    public async Task StopNodeAsync(int index)
    {
        CheckIndex(index);

        await lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(disposed, this);

            EmbeddedKahunaNode node = nodes[index] ?? throw new InvalidOperationException($"Node {index} ({endpoints[index]}) is already stopped.");

            // Cut the node off before it is disposed, so no member calls into a node that is shutting
            // down: the Raft transport drops its traffic in both directions, and the Kahuna transport no
            // longer routes to it.
            raftTransport.PartitionNode(endpoints[index]);
            nodes[index] = null;
            PublishRoutes();

            await node.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            lifecycle.Release();
        }
    }

    /// <summary>
    /// Starts a stopped member again under the same endpoint, with empty storage and log. It rejoins
    /// through the static roster and catches up from the other members. Returns once the member has
    /// started and sees a leader for every partition it hosts.
    /// </summary>
    public async Task RestartNodeAsync(int index, CancellationToken cancellationToken = default)
    {
        CheckIndex(index);

        await lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(disposed, this);

            if (nodes[index] is not null)
                throw new InvalidOperationException($"Node {index} ({endpoints[index]}) is already running.");

            EmbeddedKahunaNode node = BuildNode(index);
            nodes[index] = node;
            PublishRoutes();
            raftTransport.HealPartition(endpoints[index]);

            try
            {
                await node.StartAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                raftTransport.PartitionNode(endpoints[index]);
                nodes[index] = null;
                PublishRoutes();
                await node.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            lifecycle.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            if (disposed)
                return;

            disposed = true;

            for (int i = 0; i < nodes.Length; i++)
            {
                EmbeddedKahunaNode? node = nodes[i];
                if (node is null)
                    continue;

                nodes[i] = null;
                await node.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            lifecycle.Release();
        }
    }

    private EmbeddedKahunaNode BuildNode(int index)
    {
        List<RaftNode> peers = new(endpoints.Length - 1);
        for (int i = 0; i < endpoints.Length; i++)
        {
            if (i != index)
                peers.Add(new(endpoints[i]));
        }

        // Each node calls through its own view of the shared inter-node transport, so the transport
        // knows the sender of every call and can drop traffic on a blocked link.
        return new(nodeOptions[index], interNode.ForNode(endpoints[index]), raftTransport, new StaticDiscovery(peers), loggerFactory);
    }

    /// <summary>Publishes the running members to both transports.</summary>
    private void PublishRoutes()
    {
        Dictionary<string, IKahuna> kahunaRoutes = new(nodes.Length);
        Dictionary<string, IRaft> raftRoutes = new(nodes.Length);

        for (int i = 0; i < nodes.Length; i++)
        {
            EmbeddedKahunaNode? node = nodes[i];
            if (node is null)
                continue;

            kahunaRoutes[endpoints[i]] = node.Kahuna;
            raftRoutes[endpoints[i]] = node.Raft;
        }

        interNode.SetNodes(kahunaRoutes);
        raftTransport.SetNodes(raftRoutes);
    }

    private int CheckIndex(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, nodes.Length);
        return index;
    }
}
