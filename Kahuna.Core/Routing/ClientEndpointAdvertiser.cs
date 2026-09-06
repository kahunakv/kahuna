using System.Collections.Concurrent;
using Kommander;

namespace Kahuna.Server.Routing;

/// <summary>
/// Translates the endpoints Kahuna routes on into the base URLs a client can dial.
///
/// <para>
/// A node is identified inside the cluster by its Raft endpoint — <c>host:port</c> with no scheme.
/// That address is where peers send consensus and forwarding traffic; it is not, in general, where
/// an application reaches the node. Container port mapping, split internal/external host names and
/// a separate public listener all break the assumption that the two are the same. So a hint never
/// carries a Raft endpoint: it carries what the node explicitly advertises.
/// </para>
///
/// <para>
/// An operator can state the local node's URL outright. Left unstated, it is derived the same way a
/// peer's is: the Raft endpoint prefixed with the advertisement scheme, which is the rule the
/// inter-node gRPC channels already dial peers with. A deployment where that rule does not hold
/// states the URL explicitly, or turns advertisement off rather than publishing an address no
/// client can reach.
/// </para>
/// </summary>
internal sealed class ClientEndpointAdvertiser
{
    private readonly IRaft raft;

    private readonly string localEndpoint;

    private readonly string localAdvertised;

    private readonly string peerScheme;

    private readonly bool advertisePeers;

    /// <summary>
    /// Peer Raft endpoint to advertised URL. Bounded by the roster size, so it needs no eviction:
    /// a cluster has as many entries as it has nodes, and a removed node's entry is one small
    /// string pair.
    /// </summary>
    private readonly ConcurrentDictionary<string, string> peerUrls = new(StringComparer.Ordinal);

    /// <summary>This node's advertised client URL; empty when it advertises none.</summary>
    public string LocalAdvertised => localAdvertised;

    public ClientEndpointAdvertiser(IRaft raft, string localAdvertised, string peerScheme, bool advertisePeers, bool enabled)
    {
        this.raft = raft;
        this.localEndpoint = raft.GetLocalEndpoint();
        this.peerScheme = peerScheme;
        this.advertisePeers = advertisePeers;

        this.localAdvertised = !enabled
            ? ""
            : localAdvertised.Length > 0
                ? localAdvertised
                : peerScheme + localEndpoint;
    }

    /// <summary>
    /// The client-reachable URL for the node at <paramref name="endpoint"/>, or an empty string
    /// when this node cannot advertise one for it.
    /// </summary>
    public string Advertise(string? endpoint)
    {
        if (string.IsNullOrEmpty(endpoint))
            return "";

        if (string.Equals(endpoint, localEndpoint, StringComparison.Ordinal))
            return localAdvertised;

        if (!advertisePeers)
            return "";

        if (peerUrls.TryGetValue(endpoint, out string? url))
            return url;

        url = peerScheme + endpoint;
        peerUrls[endpoint] = url;
        return url;
    }

    /// <summary>
    /// The client-reachable URL of the node this one believes leads <paramref name="partitionId"/>,
    /// or an empty string when no leader is known. Best-effort, exactly like the belief it reads.
    /// </summary>
    public string AdvertiseLeaderOf(int partitionId)
    {
        try
        {
            return Advertise(raft.GetPartitionLeaderHint(partitionId));
        }
        catch (RaftException)
        {
            return "";
        }
    }
}
