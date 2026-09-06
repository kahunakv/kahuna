using System.Collections.Concurrent;

using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Routing;
using Microsoft.Extensions.Logging;

namespace Kahuna.Client.Routing;

/// <summary>
/// The client's routing brain: picks a destination for an operation, learns the destinations
/// responses report, holds failed endpoints out for a cooldown, and — in metadata mode — keeps one
/// coherent view of how the cluster routes resources.
///
/// <para>
/// Nothing here is authoritative. It changes which node a request is sent to first and nothing
/// else: the receiving node re-resolves the resource, re-applies range fences and re-checks
/// leadership, so a wrong or stale answer costs an inter-node forward. That is why every uncertain
/// case answers "I do not know" and lets the caller fall back to endpoint rotation, rather than
/// guessing.
/// </para>
/// </summary>
internal sealed class ClientRouteResolver : IKahunaRouteSink
{
    private readonly KahunaRoutingMode mode;

    private readonly RouteCache cache;

    private readonly RoutingEndpointPolicy endpoints;

    private readonly IKahunaRoutingTransport? metadataTransport;

    private readonly Func<string> bootstrapUrl;

    private readonly TimeSpan metadataLifetime;

    private readonly TimeSpan endpointCooldown;

    private readonly ILogger? logger;

    /// <summary>
    /// Endpoints in their failure cooldown, with the local deadline the cooldown ends at. Bounded by
    /// the number of endpoints the cluster has, so it needs no eviction of its own.
    /// </summary>
    private readonly ConcurrentDictionary<string, long> suppressed = new(StringComparer.Ordinal);

    private volatile RoutingMetadataSnapshot? metadata;

    /// <summary>
    /// Guards against several operations each starting their own metadata read. Only the caller that
    /// wins the flag issues one; every other caller keeps its existing answer for this operation and
    /// picks up the new snapshot on a later one, so no operation ever waits on discovery.
    /// </summary>
    private int metadataRefreshInFlight;

    public ClientRouteResolver(
        KahunaRoutingMode mode,
        RouteCache cache,
        RoutingEndpointPolicy endpoints,
        IKahunaRoutingTransport? metadataTransport,
        Func<string> bootstrapUrl,
        TimeSpan metadataLifetime,
        TimeSpan endpointCooldown,
        ILogger? logger
    )
    {
        this.mode = mode;
        this.cache = cache;
        this.endpoints = endpoints;
        this.metadataTransport = metadataTransport;
        this.bootstrapUrl = bootstrapUrl;
        this.metadataLifetime = metadataLifetime;
        this.endpointCooldown = endpointCooldown;
        this.logger = logger;
    }

    /// <inheritdoc/>
    public bool IsLearning => true;

    /// <summary>Entries currently held. For tests and diagnostics, not the request path.</summary>
    public int CachedRouteCount => cache.Count;

    /// <summary>
    /// The entry held for a resource, expired ones included. For tests and diagnostics: it reports
    /// what was learned — the partition, the range generation and the provenance — rather than only
    /// the endpoint an operation acts on.
    /// </summary>
    public RouteEntry? Peek(KahunaRoutingDomain domain, string resource) => cache.Peek(new RouteCacheKey(domain, resource));

    /// <summary>
    /// The endpoint to send an operation on <paramref name="resource"/> to, or null when the caller
    /// should use its ordinary endpoint rotation. Allocation-free on a hit, which is the path the
    /// cache exists for.
    /// </summary>
    public string? Select(KahunaRoutingDomain domain, string resource)
    {
        if (string.IsNullOrEmpty(resource))
            return null;

        RouteCacheKey key = new(domain, resource);

        RouteEntry? entry = cache.TryGet(key);

        if (entry is not null)
        {
            if (!IsSuppressed(entry.Endpoint))
            {
                RoutingMetrics.CacheHits.Add(1);
                return entry.Endpoint;
            }

            // The endpoint that entry names just failed. The entry itself is not evicted: it may
            // still be the right owner, and the endpoint's cooldown is the thing that lapses.
            RoutingMetrics.SuppressedRoutesSkipped.Add(1);
            return null;
        }

        RoutingMetrics.CacheMisses.Add(1);

        if (mode != KahunaRoutingMode.Metadata)
            return null;

        RoutingMetadataSnapshot? snapshot = metadata;

        if (snapshot is null || !snapshot.IsValidAt(Environment.TickCount64))
        {
            // Discovery is never on the request path: this operation goes out on rotation now, and
            // the map it starts loading serves the operations after it.
            StartMetadataRefresh();
            return null;
        }

        string? advertised = snapshot.TryResolve(domain, resource);

        if (advertised is null)
            return null;

        string? endpoint = endpoints.Resolve(advertised);

        if (endpoint is null || IsSuppressed(endpoint))
            return null;

        RoutingMetrics.MetadataHits.Add(1);

        return endpoint;
    }

    /// <inheritdoc/>
    public void Learn(KahunaRoutingDomain domain, string resource, int partitionId, string? endpoint, KahunaRouteProvenance provenance, long generation)
    {
        Learn(domain, resource, partitionId, endpoint, provenance, generation, requestUrl: null);
    }

    /// <summary>
    /// Records the route a response reported, against the endpoint the request was sent to.
    ///
    /// <para>
    /// <paramref name="requestUrl"/> is what makes a late response harmless. A response is allowed
    /// to replace the cached route only while that route still names the endpoint this request was
    /// sent to — the state the request was answering about. A reply that arrives after another
    /// response already moved the route elsewhere therefore finds a route it does not describe, and
    /// is dropped instead of undoing the newer answer. An entry that has expired, one that already
    /// names the reported endpoint, and one whose endpoint is in its failure cooldown are all
    /// replaceable: none of them is a newer answer this response could undo.
    /// </para>
    ///
    /// <para>
    /// Both provenances are stored. A forwarding node names the destination it sent the operation
    /// to, which is the same node the executor would have named except during an election, and the
    /// next response repairs it.
    /// </para>
    /// </summary>
    public void Learn(KahunaRoutingDomain domain, string resource, int partitionId, string? endpoint, KahunaRouteProvenance provenance, long generation, string? requestUrl)
    {
        if (string.IsNullOrEmpty(resource))
            return;

        if (provenance == KahunaRouteProvenance.Unknown)
        {
            RoutingMetrics.HintsRejected.Add(1, UnknownProvenance);
            return;
        }

        string? resolved = endpoints.Resolve(endpoint);

        if (resolved is null)
        {
            RoutingMetrics.HintsRejected.Add(1, EndpointRejected);
            return;
        }

        // The node answered, so it is reachable: that is exactly what its cooldown was waiting to
        // find out, and holding it out any longer would keep sending its own traffic elsewhere.
        suppressed.TryRemove(resolved, out _);

        RouteCacheKey key = new(domain, resource);

        RouteEntry? current = cache.Peek(key);

        if (current is null)
        {
            if (cache.TryAdd(key, resolved, partitionId, generation, provenance))
                RoutingMetrics.HintsLearned.Add(1);
            else
                RoutingMetrics.HintsRejected.Add(1, Superseded);

            return;
        }

        bool replaceable =
            !current.IsValidAt(Environment.TickCount64)
            || string.Equals(current.Endpoint, resolved, StringComparison.Ordinal)
            || (requestUrl is not null && string.Equals(current.Endpoint, requestUrl, StringComparison.Ordinal))
            || IsSuppressed(current.Endpoint);

        if (!replaceable)
        {
            RoutingMetrics.HintsRejected.Add(1, Superseded);
            return;
        }

        if (cache.TryReplace(key, current, resolved, partitionId, generation, provenance))
            RoutingMetrics.HintsLearned.Add(1);
        else
            RoutingMetrics.HintsRejected.Add(1, Superseded);
    }

    /// <summary>
    /// The URL to use for a handle that carries its own affinity, or null when that affinity is not
    /// usable right now.
    ///
    /// <para>
    /// A handle's affinity is still subject to the endpoint policy and the failure cooldown: the
    /// affinity says which node served the handle, not that the client may dial an address the
    /// operator never configured, nor that a node which just stopped answering is worth trying
    /// again. A null answer sends the caller down the ordinary routed path.
    /// </para>
    /// </summary>
    public string? TryUseAffinity(string? advertised)
    {
        string? resolved = endpoints.Resolve(advertised);

        return resolved is not null && !IsSuppressed(resolved) ? resolved : null;
    }

    /// <inheritdoc/>
    public void ReportEndpointFailure(string url)
    {
        if (string.IsNullOrEmpty(url))
            return;

        suppressed[url] = Environment.TickCount64 + (long)endpointCooldown.TotalMilliseconds;

        RoutingMetrics.EndpointsSuppressed.Add(1);
    }

    /// <summary>
    /// Reads the routing map now and publishes it if it is usable. Exposed so a caller — or a test —
    /// can make metadata mode deterministic instead of waiting for a background read.
    /// </summary>
    public async Task<bool> RefreshMetadataAsync(CancellationToken cancellationToken)
    {
        if (metadataTransport is null)
            return false;

        RoutingMetrics.MetadataRefreshes.Add(1);

        KahunaRoutingMetadataResponse response;

        try
        {
            response = await metadataTransport.GetRoutingMetadata(bootstrapUrl(), null, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            RoutingMetrics.MetadataRefreshFailures.Add(1, Unavailable);
            if (logger is not null && logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("Routing metadata could not be read: {Message}", ex.Message);
            return false;
        }

        RoutingMetadataSnapshot? snapshot = RoutingMetadataSnapshot.TryCreate(response, metadataLifetime, out string rejection);

        if (snapshot is null)
        {
            RoutingMetrics.MetadataRefreshFailures.Add(1, new KeyValuePair<string, object?>("reason", rejection));
            if (logger is not null && logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("Routing metadata was not usable: {Reason}", rejection);
            return false;
        }

        // Published whole. A reader either sees the previous map or this one, never a mixture, so a
        // split that landed between two key spaces cannot be observed half-applied.
        metadata = snapshot;

        return true;
    }

    private bool IsSuppressed(string endpoint)
    {
        if (suppressed.IsEmpty)
            return false;

        if (!suppressed.TryGetValue(endpoint, out long until))
            return false;

        if (Environment.TickCount64 < until)
            return true;

        suppressed.TryRemove(endpoint, out _);
        return false;
    }

    private void StartMetadataRefresh()
    {
        if (metadataTransport is null)
            return;

        if (Interlocked.CompareExchange(ref metadataRefreshInFlight, 1, 0) != 0)
        {
            RoutingMetrics.MetadataRefreshesCoalesced.Add(1);
            return;
        }

        _ = RunMetadataRefreshAsync();
    }

    /// <summary>
    /// The background half of a metadata read. It owns its own failures: nothing awaits it, so an
    /// exception escaping here would be an unobserved task fault rather than a caller's problem.
    /// The in-flight flag is cleared on every path, including cancellation, so one failed read
    /// cannot wedge discovery for the life of the client.
    /// </summary>
    private async Task RunMetadataRefreshAsync()
    {
        try
        {
            using CancellationTokenSource timeout = new(MetadataReadTimeout);

            await RefreshMetadataAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RoutingMetrics.MetadataRefreshFailures.Add(1, Unavailable);
            if (logger is not null && logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("Routing metadata read failed: {Message}", ex.Message);
        }
        finally
        {
            Volatile.Write(ref metadataRefreshInFlight, 0);
        }
    }

    /// <summary>
    /// Deadline for one background metadata read. Bounded so a node that accepts the connection and
    /// then stalls cannot hold the in-flight flag — and therefore metadata routing — indefinitely.
    /// </summary>
    private static readonly TimeSpan MetadataReadTimeout = TimeSpan.FromSeconds(10);

    // Pre-built so a rejection on the response path allocates no tag.
    private static readonly KeyValuePair<string, object?> EndpointRejected = new("reason", "endpoint_rejected");
    private static readonly KeyValuePair<string, object?> Superseded = new("reason", "superseded");
    private static readonly KeyValuePair<string, object?> UnknownProvenance = new("reason", "unknown_provenance");
    private static readonly KeyValuePair<string, object?> Unavailable = new("reason", "unavailable");
}
