using Kahuna.Shared.Routing;

namespace Kahuna.Server.Routing;

/// <summary>
/// Collects the routes an in-flight request actually resolved, so the response builder can hand
/// them back to the client as advisory hints.
///
/// <para>
/// Keyed by domain and resource rather than held as one value per request: serving one public
/// operation resolves several routes. A transaction-scoped write routes its coordinator record and
/// its data key to different partitions, and a batched read routes one key per item. A single slot
/// would hand back whichever route happened to be resolved last, which for a transactional write is
/// the coordinator's partition — a hint that sends the next write on that key to the wrong node
/// every time.
/// </para>
///
/// <para>
/// The first resource is held in fields and only a second distinct resource allocates the
/// dictionary, so a point operation — the case the routing cache exists for — captures its route
/// without touching the heap.
/// </para>
///
/// <para>
/// Recording is thread-safe because a multi-key operation fans out concurrently across partitions
/// and records from each branch. Reading is not: read it after the operation completes, from the
/// flow that built it.
/// </para>
/// </summary>
internal sealed class RouteCapture
{
    private readonly object gate = new();

    private KahunaRoutingDomain firstDomain;

    private string? firstResource;

    private RouteRecord firstRecord;

    private Dictionary<RouteCaptureKey, RouteRecord>? overflow;

    /// <summary>
    /// Records the route <paramref name="resource"/> resolved to. A later record for the same
    /// resource replaces the earlier one: a retry inside one operation resolves against a fresher
    /// view than the attempt it replaced.
    /// </summary>
    public void Record(KahunaRoutingDomain domain, string resource, in RouteRecord record)
    {
        if (string.IsNullOrEmpty(resource) || record.Endpoint.Length == 0)
            return;

        lock (gate)
        {
            if (firstResource is null)
            {
                firstDomain = domain;
                firstResource = resource;
                firstRecord = record;
                return;
            }

            if (firstDomain == domain && string.Equals(firstResource, resource, StringComparison.Ordinal))
            {
                firstRecord = record;
                return;
            }

            overflow ??= new();
            overflow[new RouteCaptureKey(domain, resource)] = record;
        }
    }

    /// <summary>The route recorded for <paramref name="resource"/>, if any was resolved.</summary>
    public bool TryGet(KahunaRoutingDomain domain, string resource, out RouteRecord record)
    {
        lock (gate)
        {
            if (firstResource is not null && firstDomain == domain && string.Equals(firstResource, resource, StringComparison.Ordinal))
            {
                record = firstRecord;
                return true;
            }

            if (overflow is not null)
                return overflow.TryGetValue(new RouteCaptureKey(domain, resource), out record);
        }

        record = default;
        return false;
    }

    /// <summary>Whether anything was recorded at all, so a response builder can skip its hint work.</summary>
    public bool IsEmpty
    {
        get
        {
            lock (gate)
                return firstResource is null;
        }
    }

    private readonly record struct RouteCaptureKey(KahunaRoutingDomain Domain, string Resource);
}
