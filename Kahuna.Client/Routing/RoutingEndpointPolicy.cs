using System.Collections.Concurrent;

namespace Kahuna.Client.Routing;

/// <summary>
/// Decides which endpoints a response may send this client to, and rewrites them into the exact URL
/// strings the client already dials.
///
/// <para>
/// A hint names a node, and the client dials URLs. Those are not always the same text: a node
/// advertises the address other nodes reach it on, which differs from the client's address whenever
/// port mapping or split internal/external host names are in play. So a hint is accepted only after
/// it resolves to a configured endpoint — directly, or through an explicit mapping the caller
/// supplies. An endpoint the operator never configured is refused by default rather than dialled,
/// which keeps a response from steering the client at an address, and a TLS peer, nobody chose.
/// </para>
///
/// <para>
/// Resolution also canonicalises: the accepted hint is replaced by the configured string instance,
/// so the transport's per-URL connection pool finds its existing entry instead of opening a second
/// pool for a different spelling of the same address.
/// </para>
/// </summary>
internal sealed class RoutingEndpointPolicy
{
    private readonly Dictionary<string, string> configured;

    private readonly Dictionary<string, string> explicitMap;

    private readonly bool allowUnlisted;

    /// <summary>
    /// Resolutions already computed, keyed by the raw hint text. A hint repeats on every response,
    /// and the resolution is pure, so it is computed once per distinct advertised endpoint. Bounded
    /// by the number of endpoints a cluster advertises; entries are added only for hints that
    /// resolved, so an unresolvable hint cannot grow it without bound.
    /// </summary>
    private readonly ConcurrentDictionary<string, string> resolved = new(StringComparer.Ordinal);

    public RoutingEndpointPolicy(IReadOnlyList<string> configuredUrls, IReadOnlyDictionary<string, string>? endpointMap, bool allowUnlisted)
    {
        this.allowUnlisted = allowUnlisted;

        configured = new Dictionary<string, string>(configuredUrls.Count, StringComparer.OrdinalIgnoreCase);

        foreach (string url in configuredUrls)
        {
            string normalized = Normalize(url);

            if (normalized.Length > 0)
                configured[normalized] = url;
        }

        explicitMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (endpointMap is null)
            return;

        foreach (KeyValuePair<string, string> pair in endpointMap)
        {
            string from = Normalize(pair.Key);
            string to = Normalize(pair.Value);

            if (from.Length == 0 || to.Length == 0)
                continue;

            // The mapped target is canonicalised to the configured instance when it names one, so a
            // mapping and a direct match end on the same string and therefore the same connection pool.
            explicitMap[from] = configured.TryGetValue(to, out string? canonical) ? canonical : pair.Value;
        }
    }

    /// <summary>
    /// The URL this client should dial for <paramref name="advertised"/>, or null when the hint is
    /// unusable under this policy.
    /// </summary>
    public string? Resolve(string? advertised)
    {
        if (string.IsNullOrEmpty(advertised))
            return null;

        if (resolved.TryGetValue(advertised, out string? cached))
            return cached;

        string? answer = ResolveUncached(advertised);

        if (answer is not null)
            resolved[advertised] = answer;

        return answer;
    }

    private string? ResolveUncached(string advertised)
    {
        string normalized = Normalize(advertised);

        if (normalized.Length == 0)
            return null;

        if (explicitMap.TryGetValue(normalized, out string? mapped))
            return mapped;

        if (configured.TryGetValue(normalized, out string? canonical))
            return canonical;

        if (!allowUnlisted)
            return null;

        // An unlisted endpoint is only ever dialled when the caller opted in, and even then only
        // when it is a well-formed absolute HTTP(S) URL: a malformed or non-HTTP hint is a
        // configuration fault on the server side, not a destination.
        return Uri.TryCreate(advertised, UriKind.Absolute, out Uri? uri)
               && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps)
            ? advertised
            : null;
    }

    /// <summary>
    /// Comparable form of an endpoint: no trailing slash, and no case distinction. A trailing slash
    /// and a host's letter case are not part of a node's identity, and treating them as part of it
    /// would make one node look like two.
    /// </summary>
    private static string Normalize(string? url) => string.IsNullOrEmpty(url) ? "" : url.TrimEnd('/');
}
