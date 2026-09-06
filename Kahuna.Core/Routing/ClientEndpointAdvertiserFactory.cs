using Kahuna.Server.Configuration;
using Kommander;

namespace Kahuna.Server.Routing;

/// <summary>
/// Builds the endpoint advertiser each locator uses from the node's configuration.
///
/// <para>
/// A node with hints turned off gets an advertiser whose local URL is empty and which names no
/// peer. Every recording path then drops its route, which is what turns hint emission off for that
/// node with no further check on the request path.
/// </para>
/// </summary>
internal static class ClientEndpointAdvertiserFactory
{
    public static ClientEndpointAdvertiser Create(IRaft raft, KahunaConfiguration configuration)
    {
        bool enabled = configuration.RoutingHintsEnabled;

        return new ClientEndpointAdvertiser(
            raft,
            configuration.AdvertisedClientEndpoint,
            string.IsNullOrEmpty(configuration.AdvertisedClientScheme) ? configuration.InterNodeGrpcScheme : configuration.AdvertisedClientScheme,
            enabled && configuration.AdvertisePeerEndpoints,
            enabled
        );
    }
}
