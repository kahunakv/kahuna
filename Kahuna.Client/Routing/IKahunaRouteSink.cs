using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Routing;

namespace Kahuna.Client.Routing;

/// <summary>
/// What a transport reports back to the routing cache.
///
/// <para>
/// A transport learns two things the cache cannot see for itself: the routing hint a response
/// carried, and the fact that an endpoint stopped answering. It reports both here rather than the
/// cache being threaded through every operation signature.
/// </para>
/// </summary>
internal interface IKahunaRouteSink
{
    /// <summary>Whether anything is listening, so a transport can skip parsing hints it need not report.</summary>
    bool IsLearning { get; }

    /// <summary>
    /// Records the route a response reported for one resource. Learning never affects the operation
    /// the response belongs to: a hint that is missing, unusable or superseded is dropped and the
    /// caller's result is unchanged.
    /// </summary>
    void Learn(KahunaRoutingDomain domain, string resource, int partitionId, string? endpoint, KahunaRouteProvenance provenance, long generation);

    /// <summary>
    /// As above, telling the cache which endpoint the request was sent to. That is what lets a
    /// response that arrives late — after another response already moved the route — be dropped
    /// instead of undoing the newer answer, so a transport should always report it.
    /// </summary>
    void Learn(KahunaRoutingDomain domain, string resource, int partitionId, string? endpoint, KahunaRouteProvenance provenance, long generation, string? requestUrl);

    /// <summary>
    /// Reports that a request to <paramref name="url"/> failed at the transport. The endpoint is
    /// held out of routing for a bounded cooldown so following operations do not keep choosing it.
    /// <para>
    /// This says nothing about whether the failed operation ran. A failure after the request was
    /// submitted has an ambiguous outcome, and whether it may be retried is decided by that
    /// operation's own contract, never here.
    /// </para>
    /// </summary>
    void ReportEndpointFailure(string url);
}

/// <summary>
/// Implemented by the built-in transports so the client can hand them the sink they report to.
/// Internal: a transport supplied from outside this assembly cannot implement it and therefore
/// keeps working exactly as it did, with no routing.
/// </summary>
internal interface IKahunaRouteSinkReceiver
{
    IKahunaRouteSink? RouteSink { set; }
}

/// <summary>
/// The optional capability a transport implements to take part in routing.
///
/// <para>
/// It is a separate interface, and not members on <c>IKahunaCommunication</c>, so that a transport
/// written outside this repository keeps compiling and keeps working. A transport that does not
/// implement it simply learns nothing, and its client stays on endpoint rotation.
/// </para>
/// </summary>
public interface IKahunaRoutingTransport
{
    /// <summary>
    /// Reads a node's routing metadata. Called only in metadata mode, at most one call in flight per
    /// client, never once per operation.
    /// </summary>
    Task<KahunaRoutingMetadataResponse> GetRoutingMetadata(string url, string? keySpace, CancellationToken cancellationToken);
}
