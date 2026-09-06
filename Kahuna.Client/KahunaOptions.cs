
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Client.Routing;

namespace Kahuna.Client;

/// <summary>
/// Represents configuration options for the Kahuna client.
/// </summary>
public class KahunaOptions
{
    public bool UpgradeUrls { get; set; }

    /// <summary>
    /// How the client picks the node to send an operation to.
    ///
    /// <para>
    /// Defaults to <see cref="KahunaRoutingMode.Auto"/>: learned routing for a client that was given
    /// several endpoints, and plain rotation for one given a single endpoint. Whichever mode is in
    /// force, the node that receives the request resolves the resource itself, so the choice changes
    /// efficiency and never an operation's outcome.
    /// </para>
    /// </summary>
    public KahunaRoutingMode Routing { get; set; } = KahunaRoutingMode.Auto;

    /// <summary>
    /// How many learned routes the client holds. Past it the least recently added are dropped, so a
    /// workload over an unbounded key space costs bounded memory and degrades to endpoint rotation
    /// rather than growing without limit. Measure it against a working set; it is a tuning value,
    /// not a guarantee.
    /// </summary>
    public int RouteCacheCapacity { get; set; } = 4096;

    /// <summary>
    /// How long a learned route is used before it must be observed again. It bounds how long a
    /// client keeps choosing a destination that leadership has moved away from; it does not make a
    /// route within it correct, since leadership can move at any moment. A shorter value repairs a
    /// moved partition sooner and learns more often.
    /// </summary>
    public TimeSpan RouteHintLifetime { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How long an endpoint is held out of routing after a transport failure. It stops following
    /// operations from queueing behind a node that is down; it says nothing about whether the failed
    /// operation ran, and never permits a retry on its own.
    /// </summary>
    public TimeSpan RoutingEndpointCooldown { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long the client keeps one routing-metadata map before reading it again. Only consulted in
    /// <see cref="KahunaRoutingMode.Metadata"/>.
    /// </summary>
    public TimeSpan RoutingMetadataLifetime { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Maps the endpoints the servers advertise onto the URLs this client dials, for a deployment
    /// where the two differ — container port mapping, or split internal and external host names.
    /// Both sides are compared without a trailing slash and without case distinction.
    /// <para>
    /// A hint that resolves through this map is dialled as the mapped URL, so the existing
    /// connection pool for that URL is reused rather than a second one opened.
    /// </para>
    /// </summary>
    public IReadOnlyDictionary<string, string>? RoutingEndpointMap { get; set; }

    /// <summary>
    /// Whether the client may dial an endpoint a response named that is neither a configured URL nor
    /// a mapped one. Off by default: a response would otherwise be able to steer the client — and
    /// its credentials and TLS trust — at an address the operator never chose. Turn it on for a
    /// cluster that grows nodes the client was not started with, and only when every node's
    /// advertised address is one the client may dial.
    /// </summary>
    public bool AllowUnlistedRoutingEndpoints { get; set; }


    public int MinConnections { get; set; } = 1;

    public int MaxConnections { get; set; } = 1;

    /// <summary>
    /// Maximum time to wait for a batched operation to complete when the caller supplies no
    /// <see cref="CancellationToken"/>.  If the server does not respond within this window the
    /// operation is cancelled and a <see cref="OperationCanceledException"/> is surfaced to the
    /// caller.  Set to <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to disable the
    /// default deadline (restores the pre-CT3 hang-forever behaviour).
    /// </summary>
    public TimeSpan DefaultOperationTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Number of gRPC channels (and matching streaming pairs) created per server URL.
    /// A higher value increases parallelism at the cost of additional connections.
    /// Must be at least 1; defaults to 2.
    /// </summary>
    public int GrpcChannelPoolSize { get; set; } = 2;

    /// <summary>
    /// When a batch has fewer items than this threshold, the batcher waits
    /// <see cref="BatchCoalescingDelayMs"/> milliseconds before dispatching to allow more requests
    /// to accumulate (higher throughput, slightly higher latency).  Set to 0 or 1 to disable
    /// coalescing entirely.  Defaults to 10.
    /// </summary>
    public int BatchCoalescingThreshold { get; set; } = 10;

    /// <summary>
    /// Maximum coalescing delay in milliseconds applied when a batch is smaller than
    /// <see cref="BatchCoalescingThreshold"/>.  The actual delay is a random value in
    /// [1, <c>BatchCoalescingDelayMs</c>].  Set to 0 to disable the delay while keeping
    /// the threshold check.  Defaults to 2.
    /// </summary>
    public int BatchCoalescingDelayMs { get; set; } = 2;

    /// <summary>
    /// When <see langword="true"/>, TLS server certificate validation is skipped entirely.
    /// Intended for development and local testing only — leaves connections open to MITM attacks.
    /// </summary>
    public bool AllowInsecureCertificateValidation { get; set; }

    /// <summary>
    /// When non-empty, only server certificates whose SHA-256 thumbprint (hex, case-insensitive)
    /// matches one of these values are accepted.  An empty list falls back to standard OS chain
    /// and hostname validation.  Ignored when <see cref="AllowInsecureCertificateValidation"/> is
    /// <see langword="true"/>.
    /// </summary>
    public IReadOnlyList<string> TrustedServerCertificateThumbprints { get; set; } = [];
}