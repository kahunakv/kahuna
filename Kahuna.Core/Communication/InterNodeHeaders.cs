
using Grpc.Core;

namespace Kahuna.Server.Communication;

/// <summary>
/// gRPC metadata shared between the inter-node client and the externally-exposed services that also
/// receive node-to-node traffic.
/// </summary>
internal static class InterNodeHeaders
{
    /// <summary>
    /// Marks a request another node already routed to its owner. The receiving service serves it
    /// under <see cref="Server.ForwardedRequestScope"/> at the hop count the request carries, so its
    /// own ownership resolution is budgeted: a node that hosts the partition but does not lead it
    /// redirects once more to the leader it resolves locally, while a node that does not host the
    /// partition, or a chain that spent its budget, answers <c>MustRetry</c>. Without the budget,
    /// two nodes with disagreeing leadership views could bounce one request between them until it
    /// times out.
    /// </summary>
    public const string Forwarded = "kahuna-forwarded";

    /// <summary>Reusable header set for forwarded calls; never mutated after construction.</summary>
    public static readonly Metadata ForwardedCall = new() { { Forwarded, "1" } };

    /// <summary>
    /// True when the request claims to be forwarded by a peer. The claim changes how the request is
    /// routed, so it is a trust boundary: an untrusted caller that asserts it is refused through
    /// <paramref name="gate"/>, never served.
    /// </summary>
    public static bool IsForwarded(ServerCallContext context, NodeTransportGate gate)
    {
        if (context.RequestHeaders.GetValue(Forwarded) is null)
            return false;

        gate.RequirePeer(context);
        return true;
    }
}
