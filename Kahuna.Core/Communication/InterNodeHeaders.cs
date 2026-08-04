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
    /// directly with a single local leadership re-check (answering <c>MustRetry</c> when stale)
    /// instead of re-resolving ownership — re-resolving would forward again, and two nodes with
    /// disagreeing leadership views could bounce one request between them until it times out.
    /// </summary>
    public const string Forwarded = "kahuna-forwarded";

    /// <summary>Reusable header set for forwarded calls; never mutated after construction.</summary>
    public static readonly Metadata ForwardedCall = new() { { Forwarded, "1" } };

    public static bool IsForwarded(ServerCallContext context)
    {
        return context.RequestHeaders.GetValue(Forwarded) is not null;
    }
}
