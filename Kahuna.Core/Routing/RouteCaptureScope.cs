using Kahuna.Shared.Routing;

namespace Kahuna.Server.Routing;

/// <summary>
/// Ambient collection point for the routes an in-flight request resolves.
///
/// <para>
/// The route a public operation is admitted on is decided deep in a locator, several layers below
/// the transport method that writes the response. Carrying it back explicitly would widen the
/// return type of every locator, manager and <c>IKahuna</c> member on the way — a breaking change
/// to a public contract other repositories implement, for a value that is advisory. An
/// <see cref="AsyncLocal{T}"/> carries it instead, exactly as the forwarding budget already does.
/// </para>
///
/// <para>
/// The ambient value is set once per served request, and the locators write into the object it
/// holds, so the copy-on-write cost of an <see cref="AsyncLocal{T}"/> assignment is paid once and
/// not per resolved route. A request served while no scope is open records nothing and costs a
/// single null check.
/// </para>
/// </summary>
internal static class RouteCaptureScope
{
    private static readonly AsyncLocal<RouteCapture?> current = new();

    /// <summary>
    /// Whether hint collection is on for this process. Off, a request pays neither the ambient write
    /// nor the capture object, which is what makes turning hints off actually free.
    ///
    /// <para>
    /// Process-wide, and set once by the host at startup from its node's configuration — a server
    /// process runs one node. A process that embeds several nodes leaves it at its default; hint
    /// emission for such a node is governed by what that node advertises, so a node advertising no
    /// client endpoint records nothing regardless of this flag.
    /// </para>
    /// </summary>
    public static bool Enabled { get; set; } = true;

    /// <summary>The capture the current request writes into, or null when none is open.</summary>
    public static RouteCapture? Current => current.Value;

    /// <summary>
    /// Opens a capture for the current async flow and everything it awaits. Returns a scope that
    /// must be disposed by the same flow. Returns an empty scope, and captures nothing, when hint
    /// collection is off.
    /// </summary>
    public static Scope Begin(out RouteCapture? capture)
    {
        if (!Enabled)
        {
            capture = null;
            return default;
        }

        RouteCapture? previous = current.Value;

        capture = new RouteCapture();
        current.Value = capture;

        return new Scope(previous, true);
    }

    /// <summary>
    /// Records a resolved route into the open capture, if one is open. Called from the locators on
    /// the path that actually admitted the operation.
    /// </summary>
    public static void Record(KahunaRoutingDomain domain, string resource, int partitionId, string endpoint, KahunaRouteProvenance provenance, long generation = 0)
    {
        RouteCapture? capture = current.Value;

        if (capture is null || endpoint.Length == 0)
            return;

        capture.Record(domain, resource, new RouteRecord(partitionId, endpoint, provenance, generation));
    }

    /// <summary>Restores the capture that was open when this scope was entered.</summary>
    public readonly struct Scope(RouteCapture? previous, bool active) : IDisposable
    {
        public void Dispose()
        {
            if (active)
                current.Value = previous;
        }
    }
}
