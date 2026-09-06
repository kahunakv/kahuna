namespace Kahuna.Client.Routing;

/// <summary>
/// How the client picks the node to send an operation to.
///
/// <para>
/// The mode changes efficiency only. Every node accepts every operation and resolves the
/// destination itself, so a client in any mode gets the same outcome; a worse choice of endpoint
/// costs an inter-node forward.
/// </para>
/// </summary>
public enum KahunaRoutingMode
{
    /// <summary>
    /// The default. Picks <see cref="Learned"/> when the client was given more than one endpoint, and
    /// <see cref="RoundRobin"/> when it was given exactly one.
    ///
    /// <para>
    /// A single-endpoint client cannot act on most hints: a hint names whichever node owns the
    /// resource, and only endpoints the client was configured with are dialled, so hints naming the
    /// other nodes are refused. Such a client would keep a cache it could rarely use. A client that
    /// already knows several endpoints can act on every hint, which is where learned routing pays.
    /// </para>
    ///
    /// <para>
    /// Set a mode explicitly to override the choice; nothing about it is implicit at run time, and
    /// the resolved mode is what the client reports.
    /// </para>
    /// </summary>
    Auto = 0,

    /// <summary>
    /// Rotate over the configured endpoints: a request lands on an arbitrary node, which forwards it
    /// to the owner when it is not the owner itself. The behaviour of every client before routing
    /// existed.
    /// </summary>
    RoundRobin = 1,

    /// <summary>
    /// Reuse the destination a previous response reported for the same resource, and fall back to
    /// <see cref="RoundRobin"/> for a resource the client has not seen. Helps a workload that
    /// repeats resources; a stream of distinct new resources behaves as <see cref="RoundRobin"/>.
    /// </summary>
    Learned = 2,

    /// <summary>
    /// <see cref="Learned"/>, plus a resolution of unseen resources from routing metadata the
    /// client reads once per key space. A resource the client has never touched can go straight to
    /// its owner once that metadata is available. The client falls back to <see cref="Learned"/>
    /// behaviour whenever the metadata is missing, stale in a way it can detect, or expressed in
    /// terms it does not implement.
    /// </summary>
    Metadata = 3
}
