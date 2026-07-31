
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Resolves an operator-supplied wall-clock restore target (Unix milliseconds) to the HLC
/// stop-timestamp used by <b>every</b> restore and bootstrap path, so the two can never drift.
///
/// <para>A positive millisecond means "restore to the inclusive <b>end</b> of that millisecond":
/// commit HLCs order by physical millisecond, then logical counter, then node, so the inclusive upper
/// bound of a millisecond is its maximum counter (<see cref="uint.MaxValue"/>). A bare
/// <c>(·, ms, 0)</c> target would sort <i>before</i> a same-millisecond commit whose counter is greater
/// than zero and silently drop it, restoring earlier than the operator asked for.</para>
///
/// <para>A non-positive millisecond means "the natural end of the resolved chain"
/// (<see cref="HLCTimestamp.Zero"/>).</para>
/// </summary>
public static class PitrTargetResolver
{
    /// <summary>
    /// Maps <paramref name="milliseconds"/> (Unix epoch milliseconds) to the inclusive end-of-millisecond
    /// HLC target, or <see cref="HLCTimestamp.Zero"/> when non-positive (meaning "chain max").
    /// </summary>
    public static HLCTimestamp FromUnixMilliseconds(long milliseconds) =>
        milliseconds > 0 ? new HLCTimestamp(0, milliseconds, uint.MaxValue) : HLCTimestamp.Zero;
}
