namespace Kahuna.Server.KeyValues;

/// <summary>
/// Ambient marker for the check round of a transactional set-many that spans several partition leaders.
///
/// <para>A batch whose items resolve to several leaders is run in two rounds: every leader first checks its items
/// and stages nothing, and only when every item everywhere passed does every leader stage its items. The check
/// round reaches a remote leader through the same forwarded entry point a stage round does
/// (<c>IKahuna.LocateAndTrySetManyKeyValue</c>), which carries no such bit on its signature. The inter-node
/// transport copies the bit onto the wire request, the receiving node enters this scope from that field before it
/// serves the forwarded call, and the routed operation reads it on the unregistered path to run its locator in
/// check mode. In-process the in-memory transport enters the scope directly.</para>
///
/// <para>Unset means a stage round, so every path that does not enter the scope stages exactly as before.</para>
/// </summary>
internal static class SetManyCheckRoundScope
{
    private static readonly AsyncLocal<bool> current = new();

    /// <summary>Whether the current dispatch is the check round of a batch.</summary>
    public static bool IsActive => current.Value;

    /// <summary>
    /// Enters a scope that marks the dispatch as a check round when <paramref name="checkOnly"/> is true, and as a
    /// stage round otherwise. Returns a scope that restores the previous value when disposed by the same async flow.
    /// </summary>
    public static Scope Enter(bool checkOnly)
    {
        bool previous = current.Value;
        current.Value = checkOnly;
        return new Scope(previous);
    }

    public readonly struct Scope(bool previous) : IDisposable
    {
        public void Dispose() => current.Value = previous;
    }
}
