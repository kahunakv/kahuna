
namespace Kahuna.Utils;

/// <summary>
/// Counts the Kahuna nodes that are alive in this process. A production process hosts one node; an
/// embedded cluster (tests, in-process consumers) hosts several, and an in-process transport can hand
/// the same proposal byte array to every one of them. Process-wide caches keyed on such shared arrays
/// read this count as their take budget: it says how many local consumers may come for one entry. The
/// count is a hint, never a correctness input — an overcount only delays an entry's release to its
/// weakly held key, and an undercount only costs one redundant decode.
/// </summary>
internal static class InProcessNodeCensus
{
    private static int liveNodes;

    /// <summary>Records one more live node. Called once per successful <c>KahunaManager</c> construction.</summary>
    internal static void NodeStarted() => Interlocked.Increment(ref liveNodes);

    /// <summary>Records one node teardown. Called once per <c>KahunaManager</c> dispose.</summary>
    internal static void NodeStopped() => Interlocked.Decrement(ref liveNodes);

    /// <summary>The number of live nodes right now. Zero before any node exists.</summary>
    internal static int Count => Volatile.Read(ref liveNodes);
}
