using Kommander;

namespace Kahuna.Server.Tests;

/// <summary>
/// Base class for unit tests that construct <see cref="RaftManager"/> instances directly (rather than
/// through a fully-managed <c>EmbeddedKahunaNode</c> / cluster fixture that already disposes them).
///
/// <para>Every <see cref="RaftManager"/> starts a Kommander partition-executor pool in its constructor,
/// so an undisposed instance leaks those threads. xUnit constructs a fresh test-class instance per test
/// and disposes it afterwards; tracking each raft with <see cref="Track"/> and disposing here releases
/// those threads per test instead of letting them accumulate across the whole suite (which otherwise
/// exhausts threads and stalls a full-suite run).</para>
/// </summary>
public abstract class RaftTrackingTest : IDisposable
{
    private readonly List<RaftManager> trackedRafts = [];

    /// <summary>Registers <paramref name="raft"/> for disposal at test teardown and returns it, so it can
    /// be wrapped inline at the construction site: <c>Track(new RaftManager(...))</c>.</summary>
    protected RaftManager Track(RaftManager raft)
    {
        trackedRafts.Add(raft);
        return raft;
    }

    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
        foreach (RaftManager raft in trackedRafts)
        {
            try { raft.Dispose(); } catch { /* best-effort test cleanup */ }
        }
    }
}
