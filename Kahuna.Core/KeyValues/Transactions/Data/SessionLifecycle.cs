
namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Tracks the coarse-grained lifecycle state of an interactive transaction session.
/// Transitions move forward only: AcceptingOperations → Finalizing → Terminal (or back on timeout).
/// </summary>
internal enum SessionLifecycle
{
    /// <summary>The session is open and accepts new operations.</summary>
    AcceptingOperations,

    /// <summary>A commit or rollback has been requested; no new operations are accepted while in-flight ones drain.</summary>
    Finalizing,

    /// <summary>The session timed out during finalization and was force-closed by the reaper.</summary>
    Reaping,

    /// <summary>The session has completed (committed or rolled back) and will be removed.</summary>
    Terminal
}
