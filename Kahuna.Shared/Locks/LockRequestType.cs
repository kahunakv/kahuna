
namespace Kahuna.Shared.Locks;

/// <summary>
/// Represents the types of lock requests that can be issued.
/// </summary>
public enum LockRequestType
{
    TryLock,
    TryExtendLock,
    TryUnlock,
    Get,
    CompleteProposal,
    ReleaseProposal,

    /// <summary>
    /// Actor-internal cache-coherence message: carries a committed lock mutation from the
    /// replication/restore path to the owning actor so a resident in-memory entry is brought up to
    /// date with the replicated log. Never serialized into the Raft log.
    /// </summary>
    InvalidateOrApply
}